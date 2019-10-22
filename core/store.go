package core

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/gocraft/dbr/v2"
	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

type ReplayReq struct {
	TS   int64  `json:"ts" db:"ts"`
	CH   string `json:"ch" db:"ch"`
	Name string `json:"name" db:"name"`
	URL  string `json:"url" db:"url"`
	MD5  string `json:"md5" db:"md5"`
}

func (r ReplayReq) filename(md5 string) string {
	if md5 == "" {
		md5 = r.MD5
	}
	return r.CH + "_" + strconv.FormatInt(r.TS, 10) + "_" + md5
}

var NOOP = errors.New("nothing need to be done")

type ReplayReqStore interface {
	Init() error
	Push(req ReplayReq) error
	Call(ch string, replay func(req ReplayReq) (string, error)) error
}

const (
	reqErr  = -1
	reqInit = 0
	reqDone = 1
)

var _ ReplayReqStore = &sqlStore{}

type sqlStore struct {
	db   *dbr.Connection
	wip  map[int]bool
	lock sync.Mutex
}

const sqliteSchema = `
create table if not exists replay_request (id integer primary key autoincrement, ts integer, ch text, name text, url text, md5 text, path text, state integer, message text);
create index if not exists ix_replay_request__ch_state on replay_request (ch, state);
create index if not exists ix_replay_request__ts on replay_request (ts);
`

func NewSQLiteStore(file string) (ReplayReqStore, error) {
	db, err := dbr.Open("sqlite3", fmt.Sprintf("file:%s", file), nil)
	if err != nil {
		return nil, err
	}
	return &sqlStore{db: db, wip: make(map[int]bool)}, nil
}

func (s *sqlStore) Init() error {
	sess := s.db.NewSession(nil)
	_, err := sess.Exec(sqliteSchema)
	return err
}

func (s *sqlStore) Push(req ReplayReq) error {
	_, err := s.db.NewSession(nil).
		InsertInto("replay_request").
		Columns("ts", "ch", "name", "url", "md5", "state").
		Values(req.TS, req.CH, req.Name, req.URL, req.MD5, reqInit).
		Exec()
	return err
}

func (s *sqlStore) Call(ch string, replay func(req ReplayReq) (string, error)) error {
	sess := s.db.NewSession(nil)

	var rs []struct {
		ReplayReq
		ID int `db:"id"`
	}
	q := sess.Select("*").
		From("replay_request").
		Where(dbr.Eq("ch", ch)).
		Where(dbr.Eq("state", reqInit)).
		OrderAsc("ts").
		OrderAsc("id").
		Limit(10)
	ids := s.withLock(nil)
	if len(ids) > 0 {
		q = q.Where("id not in ?", ids)
	}
	n, err := q.Load(&rs)
	if err != nil {
		return err
	}
	if n == 0 {
		return NOOP
	}

	optIdx := s.withLock(func() []int {
		for i, req := range rs {
			if !s.wip[req.ID] {
				s.wip[req.ID] = true
				return []int{i}
			}
		}
		return nil
	})
	if len(optIdx) == 0 {
		return NOOP
	}
	defer s.withLock(func() []int {
		delete(s.wip, optIdx[0])
		return nil
	})

	r := rs[optIdx[0]]
	path, err := replay(r.ReplayReq)
	up := sess.Update("replay_request").Where(dbr.Eq("id", r.ID))
	if err != nil {
		up.Set("state", reqErr).Set("message", err.Error()).Exec()
		return err
	} else {
		_, err := up.Set("state", reqDone).Set("path", path).Exec()
		return err
	}
}

func (s *sqlStore) withLock(cb func() []int) []int {
	s.lock.Lock()
	defer s.lock.Unlock()
	if cb == nil {
		cb = s.listWIP
	}
	return cb()
}

func (s *sqlStore) listWIP() []int {
	ids := make([]int, 0, len(s.wip))
	for id := range s.wip {
		ids = append(ids, id)
	}
	return ids
}

type debugSQL struct {
	dbr.NullEventReceiver
}

func (x *debugSQL) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	zap.L().Info(eventName, zap.Int64("ns", nanoseconds), zap.Any("info", kvs))
}

type ReplayDataFetcher interface {
	Fetch(req ReplayReq) (string, error)
}

var _ ReplayDataFetcher = &fetcher{}

type fetcher struct {
	dir string
}

func NewFetcher(dir string) (ReplayDataFetcher, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	return &fetcher{dir: dir}, nil
}

func (f *fetcher) Fetch(req ReplayReq) (string, error) {
	p := path.Join(f.dir, req.filename(""))
	if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
		if ff, err := os.Open(p); err == nil {
			defer ff.Close()
			return p, copyAndCheck(nil, ff, req.MD5)
		}
	}
	resp, err := http.Get(req.URL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	ff, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer ff.Close()
	return p, copyAndCheck(ff, resp.Body, req.MD5)
}

func copyAndCheck(out io.Writer, in io.Reader, md5sum string) error {
	h := md5.New()
	if out == nil {
		out = h
	} else {
		out = io.MultiWriter(out, h)
	}
	if _, err := io.Copy(out, in); err != nil {
		return errors.Trace(err)
	}
	if actsum := hex.EncodeToString(h.Sum(nil)); actsum != md5sum {
		return errors.New("md5sum mismatch: expect=" + md5sum + " actual=" + actsum)
	}
	return nil
}
