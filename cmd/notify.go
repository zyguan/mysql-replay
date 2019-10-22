package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/zyguan/mysql-replay/core"
	"go.uber.org/zap"
)

func NewNotifyCmd() *cobra.Command {
	var opts struct {
		ch      string
		cbURL   string
		downURL string
	}
	cmd := &cobra.Command{
		Use:   "notify",
		Short: "Send a replay request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			t := time.Now()
			f, err := os.Open(args[0])
			if err != nil {
				zap.L().Info("open file", zap.String("name", args[0]))
				return err
			}
			defer f.Close()
			h := md5.New()
			if _, err = io.Copy(h, f); err != nil {
				zap.L().Info("calc md5sum", zap.String("name", f.Name()))
				return err
			}
			buf := new(bytes.Buffer)
			req := core.ReplayReq{
				CH:   opts.ch,
				Name: f.Name(),
				URL:  fmt.Sprintf(opts.downURL, f.Name()),
				MD5:  hex.EncodeToString(h.Sum(nil)),
				TS:   t.Unix(),
			}
			if err = json.NewEncoder(buf).Encode(req); err != nil {
				zap.L().Info("encode request data")
				return err
			}

			zap.L().Info("POST "+opts.cbURL, zap.String("body", buf.String()))
			res, err := http.Post(opts.cbURL, "application/json", buf)
			if err != nil {
				return err
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return err
			}
			zap.L().Info("POST "+opts.cbURL, zap.Int("status", res.StatusCode), zap.String("body", string(body)))
			return nil
		},
	}
	defaultCh, _ := os.Hostname()
	cmd.Flags().StringVar(&opts.ch, "channel", defaultCh, "tcpdump channel")
	cmd.Flags().StringVar(&opts.cbURL, "callback-url", "http://127.0.0.1:5000/requests", "callback url")
	cmd.Flags().StringVar(&opts.downURL, "download-url", "", "download url format")
	cmd.MarkFlagRequired("download-url")
	return cmd
}
