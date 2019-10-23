package cmd

import (
	"net/http"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"github.com/zyguan/mysql-replay/core"
	"github.com/zyguan/mysql-replay/stream"
	"go.uber.org/zap"
)

func NewServeCmd() *cobra.Command {
	var opts struct {
		ReplayFlags
		archiveDir string
		metaFile   string
		addr       string
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve for replay requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			ch := make(chan gopacket.Packet, 512)
			consume := func(pkt gopacket.Packet) { ch <- pkt }
			factory := stream.NewMySQLStreamFactory(opts.NewStreamHandler, opts.FactoryOptions)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)
			ticker := time.Tick(time.Minute)

			fetcher, err := core.NewFetcher(opts.archiveDir)
			if err != nil {
				return errors.Annotate(err, "new fetcher")
			}
			store, err := core.NewSQLiteStore(opts.metaFile)
			if err != nil {
				return errors.Annotate(err, "new store")
			}
			if err = store.Init(); err != nil {
				return errors.Annotate(err, "init store")
			}
			h := core.NewHandler(core.HandlerOptions{
				Store:   store,
				Fetcher: fetcher,
				Filter:  core.FilterByPort(opts.Ports),
				Emit:    consume,
				Speed:   opts.Speed,
			})

			go func() {
				http.Handle("/requests", h)
				zap.L().Info("listen on " + opts.addr)
				if err := http.ListenAndServe(opts.addr, nil); err != nil {
					zap.L().Error("stop serve on "+opts.addr, zap.Error(err))
				}
			}()

			for {
				select {
				case pkt := <-ch:
					if pkt == nil {
						assembler.FlushAll()
						return nil
					}
					assembler.AssembleWithContext(
						pkt.NetworkLayer().NetworkFlow(),
						pkt.Layer(layers.LayerTypeTCP).(*layers.TCP),
						wrapCaptureInfo(pkt.Metadata().CaptureInfo))
				case <-ticker:
					assembler.FlushCloseOlderThan(time.Now().Add(-2 * time.Minute))
				}
			}
		},
	}
	opts.Register(cmd.Flags(), 128)
	cmd.Flags().StringVar(&opts.archiveDir, "archive-dir", "archives", "directory to save dumped files")
	cmd.Flags().StringVar(&opts.metaFile, "meta", "meta.db", "path to meta data")
	cmd.Flags().StringVar(&opts.addr, "addr", ":5000", "address to listen on")
	return cmd
}
