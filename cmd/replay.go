package cmd

import (
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/zyguan/mysql-replay/core"
	"github.com/zyguan/mysql-replay/stats"
	"github.com/zyguan/mysql-replay/stream"
	"go.uber.org/zap"
)

type ReplayFlags struct {
	stream.ReplayOptions
	stream.FactoryOptions
	Ports []int
	Speed float64
}

func (rf *ReplayFlags) Register(flags *pflag.FlagSet, defaultCCSize uint) {
	flags.StringVar(&rf.TargetDSN, "target-dsn", "", "target dsn")
	flags.BoolVar(&rf.DryRun, "dry-run", false, "dry run mode (just print statements)")
	flags.UintVar(&rf.ConnCacheSize, "conn-cache-size", defaultCCSize, "packet cache size for each connection")
	flags.IntSliceVar(&rf.Ports, "ports", []int{4000}, "ports to filter in")
	flags.Float64Var(&rf.Speed, "speed", 1, "replay speed ratio")
	flags.StringVar(&rf.FilterIn, "filter-in", "", "filter in statements")
	flags.StringVar(&rf.FilterOut, "filter-out", "", "filter out statements")
}

func NewReplayCmd() *cobra.Command {
	var opts ReplayFlags
	var reportInterval time.Duration
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			ch := make(chan gopacket.Packet, 512)
			consume := func(pkt gopacket.Packet) { ch <- pkt }
			factory := stream.NewMySQLStreamFactory(opts.NewStreamHandler, opts.FactoryOptions)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)
			ticker := time.Tick(time.Minute)

			go func() {
				defer close(ch)
				for _, in := range args {
					core.OfflineReplayTask{
						File:       in,
						Filter:     core.FilterByPort(opts.Ports),
						SpeedRatio: opts.Speed,
					}.Run(consume)
				}
			}()

			go func() {
				prv := stats.Dump()
				for {
					time.Sleep(reportInterval)
					cur := stats.Dump()
					zap.L().Info("progress",
						zap.Int64("conns", cur[stats.Connections]),
						zap.Int64("streams", cur[stats.Streams]),
						zap.Int64("packets", cur[stats.Packets]),
						zap.Int64("queries", cur[stats.Queries]),
						zap.Int64("failed_queries", cur[stats.FailedQueries]),
						zap.Float64("qps", float64(cur[stats.Queries]-prv[stats.Queries])/float64(reportInterval/time.Second)),
					)
					prv = cur
				}
			}()

			t0 := time.Now()
			for {
				select {
				case pkt := <-ch:
					if pkt == nil {
						assembler.FlushAll()
						t1 := time.Now()
						cur := stats.Dump()
						zap.L().Info("progress",
							zap.Int64("packets", cur[stats.Packets]),
							zap.Int64("queries", cur[stats.Queries]),
							zap.Int64("failed_queries", cur[stats.FailedQueries]),
							zap.Duration("duration", t1.Sub(t0)),
						)
						return
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
	opts.Register(cmd.Flags(), 0)
	cmd.PersistentFlags().DurationVar(&reportInterval, "report-interval", 5*time.Second, "report interval")
	return cmd
}

type wrapCaptureInfo gopacket.CaptureInfo

func (ci wrapCaptureInfo) GetCaptureInfo() gopacket.CaptureInfo {
	return gopacket.CaptureInfo(ci)
}
