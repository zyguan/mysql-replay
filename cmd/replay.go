package cmd

import (
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/zyguan/mysql-replay/core"
	"github.com/zyguan/mysql-replay/stream"
)

type ReplayFlags struct {
	stream.ReplayOptions
	stream.FactoryOptions
	Ports []int
}

func (rf *ReplayFlags) Register(flags *pflag.FlagSet, defaultCCSize uint) {
	flags.StringVar(&rf.TargetDSN, "target-dsn", "", "target dsn")
	flags.BoolVar(&rf.DryRun, "dry-run", false, "dry run mode (just print statements)")
	flags.UintVar(&rf.ConnCacheSize, "conn-cache-size", defaultCCSize, "packet cache size for each connection")
	flags.IntSliceVar(&rf.Ports, "ports", []int{4000}, "ports to filter in")
}

func NewReplayCmd() *cobra.Command {
	var opts ReplayFlags
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
					core.OfflineReplayTask{File: in, Filter: core.FilterByPort(opts.Ports)}.Run(consume)
				}
			}()

			for {
				select {
				case pkt := <-ch:
					if pkt == nil {
						assembler.FlushAll()
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
	return cmd
}

type wrapCaptureInfo gopacket.CaptureInfo

func (ci wrapCaptureInfo) GetCaptureInfo() gopacket.CaptureInfo {
	return gopacket.CaptureInfo(ci)
}
