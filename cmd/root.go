package cmd

import (
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	//var opts struct {
	//	logLevel string
	//	logFile  string
	//}
	cmd := &cobra.Command{
		Use: "mysql-replay",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			rand.Seed(time.Now().UnixNano())
			logger, _ := zap.NewDevelopment()
			zap.ReplaceGlobals(logger)
		},
	}
	//cmd.PersistentFlags().StringVar(&opts.logLevel, "log-level", "debug", "log level")
	//cmd.PersistentFlags().StringVar(&opts.logFile, "log-file", "", "log file")
	cmd.AddCommand(NewNotifyCmd())
	cmd.AddCommand(NewReplayCmd())
	cmd.AddCommand(NewServeCmd())
	return cmd
}
