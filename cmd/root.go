package cmd

import (
	"math/rand"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewRootCmd() *cobra.Command {
	var opts struct {
		logLevel  LogLevel
		logOutput []string
	}
	cmd := &cobra.Command{
		Use: "mysql-replay",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			rand.Seed(time.Now().UnixNano())
			cfg := zap.NewDevelopmentConfig()
			cfg.Level = zap.NewAtomicLevelAt(opts.logLevel.Level)
			cfg.OutputPaths = opts.logOutput
			cfg.ErrorOutputPaths = opts.logOutput
			logger, _ := cfg.Build()
			zap.ReplaceGlobals(logger)
		},
	}
	opts.logLevel = LogLevel{zapcore.InfoLevel}
	cmd.PersistentFlags().Var(&opts.logLevel, "log-level", "log level")
	cmd.PersistentFlags().StringSliceVar(&opts.logOutput, "log-output", []string{"stdout"}, "log output")
	cmd.AddCommand(NewNotifyCmd())
	cmd.AddCommand(NewReplayCmd())
	cmd.AddCommand(NewServeCmd())
	return cmd
}

type LogLevel struct {
	zapcore.Level
}

func (lv *LogLevel) Type() string {
	return "string"
}
