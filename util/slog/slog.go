package slog

import (
	json "encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq3"
	"logger"
	"os"
	"stash.cloudflare.com/go-stream/util"
	"strings"
	"time"
)

var (
	LogPrefix string
	glog      *logger.Logger         // the main logger object
	Gm        *util.StreamingMetrics // Main metrics object
)

// fatal: outputs a fatal startup error to STDERR, logs it to the
// logger if available and terminates the program
func fatal(l *logger.Logger, format string, v ...interface{}) {
	exit(1, l, format, v...)
}

// exit: outputs a startup message to STDERR, logs it to the
// logger if available and terminates the program with code
func exit(code int, l *logger.Logger, format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)

	if l != nil {
		l.Printf(logger.Levels.Error, LogPrefix, format, v...)
	}

	os.Exit(code)
}

func Init(logName *string, logLevel *string, logPrefix *string, metrics *util.StreamingMetrics, metricsAddr *string) {
	// Change logger level
	if err := logger.SetLogName(*logName); err != nil {
		fatal(nil, "Cannot set log name for program")
	}

	LogPrefix = "[" + *logPrefix + "] "

	if ll, ok := logger.CfgLevels[strings.ToLower(*logLevel)]; !ok {
		fatal(nil, "Unsupported log level: "+*logLevel)
	} else {
		if glog = logger.New(ll); glog == nil {
			fatal(nil, "Cannot start logger")
		}
	}

	Gm = metrics
	go statsSender(metricsAddr, logPrefix)
}

func Logf(level logger.Level, format string, v ...interface{}) {
	if glog != nil {
		glog.Printf(level, LogPrefix, format, v...)
	}
}

func Fatalf(format string, v ...interface{}) {
	fatal(glog, format, v)
}

type statsPkg struct {
	Name        string
	TotalMsgs   int64
	TotalErrors int64
	UpTime      int64
}

func statsSender(metricsAddr *string, processName *string) {

	rep, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		Logf(logger.Levels.Error, "Stats Sender error: %v", err.Error())
		return
	}
	defer rep.Close()
	err = rep.Bind(*metricsAddr)
	if err != nil {
		Logf(logger.Levels.Error, "Stats Sender error: %v", err.Error())
		return
	}

	Logf(logger.Levels.Info, "Stats sender, listening on %s", *metricsAddr)

	// Loop, printing the stats on request
	for {
		_, err := rep.Recv(0)
		if err != nil {
			Logf(logger.Levels.Error, "%v", err.Error())
		} else {
			timestamp := time.Now().Unix() - Gm.StartTime
			dBag := statsPkg{*processName, Gm.Total.Count(), Gm.Error.Count(), timestamp}
			stats, err := json.Marshal(dBag)
			if err == nil {
				_, err = rep.SendBytes(stats, zmq.DONTWAIT)
				if err != nil {
					Logf(logger.Levels.Error, "%v", err.Error())
				}
			}
		}
	}
}
