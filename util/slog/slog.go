package slog

import (
	json "encoding/json"
	"fmt"
	"github.com/cloudflare/golog/logger"
	zmq "github.com/pebbe/zmq3"
	"os"
	"github.com/cloudflare/go-stream/util"
	"strings"
	"time"
)

var (
	LogPrefix string
	glog      *logger.Logger         // the main logger object
	Gm        *util.StreamingMetrics // Main metrics object
)

const (
	DEFAULT_STATS_LOG_NAME   = "test"
	DEFAULT_STATS_LOG_LEVEL  = "debug"
	DEFAULT_STATS_LOG_PREFIX = "test"
	DEFAULT_STATS_ADDR       = "tcp://127.0.0.1:5450"
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

func Init(logName string, logLevel string, logPrefix string, metrics *util.StreamingMetrics, metricsAddr string,
	logAddress string, logNetwork string) {

	// Change logger level
	if err := logger.SetLogName(logName); err != nil {
		fatal(nil, "Cannot set log name for program")
	}

	// And set the logger to write to a custom socket.
	if logAddress != "" && logNetwork != "" {
		if err := logger.SetCustomSocket(logAddress, logNetwork); err != nil {
			fatal(nil, "Cannot set custom log socket program: %s %s %v", logAddress, logNetwork, err)
		}
	}

	LogPrefix = "[" + logPrefix + "] "

	if ll, ok := logger.CfgLevels[strings.ToLower(logLevel)]; !ok {
		fatal(nil, "Unsupported log level: "+logLevel)
	} else {
		if glog = logger.New(ll); glog == nil {
			fatal(nil, "Cannot start logger")
		}
	}

	Gm = metrics
	go statsSender(&metricsAddr, &logPrefix)
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
	Name      string
	UpTime    int64
	OpMetrics map[string]interface{}
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
			dBag := statsPkg{*processName, timestamp, map[string]interface{}{}}
			for k, v := range Gm.OpGroups {
				dBag.OpMetrics[k] = map[string]int64{"Events": v.Events.Count(), "Errors": v.Errors.Count(), "Queue": v.QueueLength.Value()}
			}
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
