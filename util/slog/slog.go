package slog

import (
	"fmt"
	"logger"
	"os"
	"strings"
)

const LogPrefixCLI = "[CLI] "

var (
	glog *logger.Logger // the main logger object
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
		l.Printf(logger.Levels.Error, LogPrefixCLI, format, v...)
	}

	os.Exit(code)
}

func Init(logName *string, logLevel *string) {
	// Change logger level
	if err := logger.SetLogName(*logName); err != nil {
		fatal(nil, "Cannot set log name for program")
	}

	if ll, ok := logger.CfgLevels[strings.ToLower(*logLevel)]; !ok {
		fatal(nil, "Unsupported log level: "+*logLevel)
	} else {
		if glog = logger.New(ll); glog == nil {
			fatal(nil, "Cannot start logger")
		}
	}
}

func Logf(level logger.Level, format string, v ...interface{}) {
	if glog != nil {
		glog.Printf(level, LogPrefixCLI, format, v...)
	}
}

func Fatalf(format string, v ...interface{}) {
	fatal(glog, format, v)
}
