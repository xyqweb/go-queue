package qlog

import (
	"log"
	"os"
)

var DefaultLogger Logger = PrintfLogger(log.New(os.Stdout, "go-queue: ", log.LstdFlags))

type Logger interface {
	// Errorf logs a message at error level.
	Errorf(msg string, keysAndValues ...any)
	// Infof logs a message at info level.
	Infof(msg string, keysAndValues ...any)
}

// SetLogger sets the global logger.
func SetLogger(logger Logger) {
	DefaultLogger = logger
}

// PrintfLogger wraps a Printf-based logger (such as the standard library "log")
// into an implementation of the Logger interface which logs errors only.
func PrintfLogger(l interface{ Printf(string, ...interface{}) }) Logger {
	return printfLogger{l}
}

type printfLogger struct {
	logger interface{ Printf(string, ...interface{}) }
}

func (pl printfLogger) Errorf(msg string, keysAndValues ...any) {
	pl.logger.Printf(msg, keysAndValues...)
}

func (pl printfLogger) Infof(msg string, keysAndValues ...any) {
	pl.logger.Printf(msg, keysAndValues...)
}
