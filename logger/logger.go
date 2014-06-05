package logger

import (
	"log"
)

const DefaultFlags = log.LstdFlags | log.Lshortfile | log.Lmicroseconds

var (
	Trace = log.New(NewColorStdout("!b"), "  TRACE ", DefaultFlags)
	Debug = log.New(NewColorStdout("c"), "  DEBUG ", DefaultFlags)
	Info  = log.New(NewColorStdout("g"), "   INFO ", DefaultFlags)
	Warn  = log.New(NewColorStdout("y"), "   WARN ", DefaultFlags)
	Error = log.New(NewColorStdout("r"), "  ERROR ", DefaultFlags)
)

func SetFlags(flag int) {
	for _, l := range []*log.Logger{Trace, Debug, Info, Warn, Error} {
		l.SetFlags(flag)
	}
}
