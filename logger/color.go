package logger

import (
	"github.com/wsxiaoys/terminal"
	"io"
)

type ColorWriter struct {
	Color  string
	Writer *terminal.TerminalWriter
}

var _ io.Writer = ColorWriter{}

func NewColorStderr(color string) ColorWriter {
	return ColorWriter{
		Color:  color,
		Writer: terminal.Stderr,
	}
}

func NewColorStdout(color string) ColorWriter {
	return ColorWriter{
		Color:  color,
		Writer: terminal.Stdout,
	}
}

func (w ColorWriter) Write(p []byte) (n int, err error) {
	defer w.Writer.Reset()
	return w.Writer.Color(w.Color).Write(p)
}
