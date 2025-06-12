package processpgx

import (
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger helper for logging process messages
type Logger struct {
	Code     string    // The process code
	ShowTime bool      // Whether to show process run times when logging
	Time     time.Time // Baseline for how long the process has been running
}

// NewLogger initialize a new process log. Defaults to show time in logs
func NewLogger(code string) (l *Logger) {
	return &Logger{
		Code:     code,
		Time:     time.Now(),
		ShowTime: true,
	}
}

// ResetTime resets the time to now
func (l *Logger) ResetTime() {
	l.Time = time.Now()
}

// Info helper to use zerolog info
func (l *Logger) Info(msg string, args ...interface{}) {
	l.Log(log.Info(), msg, args...)
}

// Warn helper to use zerolog warn
func (l *Logger) Warn(msg string, args ...interface{}) {
	l.Log(log.Warn(), msg, args...)
}

// Error helper to use zerolog error
func (l *Logger) Error(msg string, args ...interface{}) {
	l.Log(log.Error(), msg, args...)
}

// Log writes to the logger
func (l *Logger) Log(ze *zerolog.Event, msg string, args ...interface{}) {
	sb := strings.Builder{}
	defer sb.Reset()

	_ = sb.WriteByte('[')
	_, _ = sb.WriteString(l.Code)
	_ = sb.WriteByte(']')

	if l.ShowTime {
		_ = sb.WriteByte('[')
		_, _ = sb.WriteString(time.Since(l.Time).String())
		_ = sb.WriteByte(']')
	}

	_, _ = sb.WriteString(msg)
	ze.Msgf(sb.String(), args...)
}
