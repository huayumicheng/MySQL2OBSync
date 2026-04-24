package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

type Logger struct {
	consoleOut io.Writer
	logFile    *os.File
	fileOut    io.Writer
	level      Level
	mu         sync.Mutex
}

var globalLogger *Logger

func InitWithDir(logDir string) error {
	l, err := NewLoggerWithTag(logDir, INFO, "")
	if err != nil {
		return err
	}
	globalLogger = l
	return nil
}

func InitWithDirAndTag(logDir, tag string) error {
	l, err := NewLoggerWithTag(logDir, INFO, tag)
	if err != nil {
		return err
	}
	globalLogger = l
	return nil
}

var logTagRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func sanitizeLogTag(tag string) string {
	tag = logTagRe.ReplaceAllString(tag, "_")
	for len(tag) > 0 && (tag[0] == '_' || tag[0] == '.' || tag[0] == '-') {
		tag = tag[1:]
	}
	for len(tag) > 0 && (tag[len(tag)-1] == '_' || tag[len(tag)-1] == '.' || tag[len(tag)-1] == '-') {
		tag = tag[:len(tag)-1]
	}
	if tag == "" {
		return "all"
	}
	if len(tag) > 80 {
		return tag[:80]
	}
	return tag
}

func NewLogger(logDir string, level Level) (*Logger, error) {
	return NewLoggerWithTag(logDir, level, "")
}

func NewLoggerWithTag(logDir string, level Level, tag string) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir failed: %w", err)
	}

	timestamp := time.Now().Format("20060102-150405")
	tag = sanitizeLogTag(tag)
	logFileName := fmt.Sprintf("mysql2ob-sync-%s-%s.log", timestamp, tag)
	logFilePath := filepath.Join(logDir, logFileName)

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %w", err)
	}

	return &Logger{
		consoleOut: os.Stdout,
		logFile:    logFile,
		fileOut:    logFile,
		level:      level,
	}, nil
}

func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] %s\n", timestamp, level.String(), message)

	if l.consoleOut != nil {
		fmt.Fprint(l.consoleOut, logLine)
	}
	if l.fileOut != nil {
		fmt.Fprint(l.fileOut, logLine)
	}
}

func (l *Logger) Debug(format string, args ...interface{}) { l.log(DEBUG, format, args...) }
func (l *Logger) Info(format string, args ...interface{})  { l.log(INFO, format, args...) }
func (l *Logger) Warn(format string, args ...interface{})  { l.log(WARN, format, args...) }
func (l *Logger) Error(format string, args ...interface{}) { l.log(ERROR, format, args...) }

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	os.Exit(1)
}

func Debug(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Debug(format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Info(format, args...)
	} else {
		fmt.Printf(format+"\n", args...)
	}
}

func Warn(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Warn(format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Error(format, args...)
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func Fatal(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Fatal(format, args...)
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(1)
	}
}

func Close() error {
	if globalLogger != nil {
		return globalLogger.Close()
	}
	return nil
}

func SetLevel(level Level) {
	if globalLogger != nil {
		globalLogger.SetLevel(level)
	}
}

