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
	logDir     string
	sessionTS  string
	jobName    string
	tableFiles map[string]*os.File
	mu         sync.Mutex
}

var globalLogger *Logger

func InitWithDir(logDir string) error {
	l, err := NewLoggerWithJobName(logDir, INFO, "mysql2ob-sync")
	if err != nil {
		return err
	}
	globalLogger = l
	return nil
}

func InitWithDirAndJobName(logDir, jobName string) error {
	l, err := NewLoggerWithJobName(logDir, INFO, jobName)
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
	return NewLoggerWithJobName(logDir, level, "mysql2ob-sync")
}

func NewLoggerWithJobName(logDir string, level Level, jobName string) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir failed: %w", err)
	}

	sessionTS := time.Now().Format("20060102-150405")
	jobName = sanitizeLogTag(jobName)
	if jobName == "" || jobName == "all" {
		jobName = "mysql2ob-sync"
	}
	logFileName := fmt.Sprintf("%s_%s.log", jobName, sessionTS)
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
		logDir:     logDir,
		sessionTS:  sessionTS,
		jobName:    jobName,
		tableFiles: map[string]*os.File{},
	}, nil
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, f := range l.tableFiles {
		_ = f.Close()
	}
	l.tableFiles = map[string]*os.File{}
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
	if tag, ok := extractTableTag(message); ok {
		l.writeTableLog(tag, logLine)
	}
}

var tableTagPrefixRe = regexp.MustCompile(`^\s*\[([A-Za-z0-9_.$-]+)\]`)
var compareTableRe = regexp.MustCompile(`^\s*Comparing table:\s*([A-Za-z0-9_.$-]+)\s*<->`)
var syncingTableRe = regexp.MustCompile(`^\s*\[\d+/\d+\]\s*Syncing table:\s*([A-Za-z0-9_.$-]+)\s*->`)

func extractTableTag(message string) (string, bool) {
	if m := tableTagPrefixRe.FindStringSubmatch(message); len(m) == 2 {
		return m[1], true
	}
	if m := compareTableRe.FindStringSubmatch(message); len(m) == 2 {
		return m[1], true
	}
	if m := syncingTableRe.FindStringSubmatch(message); len(m) == 2 {
		return m[1], true
	}
	return "", false
}

func (l *Logger) writeTableLog(tag, logLine string) {
	if l.logDir == "" || l.sessionTS == "" || l.jobName == "" {
		return
	}
	tag = sanitizeLogTag(tag)
	if tag == "" || tag == "all" {
		return
	}
	if l.tableFiles == nil {
		l.tableFiles = map[string]*os.File{}
	}
	f := l.tableFiles[tag]
	if f == nil {
		name := fmt.Sprintf("%s_%s_%s.log", l.jobName, l.sessionTS, tag)
		path := filepath.Join(l.logDir, name)
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		l.tableFiles[tag] = f
	}
	_, _ = f.WriteString(logLine)
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

