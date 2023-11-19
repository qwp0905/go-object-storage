package logger

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
)

type logLevel int

var (
	stdout     = log.New(os.Stdout, "", 0)
	stderr     = log.New(os.Stderr, "", 0)
	levelDebug = logLevel(0)
	levelInfo  = logLevel(1)
	levelWarn  = logLevel(2)
	levelError = logLevel(3)
	none       = logLevel(99)

	m = map[logLevel]string{
		levelDebug: "debug",
		levelInfo:  "info",
		levelWarn:  "warn",
		levelError: "error",
	}

	defaultLevel = levelInfo
)

func Config(level string) {
	switch level {
	case "debug":
		defaultLevel = levelDebug
	case "info":
		defaultLevel = levelInfo
	case "warn":
		defaultLevel = levelWarn
	case "error":
		defaultLevel = levelError
	case "none":
		defaultLevel = none
	default:
		Warnf("unknown log level %s", level)
	}
}

func getLevel(level logLevel) string {
	return m[level]
}

func Error(message string) {
	if defaultLevel > levelError {
		return
	}
	stderr.Print(format(levelError, message))
}

func Errorf(f string, v ...any) {
	Error(fmt.Sprintf(f, v...))
}

func Info(message string) {
	if defaultLevel > levelInfo {
		return
	}
	stdout.Print(format(levelInfo, message))
}

func Infof(f string, v ...any) {
	Info(fmt.Sprintf(f, v...))
}

func Debug(message string) {
	if defaultLevel > levelDebug {
		return
	}
	stdout.Print(format(levelDebug, message))
}

func Debugf(f string, v ...any) {
	Debug(fmt.Sprintf(f, v...))
}

func Warn(message string) {
	if defaultLevel > levelWarn {
		return
	}
	stdout.Print(format(levelWarn, message))
}

func Warnf(f string, v ...any) {
	Warn(fmt.Sprintf(f, v...))
}

func Fatal(err error) {
	Errorf("%+v", err)
	os.Exit(1)
}

type jsonLog struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	At      string `json:"at"`
}

func format(level logLevel, message string) string {
	l := &jsonLog{
		Level:   getLevel(level),
		Message: message,
		At:      localDateTime(),
	}
	b, _ := json.Marshal(l)
	return string(b) + "\n"
}

var tz, _ = time.LoadLocation("Asia/Seoul")

func localDateTime() string {
	return time.Now().In(tz).Format(time.DateTime)
}

func CtxError(ctx *fiber.Ctx, err error) {
	if defaultLevel > levelError {
		return
	}
	l := &ctxLog{
		Path:    ctx.Path(),
		At:      time.Now(),
		Message: fmt.Sprintf("%+v", err),
		Query:   string(ctx.Request().URI().QueryString()),
		Body:    string(ctx.Body()),
		Level:   getLevel(levelError),
		Method:  ctx.Method(),
	}
	b, _ := json.Marshal(l)
	stderr.Println(string(b))
}

type ctxLog struct {
	Level   string    `json:"level"`
	At      time.Time `json:"at"`
	Message string    `json:"message"`
	Query   string    `json:"query"`
	Path    string    `json:"path"`
	Body    string    `json:"body"`
	Method  string    `json:"method"`
}
