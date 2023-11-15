package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
)

var (
	stdout = log.New(os.Stdout, "", 0)
	stderr = log.New(os.Stderr, "", 0)
)

func Error(message string) {
	stderr.Print(format("error", message))
}

func Errorf(f string, v ...any) {
	Error(fmt.Sprintf(f, v...))
}

func Info(message string) {
	stdout.Print(format("info", message))
}

func Infof(f string, v ...any) {
	Info(fmt.Sprintf(f, v...))
}

func Debug(message string) {
	stdout.Print(format("debug", message))
}

func Debugf(f string, v ...any) {
	Debug(fmt.Sprintf(f, v...))
}

func Warn(message string) {
	stdout.Print(format("warn", message))
}

func Warnf(f string, v ...any) {
	Warn(fmt.Sprintf(f, v...))
}

func Fatal(err error) {
	stderr.Print(format("error", fmt.Sprintf("%+v", err)))
	os.Exit(1)
}

type jsonLog struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	At      string `json:"at"`
}

func format(level, message string) string {
	l := &jsonLog{
		Level:   level,
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
	l := &ctxLog{
		Path:    ctx.Path(),
		At:      time.Now(),
		Message: fmt.Sprintf("%+v", err),
		Query:   string(ctx.Request().URI().QueryString()),
		Body:    string(ctx.Body()),
		Level:   "error",
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
