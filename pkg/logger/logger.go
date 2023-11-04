package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
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
