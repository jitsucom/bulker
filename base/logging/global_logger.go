package logging

import (
	"errors"
	"fmt"
	"github.com/gookit/color"
	"io"
	"log"
	"strings"
)

const (
	errPrefix   = "[ERROR]:"
	warnPrefix  = "[WARN]:"
	infoPrefix  = "[INFO]:"
	debugPrefix = "[DEBUG]:"

	GlobalType = "global"
)

var GlobalLogsWriter io.Writer
var ConfigErr string
var ConfigWarn string

var LogLevel = UNKNOWN

type Config struct {
	FileName    string
	FileDir     string
	RotationMin int64
	MaxBackups  int
	Compress    bool

	RotateOnClose bool
}

func (c Config) Validate() error {
	if c.FileName == "" {
		return errors.New("Logger file name can't be empty")
	}
	if c.FileDir == "" {
		return errors.New("Logger file dir can't be empty")
	}

	return nil
}

//InitGlobalLogger initializes main logger
func InitGlobalLogger(writer io.Writer, levelStr string) error {
	dateTimeWriter := DateTimeWriterProxy{
		writer: writer,
	}
	log.SetOutput(dateTimeWriter)
	log.SetFlags(0)

	LogLevel = ToLevel(levelStr)

	if ConfigErr != "" {
		Error(ConfigErr)
	}

	if ConfigWarn != "" {
		Warn(ConfigWarn)
	}
	return nil
}

func SystemErrorf(format string, v ...any) {
	SystemError(fmt.Sprintf(format, v...))
}

func SystemError(v ...any) {
	msg := []any{"System error:"}
	msg = append(msg, v...)
	Error(msg...)
	//TODO: implement system error notification
	//notifications.SystemError(msg...)
}

func Errorf(format string, v ...any) {
	Error(fmt.Sprintf(format, v...))
}

func Error(v ...any) {
	if LogLevel <= ERROR {
		log.Println(errMsg(v...))
	}
}

func Infof(format string, v ...any) {
	Info(fmt.Sprintf(format, v...))
}

func Info(v ...any) {
	if LogLevel <= INFO {
		log.Println(append([]any{infoPrefix}, v...)...)
	}
}

func Debugf(format string, v ...any) {
	Debug(fmt.Sprintf(format, v...))
}

func Debug(v ...any) {
	if LogLevel <= DEBUG {
		log.Println(append([]any{debugPrefix}, v...)...)
	}
}

func Warnf(format string, v ...any) {
	Warn(fmt.Sprintf(format, v...))
}

func Warn(v ...any) {
	if LogLevel <= WARN {
		log.Println(append([]any{warnPrefix}, v...)...)
	}
}

func Fatal(v ...any) {
	if LogLevel <= FATAL {
		log.Fatal(errMsg(v...))
	}
}

func Fatalf(format string, v ...any) {
	if LogLevel <= FATAL {
		log.Fatalf(errMsg(fmt.Sprintf(format, v...)))
	}
}

func errMsg(values ...any) string {
	valuesStr := []string{errPrefix}
	for _, v := range values {
		valuesStr = append(valuesStr, fmt.Sprint(v))
	}
	return color.Red.Sprint(strings.Join(valuesStr, " "))
}
