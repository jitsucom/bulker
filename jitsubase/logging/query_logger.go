package logging

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"io"
	"log"
	"strings"
)

const (
	DDLLogerType      = "ddl-debug"
	QueriesLoggerType = "sql-debug"
)

type SQLDebugConfig struct {
	DDL     *LoggerConfig `mapstructure:"ddl" json:"ddl,omitempty" yaml:"ddl,omitempty"`
	Queries *LoggerConfig `mapstructure:"queries" json:"queries,omitempty" yaml:"queries,omitempty"`
}

type LoggerConfig struct {
	Path        string `mapstructure:"path" json:"path,omitempty" yaml:"path,omitempty"`
	RotationMin int64  `mapstructure:"rotation_min" json:"rotation_min,omitempty" yaml:"rotation_min,omitempty"`
	MaxBackups  int    `mapstructure:"max_backups" json:"max_backups,omitempty" yaml:"max_backups,omitempty"`
}

type QueryLogger struct {
	queryLogger *log.Logger
	ddlLogger   *log.Logger
	identifier  string
}

func NewQueryLogger(identifier string, ddlWriter io.Writer, queryWriter io.Writer) *QueryLogger {
	var queryLogger *log.Logger
	if queryWriter != nil {
		queryLogger = log.New(DateTimeWriterProxy{writer: queryWriter}, "", 0)
	}
	var ddlLogger *log.Logger
	if ddlWriter != nil {
		ddlLogger = log.New(DateTimeWriterProxy{writer: ddlWriter}, "", 0)
	}
	return &QueryLogger{identifier: identifier, queryLogger: queryLogger, ddlLogger: ddlLogger}
}

//func (l *QueryLogger) LogDDL(query string) {
//	if l.ddlLogger != nil {
//		l.ddlLogger.Printf("%s [%s] %s\n", debugPrefix, l.identifier, query)
//	}
//}
//
//func (l *QueryLogger) LogQuery(query string) {
//	if l.queryLogger != nil {
//		l.queryLogger.Printf("%s [%s] %s\n", debugPrefix, l.identifier, query)
//	}
//}

func (l *QueryLogger) LogQuery(query string, err error, values ...any) {
	var logger *log.Logger
	if strings.HasPrefix(query, "CREATE") ||
		strings.HasPrefix(query, "DROP") ||
		strings.HasPrefix(query, "PATCH") ||
		strings.HasPrefix(query, "ALTER") ||
		strings.HasPrefix(query, "RENAME") {
		logger = l.ddlLogger
	} else {
		logger = l.queryLogger

	}
	if logger != nil {
		valuesString := ""
		var stringBuilder strings.Builder
		for i, value := range values {
			if i > 0 {
				stringBuilder.WriteString(", ")
			}
			stringBuilder.WriteString(fmt.Sprint(value))
		}
		if stringBuilder.Len() > 0 {
			valuesString = "; values: [" + stringBuilder.String() + "]"
		}
		levelPrefix := debugPrefix
		errorString := ""
		if err != nil {
			levelPrefix = errPrefix
			errorString = "; error: " + err.Error()
		}
		logger.Printf("%s [%s] %s%s%s\n", levelPrefix, l.identifier, strings.ReplaceAll(utils.ShortenStringWithEllipsis(query, 1000), "\n", ""), errorString, utils.ShortenStringWithEllipsis(valuesString, 1000))
	}
}

//func (l *QueryLogger) LogQueryWithValues(query string, values []any) {
//	if l.queryLogger != nil {
//		var stringValues []string
//		for _, value := range values {
//			stringValues = append(stringValues, fmt.Sprint(value))
//		}
//		l.queryLogger.Printf("%s [%s] %s; values: [%s]\n", debugPrefix, l.identifier, query, strings.Join(stringValues, ", "))
//	}
//}
