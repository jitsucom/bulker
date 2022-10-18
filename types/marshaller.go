package types

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
)

const quotaByteValue = 34

type Marshaller interface {
	Init(writer io.Writer, header []string) error
	Marshal(...Object) error
	Flush() error
	NeedHeader() bool
	Format() string
}

type JSONMarshaller struct {
	gzip       bool
	writer     io.Writer
	gzipWriter *gzip.Writer
}

func (jm *JSONMarshaller) Init(writer io.Writer, header []string) error {
	if jm.gzip {
		jm.gzipWriter = gzip.NewWriter(writer)
		jm.writer = jm.gzipWriter
	} else {
		jm.writer = writer
	}
	return nil
}

// Marshal object as json
func (jm *JSONMarshaller) Marshal(object ...Object) error {
	for _, obj := range object {
		bytes, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = jm.writer.Write(bytes)
		if err != nil {
			return err
		}
		_, err = jm.writer.Write([]byte("\n"))
		return err
	}
	return nil
}

func (jm *JSONMarshaller) Flush() error {
	if jm.gzipWriter != nil {
		return jm.gzipWriter.Close()
	}
	return nil
}

func (jm *JSONMarshaller) NeedHeader() bool {
	return false
}

func (jm *JSONMarshaller) WriteHeader(fields []string, writer io.Writer) error {
	return nil
}

func (jm *JSONMarshaller) Format() string {
	return "json"
}

type CSVMarshaller struct {
	Gzip       bool
	writer     *csv.Writer
	gzipWriter *gzip.Writer
	fields     []string
}

func (cm *CSVMarshaller) Init(writer io.Writer, header []string) error {
	if cm.Gzip {
		cm.gzipWriter = gzip.NewWriter(writer)
		cm.writer = csv.NewWriter(cm.gzipWriter)
	} else {
		cm.writer = csv.NewWriter(writer)
	}
	cm.fields = header
	err := cm.writer.Write(header)
	if err != nil {
		return err
	}
	return nil
}

// Marshal marshals input object as csv values string with delimiter
func (cm *CSVMarshaller) Marshal(object ...Object) error {
	valuesArr := make([]string, len(cm.fields))
	for _, obj := range object {
		for i, field := range cm.fields {
			v, _ := obj[field]
			strValue := ""
			if v == nil {
				strValue = "\\N"
			} else {
				switch v := v.(type) {
				case string:
					strValue = v
				case bool:
					if v {
						strValue = "1"
					} else {
						strValue = "0"
					}
				default:
					//use json marshaller to marshal types like arrays and time in unified way
					b, err := json.Marshal(v)
					if err != nil {
						return err
					}
					//don't write begin and end quotas
					lastIndex := len(b) - 1
					if len(b) >= 2 && b[0] == quotaByteValue && b[lastIndex] == quotaByteValue {
						b = b[1:lastIndex]
					}
					strValue = string(b)
				}
			}
			valuesArr[i] = strValue
		}
		//logging.Info("Writing values: ", valuesArr)
		err := cm.writer.Write(valuesArr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *CSVMarshaller) NeedHeader() bool {
	return true
}

func (cm *CSVMarshaller) Format() string {
	return "csv"
}

func (cm *CSVMarshaller) Flush() error {
	cm.writer.Flush()
	if cm.gzipWriter != nil {
		return cm.gzipWriter.Close()
	}
	return nil
}
