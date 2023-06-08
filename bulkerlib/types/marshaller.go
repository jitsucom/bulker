package types

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"io"
)

const quotaByteValue = 34

type Marshaller interface {
	Init(writer io.Writer, header []string) error
	Marshal(...Object) error
	Flush() error
	NeedHeader() bool
	Format() FileFormat
	Compression() FileCompression
	Equal(Marshaller) bool
}

type AbstractMarshaller struct {
	format      FileFormat
	compression FileCompression
}

func (am *AbstractMarshaller) Equal(m Marshaller) bool {
	return am.format == m.Format() && am.compression == m.Compression()
}

func NewMarshaller(format FileFormat, compression FileCompression) (Marshaller, error) {
	switch format {
	case FileFormatCSV:
		return &CSVMarshaller{AbstractMarshaller: AbstractMarshaller{format: format, compression: compression}}, nil
	case FileFormatNDJSON, FileFormatNDJSONFLAT:
		return &JSONMarshaller{AbstractMarshaller: AbstractMarshaller{format: format, compression: compression}}, nil
	default:
		return nil, fmt.Errorf("Unknown file format: %s", format)
	}
}

type JSONMarshaller struct {
	AbstractMarshaller
	writer io.Writer
}

func (jm *JSONMarshaller) Init(writer io.Writer, _ []string) error {
	if jm.writer == nil {
		if jm.compression == FileCompressionGZIP {
			jm.writer = gzip.NewWriter(writer)
		} else {
			jm.writer = writer
		}
	}
	return nil
}

// Marshal object as json
func (jm *JSONMarshaller) Marshal(object ...Object) error {
	if jm.writer == nil {
		return fmt.Errorf("marshaller wasn't initialized. Run Init() first")
	}
	for _, obj := range object {
		bytes, err := jsoniter.Marshal(obj)
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
	if jm.writer == nil {
		return fmt.Errorf("marshaller wasn't initialized. Run Init() first")
	}
	if jm.compression == FileCompressionGZIP {
		return jm.writer.(*gzip.Writer).Close()
	}
	return nil
}

func (jm *JSONMarshaller) NeedHeader() bool {
	return false
}

func (jm *JSONMarshaller) Format() FileFormat {
	return jm.format
}

func (jm *JSONMarshaller) Compression() FileCompression {
	return jm.compression
}

type CSVMarshaller struct {
	AbstractMarshaller
	writer     *csv.Writer
	gzipWriter *gzip.Writer
	fields     []string
}

func (cm *CSVMarshaller) Init(writer io.Writer, header []string) error {
	if cm.writer == nil {
		if cm.compression == FileCompressionGZIP {
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
	}
	return nil
}

// Marshal marshals input object as csv values string with delimiter
func (cm *CSVMarshaller) Marshal(object ...Object) error {
	if cm.writer == nil {
		return fmt.Errorf("marshaller wasn't initialized. Run Init() first")
	}
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
					b, err := jsoniter.Marshal(v)
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

func (cm *CSVMarshaller) Format() FileFormat {
	return cm.format
}

func (cm *CSVMarshaller) Compression() FileCompression {
	return cm.compression
}

func (cm *CSVMarshaller) Flush() error {
	if cm.writer == nil {
		return fmt.Errorf("marshaller wasn't initialized. Run Init() first")
	}
	cm.writer.Flush()
	if cm.gzipWriter != nil {
		return cm.gzipWriter.Close()
	}
	return nil
}

type FileFormat string

const (
	FileFormatCSV        FileFormat = "csv"
	FileFormatNDJSON     FileFormat = "ndjson"
	FileFormatNDJSONFLAT FileFormat = "ndjson_flat"
)

type FileCompression string

const (
	FileCompressionGZIP    FileCompression = "gzip"
	FileCompressionNONE    FileCompression = "none"
	FileCompressionUNKNOWN FileCompression = ""
)
