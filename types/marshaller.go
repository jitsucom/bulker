package types

import (
	"encoding/csv"
	"encoding/json"
	"io"
)

const quotaByteValue = 34

var (
	JSONMarshallerInstance = JSONMarshaller{}
	CSVMarshallerInstance  = CSVMarshaller{}
)

type Marshaller interface {
	Marshal([]string, io.Writer, ...Object) error
	WriteHeader([]string, io.Writer) error
	NeedHeader() bool
	Format() string
}

type JSONMarshaller struct {
}

// Marshal object as json
func (jm JSONMarshaller) Marshal(fields []string, writer io.Writer, object ...Object) error {
	for _, obj := range object {
		bytes, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = writer.Write(bytes)
		if err != nil {
			return err
		}
		_, err = writer.Write([]byte("\n"))
		return err
	}
	return nil
}

func (jm JSONMarshaller) NeedHeader() bool {
	return false
}

func (jm JSONMarshaller) WriteHeader(fields []string, writer io.Writer) error {
	return nil
}

func (jm JSONMarshaller) Format() string {
	return "json"
}

type CSVMarshaller struct {
}

// Marshal marshals input object as csv values string with delimiter
func (cm CSVMarshaller) Marshal(fields []string, writer io.Writer, object ...Object) error {
	csvWriter := csv.NewWriter(writer)
	valuesArr := make([]string, len(fields))
	for _, obj := range object {
		for i, field := range fields {
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
		err := csvWriter.Write(valuesArr)
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return nil
}

func (cm CSVMarshaller) NeedHeader() bool {
	return true
}

func (cm CSVMarshaller) Format() string {
	return "csv"
}

func (cm CSVMarshaller) WriteHeader(fields []string, writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)
	err := csvWriter.Write(fields)
	if err != nil {
		return err
	}
	csvWriter.Flush()
	return nil
}
