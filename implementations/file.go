package implementations

import (
	"io"
)

type FileFormat string

const (
	CSV      FileFormat = "csv"
	CSV_GZIP FileFormat = "csv_gzip"
	JSON     FileFormat = "json"
)

type FileAdapter interface {
	io.Closer
	UploadBytes(fileName string, fileBytes []byte) error
	Upload(fileName string, fileReader io.ReadSeeker) error
	DeleteObject(key string) error
	Format() FileFormat
}

type FileConfig struct {
	Folder string     `mapstructure:"folder,omitempty" json:"folder,omitempty" yaml:"folder,omitempty"`
	Format FileFormat `mapstructure:"format,omitempty" json:"format,omitempty" yaml:"format,omitempty"`
	//	Compression FileCompression    `mapstructure:"compression,omitempty" json:"compression,omitempty" yaml:"compression,omitempty"`
}

//func (c FileConfig) PrepareFile(fileName *string, fileBytes *[]byte) error {
//	if c.Folder != "" {
//		*fileName = c.Folder + "/" + *fileName
//	}
//
//	if c.Compression == FileCompressionGZIP {
//		*fileName = fileNameGZIP(*fileName)
//		if fileBytes != nil {
//			var err error
//			buf, err := compressGZIP(*fileBytes)
//			if err != nil {
//				return fmt.Errorf("Error compressing file %v", err)
//			}
//
//			*fileBytes = buf.Bytes()
//		}
//	}
//
//	return nil
//}

//func (c *FileConfig) RequireDefaultStage(storageType string) {
//	if c.Folder != "" {
//		logging.Warnf("customizing folder [%s] is not supported for [%s] stage, using root directory", c.Folder, storageType)
//		c.Folder = ""
//	}
//
//	if c.Compression != "" {
//		logging.Warnf("customizing compression [%s] is not supported for [%s] stage, using no compression", c.Compression, storageType)
//		c.Compression = ""
//	}
//}

//func fileNameGZIP(fileName string) string {
//	return fileName + ".gz"
//}
//
//func compressGZIP(b []byte) (*bytes.Buffer, error) {
//	buf := new(bytes.Buffer)
//	w := gzip.NewWriter(buf)
//	defer w.Close()
//	if _, err := w.Write(b); err != nil {
//		return nil, err
//	}
//	return buf, nil
//}
