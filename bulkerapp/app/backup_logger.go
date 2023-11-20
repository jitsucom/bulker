package app

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

type BackupLogger struct {
	appbase.Service
	sync.Mutex
	config  *Config
	loggers sync.Map
}

func NewBackupLogger(config *Config) *BackupLogger {
	base := appbase.NewServiceBase("backup_logger")
	return &BackupLogger{
		Service: base,
		config:  config,
	}
}

func (bl *BackupLogger) Log(logger string, event any) (err error) {
	if bl.config.BackupLogDir == "" {
		return
	}
	var rwriter any
	var ok bool
	if rwriter, ok = bl.loggers.Load(logger); !ok {
		bl.Lock()
		if rwriter, ok = bl.loggers.Load(logger); !ok {
			filename := path.Join(bl.config.BackupLogDir, logger, logger+".log")
			bl.Infof("Creating rolling writer for [%s]", filename)
			rwriter = NewRollingWriter(bl.config, filename)
			bl.loggers.Store(logger, rwriter)
		}
		bl.Unlock()
	}
	buff := bytes.Buffer{}
	if data, ok := event.([]byte); ok {
		_, err = base64.NewEncoder(base64.RawStdEncoding, &buff).Write(data)
		buff.Write([]byte("\n"))
	} else {
		err = json.NewEncoder(&buff).Encode(event)
	}
	if err != nil {
		return
	}
	_, err = (rwriter.(io.WriteCloser)).Write(buff.Bytes())
	return
}

func (bl *BackupLogger) Close() error {
	bl.loggers.Range(func(key, value any) bool {
		_ = (value.(io.WriteCloser)).Close()
		return true
	})
	return nil

}

// RollingWriterProxy for lumberjack.Logger
// Rotate() only if file isn't empty
type RollingWriterProxy struct {
	fileName string
	lWriter  *lumberjack.Logger

	wasWritten atomic.Bool
	closed     chan struct{}
}

func NewRollingWriter(config *Config, filename string) io.WriteCloser {
	lWriter := &lumberjack.Logger{
		Filename: filename,
		MaxSize:  config.BackupLogMaxSizeMb,
		MaxAge:   config.BackupLogTTL, //days
		Compress: true,
	}
	rwp := &RollingWriterProxy{fileName: filename, lWriter: lWriter, closed: make(chan struct{})}

	if config.BackupLogRotateHours > 0 {
		ticker := time.NewTicker(time.Duration(config.BackupLogRotateHours) * time.Hour)
		safego.RunWithRestart(func() {
			////initial rotate
			//if err := rwp.lWriter.Rotate(); err != nil {
			//	log.Errorf("Error initial rotating log file [%s]: %v", rwp.lWriter.Filename, err)
			//}
			for {
				select {
				case <-rwp.closed:
					ticker.Stop()
					return
				case <-ticker.C:
					rwp.rotate()
				}
			}
		})
	}
	return rwp
}

func (rwp *RollingWriterProxy) rotate() {
	if rwp.wasWritten.CompareAndSwap(true, false) {
		if err := rwp.lWriter.Rotate(); err != nil {
			logging.Errorf("Error rotating log file [%s]: %v", rwp.lWriter.Filename, err)
		}
	}
}

func (rwp *RollingWriterProxy) Write(p []byte) (int, error) {
	rwp.wasWritten.Store(true)
	return rwp.lWriter.Write(p)
}

func (rwp *RollingWriterProxy) Close() error {
	logging.Infof("Closing rolling writer for [%s]", rwp.fileName)
	close(rwp.closed)
	//rwp.rotate()
	return rwp.lWriter.Close()
}
