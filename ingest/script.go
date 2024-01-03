package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"io"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Script struct {
	appbase.Service
	refreshPeriodSec int
	scriptOrigin     string
	cacheDir         string
	scriptCode       atomic.Pointer[[]byte]
	gzippedCode      atomic.Pointer[[]byte]
	etag             atomic.Pointer[string]
	closed           chan struct{}
}

func NewScript(scriptOrigin, cacheDir string) *Script {
	base := appbase.NewServiceBase("script")
	s := &Script{
		Service:          base,
		scriptOrigin:     scriptOrigin,
		refreshPeriodSec: 120,
		cacheDir:         cacheDir,
		closed:           make(chan struct{}),
	}
	s.refresh()
	s.start()
	return s
}

func (s *Script) loadCached() {
	file, err := os.Open(path.Join(s.cacheDir, "p.js"))
	if err != nil {
		s.Fatalf("Error opening cached script: %v\nCannot serve without p.js script. Exitting...", err)
		return
	}
	stat, err := file.Stat()
	if err != nil {
		s.Fatalf("Error getting cached script info: %v\nCannot serve without p.js script. Exitting...", err)
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		s.Fatalf("Cached script is empty\nCannot serve without p.js script. Exitting...")
		return
	}
	code, err := io.ReadAll(file)
	if err != nil {
		s.Fatalf("Error reading cached script: %v\nCannot serve without p.js script. Exitting...", err)
		return
	}
	lastModified := stat.ModTime()
	s.Infof("Loaded cached script: %d bytes, last modified: %v", fileSize, lastModified)
	s.scriptCode.Store(&code)
	s.gzip(code)

}
func (s *Script) gzip(code []byte) {
	buf := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(buf)
	_, err := writer.Write(code)
	if err == nil {
		err = writer.Close()
		if err == nil {
			gzippedCode := buf.Bytes()
			s.gzippedCode.Store(&gzippedCode)
		}
	}
}

func (s *Script) storeCached(payload []byte) {
	filePath := path.Join(s.cacheDir, "p.js")
	err := os.MkdirAll(s.cacheDir, 0755)
	if err != nil {
		s.Errorf("Cannot write cached script to %s: cannot make dir: %v", filePath, err)
		return
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		s.Errorf("Cannot write cached script to %s: %v", filePath, err)
		return
	}
	_, err = file.Write(payload)
	if err != nil {
		s.Errorf("Cannot write cached script to %s: %v", filePath, err)
		return
	}
	err = file.Sync()
	if err != nil {
		s.Errorf("Cannot write cached script to %s: %v", filePath, err)
		return
	}
}

func (s *Script) refresh() {
	var err error
	defer func() {
		if err != nil {
			currentCode := s.scriptCode.Load()
			s.Errorf("Error loading script: %v", err)
			if currentCode == nil {
				if s.cacheDir != "" {
					s.loadCached()
				} else {
					s.Fatalf("Cannot load cached version. No CACHE_DIR is set. Cannot serve without p.js script. Exitting...")
					return
				}
			}
		}
	}()
	//panic handler
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\nstack:\n%s", r, string(debug.Stack()))
		}
	}()

	var body []byte
	ifNoneMatch := s.etag.Load()
	var etag string
	for i := 0; i < 5; i++ {
		body, etag, err = s.loadScript(ifNoneMatch)
		if err != nil {
			s.Errorf("Attempt #%d: %v", i+1, err)
			time.Sleep(1 * time.Second)
			continue
		}
		if ifNoneMatch != nil && etag == *ifNoneMatch {
			s.Debugf("Script wasn't modified. Etag: %s", etag)
			return
		}
		s.Infof("Script loaded. Etag: %s", etag)
		s.scriptCode.Store(&body)
		s.gzip(body)
		s.etag.Store(&etag)
		if s.cacheDir != "" {
			s.storeCached(body)
		}
		return
	}
}

func (s *Script) loadScript(ifNoneMatch *string) (body []byte, etag string, err error) {
	req, err := http.NewRequest("GET", s.scriptOrigin, nil)
	if err != nil {
		err = s.NewError("Error creating request for loading script from %s: %v", s.scriptOrigin, err)
		return
	}
	if ifNoneMatch != nil {
		s.Debugf("Loading script from %s with If-None-Match: %v", s.scriptOrigin, *ifNoneMatch)
		req.Header.Add("If-None-Match", *ifNoneMatch)
	} else {
		s.Debugf("Loading script from %s", s.scriptOrigin)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("Error loading script from %s: %v", s.scriptOrigin, err)
		return
	}
	if resp.StatusCode == http.StatusNotModified {
		etag = *ifNoneMatch
		return
	} else if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		err = fmt.Errorf("Error loading script from %s http status: %v resp: %s", s.scriptOrigin, resp.StatusCode, string(b))
		return
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if len(b) == 0 {
		err = fmt.Errorf("Error loading script from %s empty response", s.scriptOrigin)
		return
	}
	etag = resp.Header.Get("etag")
	return b, etag, nil
}

func (s *Script) start() {
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(time.Duration(s.refreshPeriodSec) * time.Second)
		for {
			select {
			case <-ticker.C:
				s.refresh()
			case <-s.closed:
				ticker.Stop()
				return
			}
		}
	})
}

func (s *Script) GetScript() []byte {
	return *s.scriptCode.Load()
}

func (s *Script) GetEtag() *string {
	return s.etag.Load()
}

func (s *Script) WriteScript(c *gin.Context, head bool, gzip bool) {
	c.Header("Cache-Control", "public, max-age=120")
	etag := s.etag.Load()
	var body []byte
	if gzCode := s.gzippedCode.Load(); gzip && gzCode != nil {
		c.Header("Content-Encoding", "gzip")
		c.Header("Vary", "Accept-Encoding")
		body = *gzCode
	} else {
		body = *s.scriptCode.Load()
	}
	if etag != nil {
		c.Header("ETag", *etag)
	}
	c.Header("Content-Length", fmt.Sprintf("%d", len(body)))
	if head {
		c.Header("Content-Type", "application/javascript")
		c.Status(http.StatusOK)
		return
	} else {
		c.Data(http.StatusOK, "application/javascript", body)
	}
}

func (s *Script) Close() {
	close(s.closed)
}
