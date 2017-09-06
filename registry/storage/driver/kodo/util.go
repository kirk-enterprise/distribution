// +build include_kodo

package kodo

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

type TransportWithLogger struct {
	*http.Transport
}

func NewTransportWithLogger() *TransportWithLogger {
	return &TransportWithLogger{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func (t *TransportWithLogger) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	start := time.Now()
	logStart(start, req)
	resp, err = t.Transport.RoundTrip(req)
	logEnd(start, req, resp, err)
	return
}

type RespError interface {
	ErrorDetail() string
	Error() string
	HttpCode() int
}

func logStart(start time.Time, req *http.Request) {
	id := strconv.FormatInt(start.UnixNano(), 36)
	addr := req.Host + " - " + req.URL.String()
	logrus.Infof("[REQ_BEG][%s] %s %s", id, req.Method, addr)
}

func logEnd(start time.Time, req *http.Request, resp *http.Response, err error) {
	var (
		respReqId, xlog string
		code            int
		extra           string

		addr     = req.Host + " - " + req.URL.String()
		elaplsed = time.Since(start)
	)

	if resp != nil {
		respReqId = resp.Header.Get("X-Reqid")
		xlog = resp.Header.Get("X-Log")
		code = resp.StatusCode
	}

	if len(respReqId) > 0 {
		extra = ", RespReqId: " + respReqId
	}

	if len(xlog) > 0 {
		extra += ", Xlog: " + xlog
	}

	if err != nil {
		extra += ", Err: " + err.Error()
		if er, ok := err.(RespError); ok {
			extra += ", " + er.ErrorDetail()
		}
	}

	id := strconv.FormatInt(start.UnixNano(), 36)
	logrus.Infof("[REQ_END][%s] %s %s, Code: %d%s, Time: %dms", id, req.Method, addr, code, extra, elaplsed.Nanoseconds()/1e6)
}
