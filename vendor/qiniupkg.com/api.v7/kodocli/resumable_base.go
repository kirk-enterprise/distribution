package kodocli

import (
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"

	"qiniupkg.com/x/bytes.v7"
	"qiniupkg.com/x/rpc.v7"
	"qiniupkg.com/x/xlog.v7"

	. "golang.org/x/net/context"
)

// ----------------------------------------------------------

type uptokenTransport struct {
	token     string
	Transport http.RoundTripper
}

func (t *uptokenTransport) NestedObject() interface{} {
	return t.Transport
}

func (t *uptokenTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set("Authorization", t.token)
	return t.Transport.RoundTrip(req)
}

func newUptokenTransport(token string, transport http.RoundTripper) *uptokenTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &uptokenTransport{"UpToken " + token, transport}
}

func newUptokenClient(token string, transport http.RoundTripper) *http.Client {
	t := newUptokenTransport(token, transport)
	return &http.Client{Transport: t}
}

// ----------------------------------------------------------

func (p Uploader) Mkblk(
	ctx Context, ret *BlkputRet, blockSize int, body io.Reader, size int) error {

	url := p.UpHosts[0] + "/mkblk/" + strconv.Itoa(blockSize)
	return p.Conn.CallWith(ctx, ret, "POST", url, "application/octet-stream", body, size)
}

func (p Uploader) Bput(
	ctx Context, ret *BlkputRet, body io.Reader, size int) error {

	url := ret.Host + "/bput/" + ret.Ctx + "/" + strconv.FormatUint(uint64(ret.Offset), 10)
	return p.Conn.CallWith(ctx, ret, "POST", url, "application/octet-stream", body, size)
}

// ----------------------------------------------------------

func (p Uploader) resumableBput(
	ctx Context, ret *BlkputRet, f io.ReaderAt, blkIdx, blkSize int, extra *RputExtra) (err error) {

	log := xlog.NewWith(ctx)
	h := crc32.NewIEEE()
	offbase := int64(blkIdx) << blockBits
	chunkSize := extra.ChunkSize

	var bodyLength int

	if ret.Ctx == "" {

		if chunkSize < blkSize {
			bodyLength = chunkSize
		} else {
			bodyLength = blkSize
		}

		body1 := io.NewSectionReader(f, offbase, int64(bodyLength))
		body := io.TeeReader(body1, h)

		// todo: need check hash and length
		err = p.Mkblk(ctx, ret, blkSize, body, bodyLength)
		if err != nil {
			return
		}
		if ret.Crc32 != h.Sum32() || int(ret.Offset) != bodyLength {
			err = ErrUnmatchedChecksum
			return
		}
		extra.Notify(blkIdx, blkSize, ret)
	}

	for int(ret.Offset) < blkSize {

		if chunkSize < blkSize-int(ret.Offset) {
			bodyLength = chunkSize
		} else {
			bodyLength = blkSize - int(ret.Offset)
		}

		tryTimes := extra.TryTimes

	lzRetry:
		h.Reset()
		body1 := io.NewSectionReader(f, offbase+int64(ret.Offset), int64(bodyLength))
		body := io.TeeReader(body1, h)

		err = p.Bput(ctx, ret, body, bodyLength)
		if err == nil {
			if ret.Crc32 == h.Sum32() {
				extra.Notify(blkIdx, blkSize, ret)
				continue
			}
			log.Warn("ResumableBlockput: invalid checksum, retry")
			err = ErrUnmatchedChecksum
		} else {
			if ei, ok := err.(*rpc.ErrorInfo); ok && ei.Code == InvalidCtx {
				ret.Ctx = "" // reset
				log.Warn("ResumableBlockput: invalid ctx, please retry")
				return
			}
			log.Warn("ResumableBlockput: bput failed -", err)
		}
		if tryTimes > 1 {
			tryTimes--
			log.Info("ResumableBlockput retrying ...")
			goto lzRetry
		}
		break
	}
	return
}

// ----------------------------------------------------------

func (p Uploader) Mkfile(
	ctx Context, ret interface{}, key string, hasKey bool, fsize int64, extra *RputExtra) (err error) {

	url := p.UpHosts[0] + "/mkfile/" + strconv.FormatInt(fsize, 10)

	if extra.MimeType != "" {
		url += "/mimeType/" + encode(extra.MimeType)
	}
	if hasKey {
		url += "/key/" + encode(key)
	}
	for k, v := range extra.Params {
		url += fmt.Sprintf("/%s/%s", k, encode(v))
	}

	buf := make([]byte, 0, 176*len(extra.Progresses))
	for _, prog := range extra.Progresses {
		buf = append(buf, prog.Ctx...)
		buf = append(buf, ',')
	}
	if len(buf) > 0 {
		buf = buf[:len(buf)-1]
	}

	return p.Conn.CallWith(
		ctx, ret, "POST", url, "application/octet-stream", bytes.NewReader(buf), len(buf))
}

// ----------------------------------------------------------

func encode(raw string) string {
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// ----------------------------------------------------------
