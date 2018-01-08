// Package kodo provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu KODO cloud storage.
//
// Because KODO is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//

package kodo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"qiniupkg.com/api.v7/kodo"
	"qiniupkg.com/api.v7/kodocli"
	"qiniupkg.com/x/rpc.v7"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

// cache file writer

const driverName = "kodo"
const listMax = 1000
const defaultExpiry = 3600
const defaultChunkSize = 1 << 22 // 4mb
const minChunkSize = defaultChunkSize / 4

var DataStore = &PutStore{store: make(map[string]*writer, 0)}

// PutStore ...
type PutStore struct {
	sync.Mutex
	store map[string]*writer // key : pathkey
}

// Get ...
func (p *PutStore) Get(key string) (w *writer) {
	p.Lock()
	defer p.Unlock()
	w, _ = p.store[key]
	return
}

// Add ...
func (p *PutStore) Add(key string, w *writer) {
	p.Lock()
	defer p.Unlock()
	p.store[key] = w
	return
}

// Del ...
func (p *PutStore) Del(key string) {
	p.Lock()
	defer p.Unlock()
	delete(p.store, key)

	// trigger recycle
	keyToDelete := []string{}
	checklength := len(p.store)
	if checklength > 5 {
		checklength = 5
	}

	if checklength == 0 {
		return
	}
	for k, a := range p.store {
		checklength--
		if checklength <= 0 {
			break
		}
		if time.Now().Sub(a.created).Minutes() > 60 {
			keyToDelete = append(keyToDelete, k)
		}
	}
	for _, k := range keyToDelete {
		delete(p.store, k)
	}
}

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	Zone          int
	ChunkSize     int
	Bucket        string
	BaseURL       string
	RootDirectory string
	kodo.Config

	RedirectMap map[string]string
}

func init() {
	factory.Register(driverName, &kodoDriverFactory{})
}

type kodoDriverFactory struct {
}

func (factory *kodoDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters ...
func FromParameters(parameters map[string]interface{}) (*Driver, error) {

	params := DriverParameters{}

	params.Zone, _ = parameters["zone"].(int)

	if chuckSize, ok := parameters["chucksize"].(int); ok {
		params.ChunkSize = chuckSize
	}
	if params.ChunkSize < minChunkSize {
		params.ChunkSize = defaultChunkSize
	}

	params.Bucket = getParameter(parameters, "bucket")
	if params.Bucket == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	params.BaseURL = getParameter(parameters, "baseurl")
	if params.BaseURL == "" {
		return nil, fmt.Errorf("No baseurl parameter provided")
	}

	params.Config.AccessKey = getParameter(parameters, "accesskey")
	if params.Config.AccessKey == "" {
		return nil, fmt.Errorf("No accesskey parameter provided")
	}

	params.Config.SecretKey = getParameter(parameters, "secretkey")
	if params.Config.SecretKey == "" {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}

	params.RootDirectory = getParameter(parameters, "rootdirectory")

	params.Config.RSHost = getParameter(parameters, "rshost")
	params.Config.RSFHost = getParameter(parameters, "rsfhost")
	params.Config.IoHost = getParameter(parameters, "iohost")
	uphosts, ok := parameters["uphosts"].([]interface{})
	if ok {
		for _, a := range uphosts {
			params.Config.UpHosts = append(params.Config.UpHosts, a.(string))
		}
	}
	redirect, ok := parameters["redirect"].([]interface{})
	params.RedirectMap = make(map[string]string, 0)
	if ok {
		for _, a := range redirect {
			keyval := strings.Split(a.(string), "::")
			if len(keyval) == 2 {
				params.RedirectMap[keyval[0]] = keyval[1]
			}
		}
	}

	params.Config.Transport = NewTransportWithLogger("")

	logrus.Info("kodo.config", params)

	return New(params)
}

type baseEmbed struct {
	base.Base
}

// Driver ...
type Driver struct {
	baseEmbed
}

// New ...
func New(params DriverParameters) (*Driver, error) {

	client := kodo.New(params.Zone, &params.Config)
	bucket := client.Bucket(params.Bucket)

	params.RootDirectory = strings.TrimRight(params.RootDirectory, "/")

	if !strings.HasSuffix(params.BaseURL, "/") {
		params.BaseURL += "/"
	}

	d := &driver{
		params: params,
		client: client,
		bucket: &bucket,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

type driver struct {
	params DriverParameters
	bucket *kodo.Bucket
	client *kodo.Client
}

// Name returns the human-readable "name" of the driver, useful in error
// messages and logging. By convention, this will just be the registration
// name, but drivers may provide other information here.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	debugLog("debugkodo GetContent " + path)
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		logrus.Errorln("debugkodo GetContent", err)
		return nil, err
	}
	return body, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	debugLog("debugkodo PutContent " + path)
	err := d.bucket.Put(ctx, nil, d.getKey(path), bytes.NewBuffer(content), int64(len(content)), nil)
	if err != nil {
		logrus.Errorln("debugkodo PutContent", errorInfo(err))
	}
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	debugLog("debugkodo Reader " + path)
	policy := kodo.GetPolicy{Expires: defaultExpiry}
	baseURL := d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logrus.Errorln("debugkodo Reader", err)
		return nil, err
	}
	if offset > 0 {
		req.Header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	resp, err := d.client.Do(ctx, req)
	if err != nil {
		logrus.Errorln("debugkodo reader do req", errorInfo(err))
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	debugLog("debugkodo Writer " + path + " " + strconv.FormatBool(append))
	policy := &kodo.PutPolicy{
		Scope:   d.bucket.Name,
		Expires: 3600,
	}

	if append {
		writer := DataStore.Get(d.getKey(path))
		if writer == nil {
			debugLog("debugkodo not found")
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}
		writer.closed = false
		return writer, nil
	}
	// !appdend
	writer := &writer{
		driver:    d,
		ctx:       ctx,
		key:       d.getKey(path),
		chuckSize: d.params.ChunkSize,
		uploader: kodocli.NewUploader(d.params.Zone, &kodocli.UploadConfig{
			UpHosts:   d.params.UpHosts,
			Transport: NewTransportWithLogger("UpToken " + d.client.MakeUptoken(policy)),
		}),
	}
	err := writer.initUpload()
	if err != nil {
		logrus.Errorln("debugkodo initUpload", errorInfo(err))
	}
	DataStore.Add(writer.key, writer)
	return writer, err
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	debugLog("debugkodo Stat " + path)
	items, _, _, err := bucketlistWithRetry(ctx, d.bucket, d.getKey(path), "", "", 1)
	if err != nil {
		if err != io.EOF {
			logrus.Errorln("debugkodo Stat", errorInfo(err))
			return nil, err
		}
		err = nil
	}

	if len(items) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	item := items[0]

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if d.getKey(path) != item.Key {
		fi.IsDir = true
	}

	if !fi.IsDir {
		fi.Size = item.Fsize
		fi.ModTime = time.Unix(0, item.PutTime*100)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func bucketlistWithRetry(ctx context.Context, b *kodo.Bucket, prefix, delimiter, marker string, limit int) (entries []kodo.ListItem, commonPrefixes []string, markerOut string, err error) {
	for i := 0; i < 3; i++ {
		entries, commonPrefixes, markerOut, err = b.List(ctx, prefix, delimiter, marker, limit)
		if err != nil {
			if err1, ok := err.(*rpc.ErrorInfo); ok && err1.Code == 599 {
				logrus.Infoln("debugkodo bucketlistWithRetry triggered", errorInfo(err))
				time.Sleep(time.Millisecond * 500)
				continue
			}
		}
		break
	}
	return
}

// List returns a list of the objects that are direct descendants of the
// given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {

	path := opath

	if path != "/" && path[len(path)-1] != '/' {
		path += "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	rootPrefix := ""
	if d.getKey("") == "" {
		rootPrefix = "/"
	}

	var (
		items    []kodo.ListItem
		marker   string
		prefixes []string
		err      error

		files       []string
		directories []string
	)

	for {
		items, prefixes, marker, err = bucketlistWithRetry(ctx, d.bucket, d.getKey(path), "/", marker, listMax)
		if err != nil {
			if err != io.EOF {
				logrus.Errorln("debugkodo bucketlistWithRetry", errorInfo(err))
				return nil, err
			}
			err = nil
		}

		for _, item := range items {
			files = append(files, strings.Replace(item.Key, d.getKey(""), rootPrefix, 1))
		}

		for _, prefix := range prefixes {
			directories = append(directories, strings.Replace(strings.TrimSuffix(prefix, "/"), d.getKey(""), rootPrefix, 1))
		}

		if marker == "" {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			return nil, storagedriver.PathNotFoundError{Path: opath, DriverName: driverName}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	debugLog("debugkodo Move " + sourcePath + " " + destPath)
	err := d.bucket.Move(ctx, d.getKey(sourcePath), d.getKey(destPath), true)
	if err != nil {
		logrus.Errorln("debugkodo Move", errorInfo(err))
	}
	return parseError(sourcePath, err)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	debugLog("debugkodo Delete " + path)
	var (
		items  []kodo.ListItem
		marker string
		err    error

		cnt int
	)

	for {
		items, _, marker, err = bucketlistWithRetry(ctx, d.bucket, d.getKey(path), "", marker, listMax)
		if err != nil {
			if err != io.EOF {
				logrus.Errorln("debugkodo Delete bucketlistWithRetry", errorInfo(err))
				return err
			}
			err = nil
		}

		cnt += len(items)
		if cnt == 0 {
			return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}

		for _, item := range items {
			err = d.bucket.Delete(ctx, item.Key)
			if err != nil {
				if isKeyNotExists(err) {
					continue
				}
				return err
			}
		}

		if marker == "" {
			break
		}
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// May return an ErrUnsupportedMethod in certain StorageDriver
// implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	var baseURL string
	policy := kodo.GetPolicy{Expires: defaultExpiry}

	if expiresTime, ok := options["expiry"].(time.Time); ok {
		if expires := expiresTime.Unix() - time.Now().Unix(); expires > 0 {
			policy.Expires = uint32(expires)
		}
	}

	if host, ok := options["host"].(string); ok {
		for key, val := range d.params.RedirectMap {
			if strings.Contains(host, key) {
				baseURL = val + d.getKey(path)
				url := d.client.MakePrivateUrl(baseURL, &policy)
				logrus.Info("URLFor ", key, val)
				return url, nil
			}
		}
	}
	logrus.Info("URLFor ", options["host"])
	baseURL = d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)
	return url, nil
}

func (d *driver) getKey(path string) string {
	return strings.TrimLeft(d.params.RootDirectory+path, "/")
}

// writer provides an abstraction for an opened writable file-like object in
// the storage backend. The writer must flush all content written to it on
// the call to Close, but is only required to make its content readable on a
// call to Commit.
type writer struct {
	created   time.Time
	ctx       context.Context
	driver    *driver
	key       string
	chuckSize int
	uploader  kodocli.Uploader
	parts     []*kodocli.BlkputRet
	size      int64
	buffer    []byte
	closed    bool
	committed bool
	cancelled bool
	wg        sync.WaitGroup
}

func (w *writer) Write(p []byte) (n int, err error) {
	debugLog("debugkodo Write " + strconv.Itoa(len(p)) + " " + strconv.Itoa(len(w.buffer)))
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part

		if neededBytes := w.chuckSize - len(w.buffer); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.buffer = append(w.buffer, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart(false)
				if err != nil {
					w.size += int64(n)
					return n, err
				}
			} else {
				w.buffer = append(w.buffer, p...)
				n += len(p)
				p = nil
			}
		}
	}
	w.size += int64(n)
	return
}

// Size returns the number of bytes written to this FileWriter.
func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	debugLog("debugkodo Close " + w.key)
	if w.closed {
		logrus.Errorln("debugkodo writer close: already closed")
		//return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart(false)
}

// Cancel removes any written content from this FileWriter.
func (w *writer) Cancel() error {
	debugLog("debugkodo Cancel " + w.key)
	if w.closed {
		logrus.Errorln("debugkodo writer cancel: already closed")
		return fmt.Errorf("already closed")
	} else if w.committed {
		logrus.Errorln("debugkodo writer commit: already closed")
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	DataStore.Del(w.key)
	// kodo目前不支持放弃上传的操作
	// err := w.upload.Abort()
	return nil
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver./ and
// StorageDriver.Reader.
func (w *writer) Commit() error {
	debugLog("debugkodo Commit " + w.key)
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}

	err := w.flushPart(true)
	if err != nil {
		return err
	}
	w.committed = true
	err = w.Complete()
	if err != nil {
		// kodo目前不支持放弃上传的操作
		// w.upload.Abort()
		return err
	}
	DataStore.Del(w.key)
	return nil
}

func (w *writer) waitAndGetParts() (ret []kodocli.BlkputRet, err error) {
	a := ""
	w.wg.Wait()
	for i, p := range w.parts {
		if p.Offset > 0 {
			a += fmt.Sprintf("%d %v ; ", i, p)
			ret = append(ret, *(w.parts[i]))
		}
		if p.Offset == 0 && p.Ctx != "init" {
			err = errors.New("some part has error")
		}
	}
	logrus.Infoln("debugkodo blks", a)

	return
}

func (w *writer) Complete() (err error) {
	debugLog("debugkodo Complete " + w.key)
	parts, err := w.waitAndGetParts()
	if err != nil {
		logrus.Errorln("debugkodo Mkfile error", errorInfo(err))
		return
	}
	ret := kodocli.PutRet{}
	for i := 0; i < 3; i++ {
		err = w.uploader.Mkfile(context.Background(), &ret, w.key, true, w.size, &kodocli.RputExtra{
			Progresses: parts,
		})
		if err != nil {
			logrus.Errorln("debugkodo Mkfile error", errorInfo(err))
		} else {
			break
		}
	}
	return
}

func debugLog(header string) {
	// pc := make([]uintptr, 8, 8)
	// cnt := runtime.Callers(1, pc)
	logrus.Infoln(header)
	// for i := 0; i < cnt; i++ {
	// 	fu := runtime.FuncForPC(pc[i] - 1)

	// 	file, line := fu.FileLine(pc[i] - 1)
	// 	logrus.Infoln(fmt.Sprintf("%s:%d", filepath.Base(file), line))
	// }
	return
}

func (w *writer) initUpload() (err error) {
	// hack init
	part := &kodocli.BlkputRet{
		Ctx: "init",
	}
	w.parts = append(w.parts, part)
	return
}

// flushPart flushes buffers to write a part to kodo.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart(isCommit bool) (err error) {
	debugLog("debugkodo flushPart " + w.key + " " + strconv.Itoa(len(w.buffer)))
	if len(w.buffer) != w.chuckSize && !isCommit {
		// nothing to write
		return nil
	}

	part := &kodocli.BlkputRet{}
	w.wg.Add(1)
	go func(buffer []byte) {
		for i := 0; i < 3; i++ {
			// todo: optimize retry logic and error/checksum check
			err = w.uploader.Mkblk(context.Background(),
				part, len(buffer), bytes.NewReader(buffer), len(buffer))
			if err != nil {
				logrus.Errorln("debugkodo Mkblk error", errorInfo(err))
			} else {
				logrus.Infoln("debugkodo Mkblk success", part)
				break
			}
		}
		w.wg.Done()
	}(w.buffer)

	if err != nil {
		return err
	}

	w.parts = append(w.parts, part)
	w.buffer = nil
	return nil
}

func isKeyNotExists(err error) bool {
	if er, ok := err.(*rpc.ErrorInfo); ok && er.Code == 612 {
		return true
	}
	return false
}

func parseError(path string, err error) error {
	if er, ok := err.(*rpc.ErrorInfo); ok && er.Code == 612 {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return err
}

func getParameter(parameters map[string]interface{}, key string) (value string) {
	if v, ok := parameters[key]; !ok || v == nil {
		return
	} else {
		value = fmt.Sprint(v)
	}
	return
}

func errorInfo(err error) string {
	if err1, ok := err.(*rpc.ErrorInfo); ok {
		return err1.ErrorDetail()
	}
	if err != nil {
		return err.Error()
	}
	return ""
}
