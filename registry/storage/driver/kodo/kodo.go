// Package kodo provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu KODO cloud storage.
//
// Because KODO is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// +build include_kodo

package kodo

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"qiniupkg.com/api.v7/auth/qbox"
	"qiniupkg.com/api.v7/kodo"
	"qiniupkg.com/x/rpc.v7"

	"github.com/docker/distribution/context"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "kodo"
const listMax = 1000
const defaultExpiry = 3600

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	Zone          int
	Bucket        string
	BaseURL       string
	RootDirectory string
	kodo.Config

	UserUid    uint64
	AdminAk    string
	AdminSk    string
	RefreshURL string
}

func init() {
	factory.Register(driverName, &kodoDriverFactory{})
}

type kodoDriverFactory struct {
}

func (factory *kodoDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {

	var err error

	params := DriverParameters{}

	params.Zone, _ = parameters["zone"].(int)

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

	params.AdminAk = getParameter(parameters, "adminaccesskey")
	if params.AdminAk == "" {
		return nil, fmt.Errorf("No adminaccesskey parameter provided")
	}

	params.AdminSk = getParameter(parameters, "adminsecretkey")
	if params.AdminSk == "" {
		return nil, fmt.Errorf("No adminsecretkey parameter provided")
	}

	userUid := getParameter(parameters, "useruid")
	if userUid == "" {
		return nil, fmt.Errorf("No useruid parameter provided")
	}
	params.UserUid, err = strconv.ParseUint(userUid, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("useruid format err: %v", err)
	}

	params.RefreshURL = getParameter(parameters, "refreshurl")
	if params.RefreshURL == "" {
		return nil, fmt.Errorf("No refreshurl parameter provided")
	}

	params.RootDirectory = getParameter(parameters, "rootdirectory")

	params.Config.RSHost = getParameter(parameters, "rshost")
	params.Config.RSFHost = getParameter(parameters, "rsfhost")
	params.Config.IoHost = getParameter(parameters, "iohost")
	params.Config.UpHosts, _ = parameters["uphosts"].([]string)

	params.Config.Transport = NewTransportWithLogger()

	return New(params)
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func New(params DriverParameters) (*Driver, error) {

	client := kodo.New(params.Zone, &params.Config)
	bucket := client.Bucket(params.Bucket)
	refresh := qbox.NewClient(qbox.NewMac(params.AdminAk, params.AdminSk), params.Config.Transport)

	params.RootDirectory = strings.TrimRight(params.RootDirectory, "/")

	if !strings.HasSuffix(params.BaseURL, "/") {
		params.BaseURL += "/"
	}

	d := &driver{
		params:    params,
		client:    client,
		bucket:    &bucket,
		refresh:   refresh,
		refreshCh: make(chan string, 100),
	}

	for i := 0; i < 10; i++ {
		go d.refreshWorker()
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
	params    DriverParameters
	bucket    *kodo.Bucket
	client    *kodo.Client
	refresh   *http.Client
	refreshCh chan string
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

	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {

	err := d.bucket.Put(ctx, nil, d.getKey(path), bytes.NewBuffer(content), int64(len(content)), nil)
	if err == nil {
		return d.refreshCache(d.getKey(path))
	}
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	policy := kodo.GetPolicy{Expires: defaultExpiry}
	baseURL := d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	resp, err := d.client.Do(ctx, req)
	if err != nil {
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

	var offset int64
	if append {
		stat, err := d.Stat(ctx, path)
		if err != nil {
			pathNotFoundErr := storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
			if err != pathNotFoundErr {
				return nil, err
			}
		} else {
			offset = stat.Size()
		}
	}
	return newFileWriter(d, ctx, path, offset), nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {

	items, _, _, err := d.bucket.List(ctx, d.getKey(path), "", "", 1)
	if err != nil {
		if err != io.EOF {
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
		items, prefixes, marker, err = d.bucket.List(ctx, d.getKey(path), "/", marker, listMax)
		if err != nil {
			if err != io.EOF {
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

	err := d.bucket.Move(ctx, d.getKey(sourcePath), d.getKey(destPath), true)
	return parseError(sourcePath, err)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {

	var (
		items  []kodo.ListItem
		marker string
		err    error

		cnt int
	)

	for {
		items, _, marker, err = d.bucket.List(ctx, d.getKey(path), "", marker, listMax)
		if err != nil {
			if err != io.EOF {
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
			err = d.refreshCache(item.Key)
			if err != nil {
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

	policy := kodo.GetPolicy{Expires: defaultExpiry}

	if expiresTime, ok := options["expiry"].(time.Time); ok {
		if expires := expiresTime.Unix() - time.Now().Unix(); expires > 0 {
			policy.Expires = uint32(expires)
		}
	}

	baseURL := d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)
	return url, nil
}

func (d *driver) refreshCache(key string) (err error) {
	// async
	d.refreshCh <- key
	// sync
	// err = d.refreshCacheNow(key)
	return
}

func (d *driver) refreshCacheNow(key string) (err error) {
	memcacheKey := "io:" + strconv.FormatUint(uint64(d.params.UserUid), 36) + ":" + d.params.Bucket + ":" + key
	encodedKey := base64.URLEncoding.EncodeToString([]byte(memcacheKey))
	resp, err := d.refresh.Get(d.params.RefreshURL + "/" + encodedKey)
	if err != nil {
		logrus.Error("refresh failed:", encodedKey, err)
		return
	}
	resp.Body.Close()
	return
}

func (d *driver) refreshWorker() {
	for key := range d.refreshCh {
		d.refreshCacheNow(key)
	}
}

func (d *driver) getKey(path string) string {
	return strings.TrimLeft(d.params.RootDirectory+path, "/")
}

// writer provides an abstraction for an opened writable file-like object in
// the storage backend. The writer must flush all content written to it on
// the call to Close, but is only required to make its content readable on a
// call to Commit.
type writer struct {
	*driver
	ctx  context.Context
	path string

	rd   *io.PipeReader
	wt   *io.PipeWriter
	size int64
	from int64

	closed    bool
	committed bool
	cancelled bool

	exitch chan struct{}
	err    error
}

func newFileWriter(d *driver, ctx context.Context, path string, size int64) storagedriver.FileWriter {
	return &writer{
		driver: d, ctx: ctx, path: path, from: size, size: size,
		exitch: make(chan struct{}, 1),
	}
}

func (w *writer) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	if w.err != nil {
		err = w.err
		return
	}

	if w.rd == nil {
		w.rd, w.wt = io.Pipe()
		go w.background()
	}

	n, err = w.wt.Write(p)
	if err != nil {
		return
	}
	w.size += int64(n)
	return
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	w.close()
	if w.err != nil {
		return w.err
	}

	w.closed = true
	return nil
}

// Size returns the number of bytes written to this FileWriter.
func (w *writer) Size() int64 {
	return w.size
}

// Cancel removes any written content from this FileWriter.
func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}

	w.close()

	w.cancelled = true
	err := w.Delete(w.ctx, w.path)
	return err
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}

	w.close()
	if w.err != nil {
		return w.err
	}

	w.committed = true
	return nil
}

func (w *writer) close() {
	if w.wt != nil {
		w.wt.CloseWithError(io.EOF)
		w.wt = nil
		<-w.exitch
		w.rd.CloseWithError(io.EOF)
		w.rd = nil
		close(w.exitch)
	}
}

func (w *writer) background() {
	key := w.getKey(w.path)
	parts := make([]kodo.PutPart, 0, 2)

	defer func() {
		w.exitch <- struct{}{}
	}()

	if w.from > 0 {
		parts = append(parts, kodo.PutPart{
			Key:  key,
			From: 0,
			To:   w.from,
		})
	}

	parts = append(parts, kodo.PutPart{
		R: w.rd,
	})

	w.err = w.bucket.PutParts(w.ctx, nil, key, []string{key}, parts, nil)
	if w.err != nil {
		logrus.Warn("writer background PutParts:", key, w.err)
		return
	}

	w.err = w.refreshCache(key)
	if w.err != nil {
		logrus.Warn("writer background refreshCache:", key, w.err)
		return
	}
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