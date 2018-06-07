// Package kodo provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu KODO cloud storage.
//
// Because KODO is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//

package kodo

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	pathlib "path"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
	"qiniupkg.com/x/rpc.v7"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "kodo"

var mac *qbox.Mac
var BUCKET string

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
func init() {
	factory.Register(driverName, &kodoDriverFactory{})
}

type kodoDriverFactory struct{}

func (factory *kodoDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type DriverParameters struct {
	Bucket        string
	BaseURL       string
	BaseHost      string
	AccessKey     string
	SecretKey     string
	RootDirectory string
	Zone          *storage.Zone
	IsInternal    bool
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	bucket := parameters["bucket"].(string)
	baseurl := parameters["baseurl"].(string)
	accesskey := parameters["accesskey"].(string)
	secretkey := parameters["secretkey"].(string)
	rootdirectory := parameters["rootdirectory"].(string)
	isinternal := parameters["isinternal"].(bool)

	// 解析出baseurl对应的host，后面在配置内网访问时需要用到
	u, err := url.Parse(baseurl)
	if err != nil {
		logrus.Fatal("illegal baseurl:", baseurl)
		return nil, err
	}
	basehost := u.Host

	// 配置内网加速的iohost和uphost，这两个配置在测试环境没用
	zone := &storage.ZoneHuadong
	if iohost, ok := parameters["iohost"].(string); ok && len(iohost) > 0 {
		zone.IovipHost = iohost
	}

	if uphosts, ok := parameters["uphosts"].([]interface{}); ok && len(uphosts) > 0 {
		srcUpHosts := make([]string, len(uphosts))
		for i, host := range uphosts {
			srcUpHosts[i] = host.(string)
		}
		zone.SrcUpHosts = srcUpHosts
	}

	return New(&DriverParameters{
		Bucket:        bucket,
		BaseURL:       baseurl,
		BaseHost:      basehost,
		AccessKey:     accesskey,
		SecretKey:     secretkey,
		RootDirectory: rootdirectory,
		Zone:          zone,
		IsInternal:    isinternal,
	})
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func New(params *DriverParameters) (*Driver, error) {
	mac = qbox.NewMac(params.AccessKey, params.SecretKey)

	BUCKET = params.Bucket

	cfg := storage.Config{
		UseHTTPS: false,
		Zone:     params.Zone,
	}

	uploader := storage.NewResumeUploader(&cfg)
	bucketManager := storage.NewBucketManager(mac, &cfg)

	d := &driver{
		params:        params,
		uploader:      uploader,
		bucketManager: bucketManager,
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
	params        *DriverParameters
	uploader      *storage.ResumeUploader
	bucketManager *storage.BucketManager
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
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "GetContent",
	})

	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		logger.Error("Reader:", err)
		return nil, err
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		logger.Error("ReadAll:", err)
		return nil, err
	}
	return body, nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "Reader", "offset": offset,
	})

	privateAccessURL := d.privateURL(path)

	// 转换为内网加速下载模式，这是kodo的黑科技
	// 1. 将privateAccessURL中的域名部分替换为内网域名xsio.qiniu.io
	// 2. http header中设置Host: [BaseURL 对应的域名]

	// 这是配置加速的第一步
	if d.params.IsInternal {
		privateAccessURL = strings.Replace(
			privateAccessURL, d.params.BaseHost, d.params.Zone.IovipHost, 1,
		)
	}

	req, err := http.NewRequest("GET", privateAccessURL, nil)
	if err != nil {
		logger.Error("http.NewRequest:", err)
		return nil, err
	}

	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	// 这是配置加速的第二步
	if d.params.IsInternal {
		req.Host = d.params.BaseHost
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("http.Do:", err)
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		logger.Warn("404: not found")
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	return resp.Body, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "PutContent",
	})

	writer, err := d.Writer(ctx, path, false)
	if err != nil {
		logger.Error("d.Writer:", err)
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, bytes.NewReader(content))
	if err != nil {
		writer.Cancel()
		logger.Error("io.Copy:", err)
		return err
	}
	return writer.Commit()
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
// 先将文件写入临时文件中，在Commit的时候在分片上传进入kodo
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	kodoKey := d.kodoKey(path)
	fullPath := buildTmpPath(path)

	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "Writer", "append": append,
	})

	parentDir := pathlib.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0777); err != nil {
		return nil, err
	}

	fp, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("OpenFile:", err)
		return nil, err
	}

	var offset int64
	if !append {
		err := fp.Truncate(0)
		if err != nil {
			fp.Close()
			return nil, err
		}
	} else {
		n, err := fp.Seek(0, os.SEEK_END)
		if err != nil {
			fp.Close()
			return nil, err
		}
		offset = int64(n)
	}
	logger.WithField("offset", offset).Info()

	return newFileWriter(d, ctx, fp, kodoKey, offset), nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "Stat",
	})

	//只列出一个文件来查看，所以limit字段传1
	limit := 1      //每次最多list 1000个文件
	delimiter := "" //目录分隔符
	marker := ""    //初始列举marker为空

	entries, _, _, _, err := d.bucketManager.ListFiles(BUCKET, d.kodoKey(path), delimiter, marker, limit)
	if err != nil {
		logger.Error("ListFiles:", d.kodoKey(path), err)
		return nil, err
	}

	if len(entries) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	entry := entries[0]

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if d.kodoKey(path) != entry.Key {
		fi.IsDir = true
	}

	if !fi.IsDir {
		fi.Size = entry.Fsize
		fi.ModTime = time.Unix(0, entry.PutTime*100)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// registry要求所有的文件路径都是绝对路径，以"/"开头
// List returns a list of the objects that are direct descendants of the
// given path.
// 这里的path参数应该是带前缀的"/" TODO
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": opath, "func": "List",
	})

	path := opath
	if path != "/" && path[len(path)-1] != '/' {
		path += "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	rootPrefix := ""
	if d.kodoKey("") == "" {
		rootPrefix = "/"
	}

	limit := 1000    //每次最多list 1000个文件
	delimiter := "/" //目录分隔符
	marker := ""     //初始列举marker为空

	var files []string
	var directories []string

	for {
		entries, prefixes, nextMarker, hasNext, err := d.bucketManager.ListFiles(BUCKET, d.kodoKey(path), delimiter, marker, limit)

		if err != nil {
			logger.Error("bucketManager.ListFiles:", d.kodoKey(path), err)
			return nil, err
		}

		for _, entry := range entries {
			files = append(files, strings.Replace(entry.Key, d.kodoKey(""), rootPrefix, 1))
		}

		for _, prefix := range prefixes {
			directories = append(directories, strings.Replace(strings.TrimSuffix(prefix, "/"), d.kodoKey(""), rootPrefix, 1))
		}

		if hasNext {
			marker = nextMarker
		} else {
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
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"sourcePath": sourcePath,
		"destPath":   destPath,
		"func":       "Move",
	})

	bucket := d.params.Bucket
	err := d.bucketManager.Move(bucket, d.kodoKey(sourcePath), bucket, d.kodoKey(destPath), true)
	if err != nil {
		logger.Error("debugkodo Move:", errorInfo(err))
	}
	return parseError(sourcePath, err)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"path": path, "func": "Delete",
	})

	bucket := d.params.Bucket
	limit := 1000 //每次最多list 1000个文件
	marker := ""  //初始列举marker为空

	for {
		entries, _, nextMarker, hasNext, err := d.bucketManager.ListFiles(bucket, d.kodoKey(path), "", marker, limit)
		if err != nil {
			logger.Error("ListFiles:", d.kodoKey(path), err)
			return err
		}

		if len(entries) == 0 {
			logger.Error("ListFiles:", d.kodoKey(path), "no entry")
			return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}

		for _, entry := range entries {
			err = d.bucketManager.Delete(bucket, entry.Key)
			if err != nil {
				if isKeyNotExists(err) {
					logger.Error("Delete:", d.kodoKey(path), " does not exist")
					continue
				}
				logger.Error("Delete:", d.kodoKey(path), err)
				return err
			}
		}

		if hasNext {
			marker = nextMarker
		} else {
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
	fmt.Printf("options = %+v\n", options)
	return d.privateURL(path), nil
}

func (d *driver) privateURL(path string) string {
	domain := d.params.BaseURL
	key := d.kodoKey(path)
	deadline := time.Now().Add(time.Second * 3600 * 3).Unix() //3小时有效期
	privateAccessURL := storage.MakePrivateURL(mac, domain, key, deadline)

	return privateAccessURL
}

func (d *driver) upload(ctx context.Context, localFile, kodoKey string) error {
	logger := ReqEntry(ctx).WithFields(logrus.Fields{
		"kodoKey": kodoKey, "func": "upload",
	})

	putPolicy := storage.PutPolicy{
		Scope:   fmt.Sprintf("%s:%s", BUCKET, kodoKey), //覆盖上传
		Expires: 3600 * 3,                              //token过期时间 3小时
	}
	upToken := putPolicy.UploadToken(mac)

	fileInfo, err := os.Stat(localFile)
	if err != nil {
		logger.Error("os.Stat:", err)
		return err
	}

	fileSize := fileInfo.Size()
	logger = logger.WithFields(logrus.Fields{
		"fileSize":   fileSize,
		"blockCount": storage.BlockCount(fileSize),
	})

	putExtra := storage.RputExtra{
		Notify: func(blkIdx int, blkSize int, ret *storage.BlkputRet) {
			logger.Infof("current upload: %d, %d", blkIdx, blkSize)
		},
	}
	ret := storage.PutRet{}

	//设置kodo的reqid，不知道为啥reqid的key需要设置为0
	//没有找到相关文档，我是直接从rpc的代码里找到的
	kodoCtx := context.WithValue(ctx, 0, ctx.Value("trace.id"))
	err = d.uploader.PutFile(kodoCtx, &ret, upToken, kodoKey, localFile, &putExtra)
	if err != nil {
		logger.Error("PutFile:", err)
	}
	return err
}

func (d *driver) kodoKey(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.params.RootDirectory, "/")+path, "/")
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

func errorInfo(err error) string {
	if err1, ok := err.(*rpc.ErrorInfo); ok {
		return err1.ErrorDetail()
	}
	return err.Error()
}
