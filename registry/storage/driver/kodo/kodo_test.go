package kodo

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"gopkg.in/check.v1"

	"qiniupkg.com/api.v7/kodo"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var kodoDriverConstructor func(rootDirectory string) (*Driver, error)
var skipkodo func() string

func init() {
	zone := os.Getenv("KODO_ZONE")
	accessKey := os.Getenv("KODO_ACCESS_KEY")
	secretKey := os.Getenv("KODO_SECRET_KEY")
	bucket := os.Getenv("KODO_BUCKET")
	baseURL := os.Getenv("KODO_BASE_URL")
	uidStr := os.Getenv("KODO_USER_UID")
	refreshUrl := os.Getenv("KODO_REFRESH_URL")
	adminAk := os.Getenv("KODO_ADMIN_ACCESS_KEY")
	adminSk := os.Getenv("KODO_ADMIN_SECRET_KEY")
	uid, _ := strconv.ParseUint(uidStr, 10, 64)

	debug := os.Getenv("KODO_DEBUG")

	root, err := ioutil.TempDir("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	kodoDriverConstructor = func(rootDirectory string) (*Driver, error) {
		var zoneValue int64
		if zone != "" {
			zoneValue, err = strconv.ParseInt(zone, 10, 64)
			if err != nil {
				return nil, err
			}
		}

		config := kodo.Config{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}
		if debug != "" {
			config.Transport = NewTransportWithLogger()
		}

		parameters := DriverParameters{
			int(zoneValue),
			bucket,
			baseURL,
			rootDirectory,
			config,
			uid,
			adminAk,
			adminSk,
			refreshUrl,
		}

		return New(parameters)
	}

	skipkodo = func() string {
		if accessKey == "" || secretKey == "" || bucket == "" || baseURL == "" {
			return "Must set KODO_ACCESS_KEY, KODO_SECRET_KEY, KODO_BUCKET, KODO_BASE_URL to run kodo tests"
		}

		if uid == 0 || refreshUrl == "" || adminAk == "" || adminSk == "" {
			return "Hack！！ Must set KODO_USER_UID, KODO_REFRESH_URL, KODO_ADMIN_ACCESS_KEY, nKODO_ADMIN_SECRET_KEY"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return kodoDriverConstructor(root)
	}, skipkodo)
}

func TestEmptyRootList(t *testing.T) {
	if skipkodo() != "" {
		t.Skip(skipkodo())
	}

	validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(validRoot)

	rootedDriver, err := kodoDriverConstructor(validRoot)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := kodoDriverConstructor("")
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := kodoDriverConstructor("/")
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, err := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}

	keys, err = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
}
