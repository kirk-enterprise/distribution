package kodo

import (
	"os"
	"strconv"
	"testing"

	"gopkg.in/check.v1"

	"qiniupkg.com/api.v7/kodo"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
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
	debug := os.Getenv("KODO_DEBUG")

	zone = "0"
	bucket = "registry-dev-cloudappl-com"
	debug = "true"

	kodoDriverConstructor = func(rootDirectory string) (*Driver, error) {
		var zoneValue int64
		var err error
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
			config.Transport = NewTransportWithLogger("")
		}

		parameters := DriverParameters{
			Zone:          int(zoneValue),
			Bucket:        bucket,
			BaseURL:       baseURL,
			RootDirectory: rootDirectory,
			Config:        config,
			ChunkSize:     defaultChunkSize,
		}

		return New(parameters)
	}

	skipkodo = func() string {
		if accessKey == "" || secretKey == "" || bucket == "" || baseURL == "" {
			return "Must set KODO_ACCESS_KEY, KODO_SECRET_KEY, KODO_BUCKET, KODO_BASE_URL to run kodo tests"
		}
		return ""
	}

}

func TestEmptyRootList(t *testing.T) {
	if skipkodo() != "" {
		t.Skip(skipkodo())
	}

	rootedDriver, err := kodoDriverConstructor("test123")
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

	_, err = rootedDriver.Stat(ctx, filename)
	if err != nil {
		t.Fatalf("unexpected error Stat: %v", err)
	}

	// defer rootedDriver.Delete(ctx, filename)

	filename2 := "/test2"
	writer, err := rootedDriver.Writer(ctx, filename2, false)
	if err != nil {
		t.Fatalf("unexpected error creating Writer: %v", err)
	}

	// t.Fatalf("%+v", writer)

	for i := 0; i < 400; i++ {
		_, err = writer.Write([]byte("1234567890123456789012345678901234567890"))
		if err != nil {
			t.Fatalf("unexpected error Writer write: %v", err)
		}
	}

	err = writer.Commit()
	if err != nil {
		t.Fatalf("unexpected error Writer write: %v", err)
	}

	info, err := rootedDriver.Stat(ctx, filename2)
	if err != nil {
		t.Fatalf("unexpected error Stat: %v", err)
	}
	t.Fatal(info)

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
