package kodo

import (
	"bufio"
	"fmt"
	"os"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

var _ storagedriver.FileWriter = &fileWriter{}

// fileWriter provides an abstraction for an opened writable file-like object in
// the storage backend. The writer must flush all content written to it on
// the call to Close, but is only required to make its content readable on a
// call to Commit.
type fileWriter struct {
	*driver
	ctx       context.Context
	file      *os.File
	kodoKey   string
	size      int64
	bw        *bufio.Writer
	closed    bool
	committed bool
	cancelled bool
}

func newFileWriter(d *driver, ctx context.Context, file *os.File, kodoKey string, size int64) *fileWriter {
	return &fileWriter{
		driver:  d,
		ctx:     ctx,
		file:    file,
		kodoKey: kodoKey,
		size:    size,
		bw:      bufio.NewWriter(file),
	}
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	n, err := fw.bw.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

func (fw *fileWriter) Close() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.closed = true
	return nil
}

func (fw *fileWriter) Cancel() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.cancelled = true
	fw.file.Close()
	return os.Remove(fw.file.Name())
}

func (fw *fileWriter) Commit() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	//调用驱动上传文件
	localFile := fw.file.Name()
	if err := fw.driver.upload(fw.ctx, localFile, fw.kodoKey); err != nil {
		return err
	}

	fw.committed = true

	go func() {
		if err := os.Remove(fw.file.Name()); err != nil {
			fmt.Errorf("remove failed:", fw.file.Name())
		}
	}()
	return nil
}
