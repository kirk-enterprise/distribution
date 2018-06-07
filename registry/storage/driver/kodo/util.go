package kodo

import (
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/context"
)

func ReqEntry(c context.Context) *logrus.Entry {
	reqid, _ := c.Value("trace.id").(string)
	return logrus.WithField("reqid", reqid)
}

// 生成文件临时存储的路径
func buildTmpPath(subPath string) string {
	return path.Join("/tmp", subPath)
}
