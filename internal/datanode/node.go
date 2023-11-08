package datanode

import (
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
)

type DataNode struct {
	fs   *filesystem.FileSystem
	base string
	id   string
}

func (d *DataNode) getMetaKey(key string) string {
	return fmt.Sprintf(
		"%s/meta/%s",
		d.base,
		base64.StdEncoding.EncodeToString([]byte(key)),
	)
}

func (d *DataNode) getDataKey(key string) string {
	return fmt.Sprintf("%s/object/%s", d.base, key)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
