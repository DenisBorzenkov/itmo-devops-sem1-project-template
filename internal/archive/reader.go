package archive

import (
	"io"
)

type ArchiveReader interface {
	ReadDataCSV() (io.ReadCloser, error)
}
