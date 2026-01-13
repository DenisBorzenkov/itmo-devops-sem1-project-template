package archive

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"
)

type ZipReader struct {
	r *zip.Reader
}

func NewZipReader(src io.Reader) (*ZipReader, error) {
	buf, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}

	zr, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, err
	}

	return &ZipReader{r: zr}, nil
}

func (z *ZipReader) ReadDataCSV() (io.ReadCloser, error) {
	for _, f := range z.r.File {
		if strings.HasSuffix(f.Name, ".csv") {
			return f.Open()
		}
	}
	return nil, fmt.Errorf("csv file not found in zip")
}
