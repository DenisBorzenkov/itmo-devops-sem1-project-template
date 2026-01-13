package archive

import (
	"archive/tar"
	"fmt"
	"io"
	"strings"
)

type TarReader struct {
	r io.Reader
}

func NewTarReader(src io.Reader) *TarReader {
	return &TarReader{r: src}
}

func (t *TarReader) ReadDataCSV() (io.ReadCloser, error) {
	tr := tar.NewReader(t.r)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("csv not found in tar")
		}
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(header.Name, ".csv") {
			return io.NopCloser(tr), nil
		}
	}
}
