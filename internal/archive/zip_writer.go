package archive

import (
	"archive/zip"
	"bytes"
)

func WriteDataZip(filename string, data []byte) ([]byte, error) {
	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	f, err := w.Create(filename)
	if err != nil {
		return nil, err
	}

	if _, err := f.Write(data); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
