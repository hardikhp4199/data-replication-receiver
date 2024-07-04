package common

import (
	"bytes"
	"compress/gzip"
	"datareplication_receiver/core"
	"datareplication_receiver/storage/logging"
	"encoding/json"
	"io"
	"io/ioutil"
)

func DecompressGzip(r io.Reader) (doc core.DocumentMetaData, err_decompressor error) {

	zr, err_reader := gzip.NewReader(r)

	if err_reader != nil {
		err_decompressor = logging.EnrichErrorWithStackTrace(err_reader)
	} else {
		docData, err_io := ioutil.ReadAll(zr)

		if err_io != nil {
			err_decompressor = logging.EnrichErrorWithStackTrace(err_io)
		} else {
			if err_json := json.Unmarshal(docData, &doc); err_json != nil {
				err_decompressor = logging.EnrichErrorWithStackTrace(err_json)
			}
		}
	}
	return
}

func CompressGzip(str []byte) (buf bytes.Buffer, errOut error) {

	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(str)
	if err != nil {
		errOut = logging.EnrichErrorWithStackTrace(err)
	} else {
		if err := zw.Close(); err != nil {
			errOut = logging.EnrichErrorWithStackTrace(err)
		}
	}
	return
}
