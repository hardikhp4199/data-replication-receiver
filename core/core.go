package core

import (
	"datareplication_receiver/storage/couchbase"
)

type Flag int32

var SSLExpired bool = false
var SSLExpiredMessage string
var SSLCRTFileContent []byte

const (
	Error Flag = iota + 1
	Success
	DocumentExists
	DocumentNotExists
	NullDocument
)

// for http response
type ResponseResult struct {
	Status         Flag
	ErrorMessage   string
	SuccessMessage string
}

type DocumentMetaData struct {
	Key      string
	Document couchbase.CBRawDataResult
	Bucket   string
}

type GetDocumentResponse struct {
	Status         Flag
	ErrorMessage   string
	SuccessMessage string
	NullFlag       bool
}
