package server

import (
	"datareplication_receiver/config"
	"datareplication_receiver/core"
	"datareplication_receiver/healthcheck"
	"datareplication_receiver/receiver"
	"datareplication_receiver/storage/couchbase"
	"datareplication_receiver/storage/logging"
	"datareplication_receiver/util/common"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/couchbase/gocb/v2"
	"github.com/go-chi/chi"
)

var (
	cbConfigBucket             = config.GetString("Couchbase.CBConfigBucket")
	cbConfigDocKey             = config.GetString("Couchbase.CBConfigDocKey")
	upsertEndpoint             = config.GetString("APIEndpoints.Upsert")
	getEndpoint                = config.GetString("APIEndpoints.Get")
	removeEndpoint             = config.GetString("APIEndpoints.Remove")
	existEndpoint              = config.GetString("APIEndpoints.Exists")
	healthcheckEndpoint        = config.GetString("APIEndpoints.Healthcheck")
	cbtocbconfigrationEndpoint = config.GetString("APIEndpoints.CBtoCBconfigration")
	nullDataDocumentEndpoint   = config.GetString("APIEndpoints.NullDataDocumentEndpoint")
	nullDocumentBucketName     = config.GetString("Couchbase.NullDocBucketName")
)

func getRoutes() http.Handler {

	r := chi.NewRouter()

	r.Post(getEndpoint, getDocument())
	r.Post(upsertEndpoint, createDocument())
	r.Post(removeEndpoint, removeDocument())
	r.Post(existEndpoint, existDocument())
	r.Post(nullDataDocumentEndpoint, nullDataDocument())
	r.Post(cbtocbconfigrationEndpoint, getCbToCbConfiguration())
	r.Get(healthcheckEndpoint, healthcheck.HealthCheck())

	return r
}

// create couchbase document processing
func createDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			documentMetaData, err := common.DecompressGzip(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				updateDocument(w, documentMetaData, true)
			}
		default:
			var documentMetaData core.DocumentMetaData
			err := json.NewDecoder(r.Body).Decode(&documentMetaData)

			if err != nil {
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				updateDocument(w, documentMetaData, false)
			}
		}
	}
}

// null couchbase document content processing
func nullDataDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			documentMetaData, err := common.DecompressGzip(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				checkDocumentContentAndRemoveDocument(w, documentMetaData, true)
			}
		default:
			var documentMetaData core.DocumentMetaData
			err := json.NewDecoder(r.Body).Decode(&documentMetaData)

			if err != nil {
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				checkDocumentContentAndRemoveDocument(w, documentMetaData, false)
			}
		}
	}
}

// remove couchbase document processing
func removeDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			documentMetaData, err := common.DecompressGzip(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				deleteDocument(w, documentMetaData, true)
			}
		default:
			var documentMetaData core.DocumentMetaData
			err := json.NewDecoder(r.Body).Decode(&documentMetaData)
			if err != nil {
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				deleteDocument(w, documentMetaData, false)
			}
		}
	}
}

// exists couchbase document processing
func existDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			documentMetaData, err := common.DecompressGzip(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				getAndSendExistDocumentStatus(w, documentMetaData, true)
			}
		default:
			var documentMetaData core.DocumentMetaData
			err := json.NewDecoder(r.Body).Decode(&documentMetaData)
			if err != nil {
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				getAndSendExistDocumentStatus(w, documentMetaData, true)
			}
		}
	}
}

// get couchbase document processing
func getDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			documentMetaData, err := common.DecompressGzip(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				getAndSendDocument(w, documentMetaData, true)
			}
		default:
			var documentMetaData core.DocumentMetaData
			err := json.NewDecoder(r.Body).Decode(&documentMetaData)
			if err != nil {
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				getAndSendDocument(w, documentMetaData, true)
			}
		}
	}
}

// get couchbase document processing
func getCbToCbConfiguration() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			cbResult, err := getDocFromCouchbase(cbConfigBucket, cbConfigDocKey)
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, true)
			} else {
				w.WriteHeader(http.StatusOK)
				sendResponseBack(w, core.ResponseResult{Status: core.Success, ErrorMessage: "", SuccessMessage: string(cbResult.Value)}, true)
			}
		default:
			cbResult, err := getDocFromCouchbase(cbConfigBucket, cbConfigDocKey)
			if err != nil {
				log.Println(err)
				logging.EnrichErrorWithStackTraceAndLog(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, false)
			} else {
				w.WriteHeader(http.StatusOK)
				sendResponseBack(w, core.ResponseResult{Status: core.Success, ErrorMessage: "", SuccessMessage: string(cbResult.Value)}, false)
			}
		}
	}
}

/*
// Get document from the couchbase and return back couchbase document \
// Arguments:

	w: http write object
	documentMetaData:That contain tjhe couchbase document bucket name, key and value
	compress: Check the request body data compress or not
*/
func getAndSendDocument(w http.ResponseWriter, documentMetaData core.DocumentMetaData, compress bool) {
	cbResult, err := getDocFromCouchbase(documentMetaData.Bucket, documentMetaData.Key)
	logging.DoLoggingLevelBasedLogs(logging.Debug, "bucket: "+documentMetaData.Bucket+" docKey: "+documentMetaData.Key+" Length: "+strconv.Itoa(int(len(cbResult.Value))), nil)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			w.WriteHeader(http.StatusOK)
			sendResponseBack(w, core.GetDocumentResponse{NullFlag: false, Status: core.Success, ErrorMessage: "", SuccessMessage: "document not found"}, compress)
		} else {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
		}
	} else {
		w.WriteHeader(http.StatusOK)
		if len(cbResult.Value) > 0 {
			sendResponseBack(w, core.GetDocumentResponse{NullFlag: false, Status: core.Success, ErrorMessage: "", SuccessMessage: "document lenght is greater then zero"}, compress)
		} else {
			sendResponseBack(w, core.GetDocumentResponse{NullFlag: true, Status: core.Success, ErrorMessage: "", SuccessMessage: "document lenght is zero"}, compress)
		}
	}
}

/*
// Check the couchbase document exists or not and revert back to response
// Arguments:

	w: http write object
	documentMetaData:That contain tjhe couchbase document bucket name, key and value
	compress: Check the request body data compress or not
*/
func getAndSendExistDocumentStatus(w http.ResponseWriter, documentMetaData core.DocumentMetaData, compress bool) {
	existDoc, err := couchbase.ExistsDocument(documentMetaData.Bucket, documentMetaData.Key)
	if err != nil {
		logging.EnrichErrorWithStackTraceAndLog(err)
		w.WriteHeader(http.StatusInternalServerError)
		sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "bucket: "+documentMetaData.Bucket+" docKey: "+documentMetaData.Key+" Status: "+strconv.FormatBool(existDoc), nil)
		w.WriteHeader(http.StatusOK)
		if existDoc {
			sendResponseBack(w, core.ResponseResult{Status: core.DocumentExists, ErrorMessage: "", SuccessMessage: "document found"}, compress)
		} else {
			sendResponseBack(w, core.ResponseResult{Status: core.DocumentNotExists, ErrorMessage: "", SuccessMessage: "document not found"}, compress)
		}
	}
}

/*
// Get document from the couchbase and check the document is empty then Insert it in Null Bucket and Remove Data from Currnet Bucket.
// Arguments:

	w: http write object
	documentMetaData:That contain tjhe couchbase document bucket name, key and value
	compress: Check the request body data compress or not
*/
func checkDocumentContentAndRemoveDocument(w http.ResponseWriter, documentMetaData core.DocumentMetaData, compress bool) {
	// get the document from the couchbase
	cbResult, err := getDocFromCouchbase(documentMetaData.Bucket, documentMetaData.Key)

	logging.DoLoggingLevelBasedLogs(logging.Debug, "bucket: "+documentMetaData.Bucket+" docKey: "+documentMetaData.Key+" Length: "+strconv.Itoa(int(len(cbResult.Value))), nil)
	if err != nil {
		// check couchbase document found or not
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			w.WriteHeader(http.StatusOK)
			sendResponseBack(w, core.ResponseResult{Status: core.DocumentNotExists, ErrorMessage: "", SuccessMessage: "ReceiverDocNotFound: document not found, docKey= " + documentMetaData.Key}, compress)
		} else {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
		}
	} else {
		// Check if Document in couchbase has Data or not
		if len(cbResult.Value) > 0 {
			w.WriteHeader(http.StatusOK)
			sendResponseBack(w, core.ResponseResult{Status: core.DocumentExists, ErrorMessage: "", SuccessMessage: "ReceiverDocFoundWithContent: document found with content, docKey= " + documentMetaData.Key}, compress)
		} else {
			// If Content is null or empty then Insert it in Other Bucket and Remove Data from Currnet Bucket.
			err := receiver.InsertAndDeleteDocument(documentMetaData.Key, documentMetaData.Bucket, cbResult)
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
			} else {
				w.WriteHeader(http.StatusOK)
				sendResponseBack(w, core.ResponseResult{Status: core.Success, ErrorMessage: "", SuccessMessage: "ReceiverSuccess: document inserted and deleted successfully, docKey= " + documentMetaData.Key + " ,nullBucket: " + nullDocumentBucketName + " ,receiverBucket: " + documentMetaData.Bucket}, compress)
			}
		}
	}
}

// update couchabse document process
func updateDocument(w http.ResponseWriter, documentMetaData core.DocumentMetaData, compress bool) {
	logging.DoLoggingLevelBasedLogs(logging.Debug, "BUCKET: "+documentMetaData.Bucket+" KEY: "+documentMetaData.Key+" EVENT : UPSERT", nil)
	err := receiver.PutDocInCouchbase(documentMetaData.Bucket, documentMetaData.Key, documentMetaData.Document)
	if err != nil {
		logging.EnrichErrorWithStackTraceAndLog(err)
		w.WriteHeader(http.StatusInternalServerError)
		sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
	} else {
		w.WriteHeader(http.StatusOK)
		sendResponseBack(w, core.ResponseResult{Status: core.Success, ErrorMessage: "", SuccessMessage: "document inserted successfully"}, compress)
	}
}

// delete couchbase document process
func deleteDocument(w http.ResponseWriter, documentMetaData core.DocumentMetaData, compress bool) {
	logging.DoLoggingLevelBasedLogs(logging.Debug, "BUCKET: "+documentMetaData.Bucket+" KEY: "+documentMetaData.Key+" EVENT : DELETE", nil)
	err := receiver.RemoveDocInCouchbase(documentMetaData.Bucket, documentMetaData.Key)
	if err != nil {
		logging.EnrichErrorWithStackTraceAndLog(err)
		w.WriteHeader(http.StatusInternalServerError)
		sendResponseBack(w, core.ResponseResult{Status: core.Error, ErrorMessage: err.Error(), SuccessMessage: ""}, compress)
	} else {
		w.WriteHeader(http.StatusOK)
		sendResponseBack(w, core.ResponseResult{Status: core.Success, ErrorMessage: "", SuccessMessage: "document removed successfully"}, compress)
	}
}

// get document from couchbase
func getDocFromCouchbase(bucket string, key string) (cbresult couchbase.CBRawDataResult, couchbaseError error) {
	_, err_get := couchbase.Raw_GetDocument(bucket, key, &cbresult)
	if err_get != nil {
		if errors.Is(err_get, gocb.ErrDocumentNotFound) {
			couchbaseError = err_get
		} else {
			couchbaseError = logging.EnrichErrorWithStackTrace(err_get)
		}
	} else {
		couchbaseError = nil
	}
	return cbresult, couchbaseError
}

// response back to sender: If the request body data was the compressed then return back the compressed response, else return back the normal response
func sendResponseBack(w http.ResponseWriter, result interface{}, compress bool) {

	if compress {
		byteData, jsonMarshalErr := json.Marshal(result)
		if jsonMarshalErr != nil {
			w.WriteHeader(http.StatusNotImplemented)
			json.NewEncoder(w).Encode(core.ResponseResult{Status: core.Error, ErrorMessage: logging.EnrichErrorWithStackTrace(jsonMarshalErr).Error(), SuccessMessage: ""})
		} else {
			bytes, compressZipErr := common.CompressGzip(byteData)
			if compressZipErr != nil {
				w.WriteHeader(http.StatusNotImplemented)
				json.NewEncoder(w).Encode(core.ResponseResult{Status: core.Error, ErrorMessage: logging.EnrichErrorWithStackTrace(compressZipErr).Error(), SuccessMessage: ""})
			} else {
				w.Write(bytes.Bytes())
			}
		}
	} else {
		json.NewEncoder(w).Encode(result)
	}
}
