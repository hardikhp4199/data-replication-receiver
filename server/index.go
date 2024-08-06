package server

import (
	"datareplication_receiver/config"
	"datareplication_receiver/core"
	"datareplication_receiver/healthcheck"
	"datareplication_receiver/receiver"
	"datareplication_receiver/storage/logging"
	"datareplication_receiver/util/common"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
)

var (
	upsertEndpoint      = config.GetString("APIEndpoints.Upsert")
	removeEndpoint      = config.GetString("APIEndpoints.Remove")
	healthcheckEndpoint = config.GetString("APIEndpoints.Healthcheck")
)

func getRoutes() http.Handler {

	r := chi.NewRouter()

	r.Post(upsertEndpoint, createDocument())
	r.Post(removeEndpoint, removeDocument())
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
