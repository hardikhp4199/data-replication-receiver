package sslcertificate

import (
	"bytes"
	"datareplication_receiver/config"
	"datareplication_receiver/core"
	"datareplication_receiver/storage/logging"
	"errors"
	"os"

	"strconv"
)

var (
	certFile = config.GetString("HttpServer.SSL.certFile")
)

func CheckSSLCertificatesStatus() {
	certFileContent, err := readFile(certFile)
	if err != nil {
		core.SSLExpired = true
		core.SSLExpiredMessage = err.Error()
		logging.DoLoggingLevelBasedLogs(logging.Error, "", err)
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "cert file: global file content length: "+strconv.Itoa(len(core.SSLCRTFileContent))+" local file content length: "+strconv.Itoa(len(certFileContent)), nil)
		if len(core.SSLCRTFileContent) > 0 {
			if !bytes.Equal(core.SSLCRTFileContent, certFileContent) {
				core.SSLExpired = true
				core.SSLExpiredMessage = "new certificate found."
				logging.EnrichErrorWithStackTraceAndLog(errors.New("new certificate found"))
			} else {
				core.SSLExpired = false
				core.SSLExpiredMessage = ""
			}
		} else {
			logging.DoLoggingLevelBasedLogs(logging.Debug, "read file: cert content length: "+strconv.Itoa(len(certFileContent)), nil)
			core.SSLCRTFileContent = certFileContent
		}
	}
	logging.DoLoggingLevelBasedLogs(logging.Debug, "global value from CheckSSLCertificatesStatus: "+strconv.FormatBool(core.SSLExpired)+" "+core.SSLExpiredMessage, nil)
}

func readFile(fileName string) (fileContent []byte, errOut error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		errOut = logging.EnrichErrorWithStackTrace(err)
	} else {
		fileContent = content
	}
	return
}
