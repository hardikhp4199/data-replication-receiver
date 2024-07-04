package healthcheck

import (
	"datareplication_receiver/core"
	"datareplication_receiver/storage/logging"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// health check
func HealthCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sslExpired := core.SSLExpired
		message := core.SSLExpiredMessage
		logging.DoLoggingLevelBasedLogs(logging.Debug, "healthcheck function: "+strconv.FormatBool(core.SSLExpired)+" "+core.SSLExpiredMessage, nil)

		if sslExpired {
			logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(errors.New(message)))
			giveHealthCheckResponseBasedOnHealth(false, message, w)
		} else {
			logging.DoLoggingLevelBasedLogs(logging.Info, "ssl certificate status is ok", nil)
			giveHealthCheckResponseBasedOnHealth(true, "", w)
		}
	}
}
func giveHealthCheckResponseBasedOnHealth(healthCheckStatus bool, message string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string)
	if healthCheckStatus {
		resp["message"] = "ssl certificate status is ok"
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		resp["message"] = message
	}
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", err)
	}
	w.Write(jsonResp)
}
