package server

import (
	"datareplication_receiver/config"
	"datareplication_receiver/storage/logging"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/v5/middleware"
)

var (
	port         = config.GetInt("HttpServer.Port")
	certFile     = config.GetString("HttpServer.SSL.certFile")
	keyFile      = config.GetString("HttpServer.SSL.keyFile")
	usernameAuth = config.GetString("HttpServer.Authorization.username")
	passwordAuth = config.GetString("HttpServer.Authorization.password")
)

// connect to server
func StartServerApp() {
	s := chi.NewRouter()

	// authorization and authentication
	s.Use(middleware.BasicAuth("unauthorized", map[string]string{
		usernameAuth: passwordAuth,
	}))

	//get routes
	s.Mount("/", getRoutes())

	// initial server and authenticate
	httpPort := ":" + strconv.Itoa(port)
	err_conn := http.ListenAndServeTLS(httpPort, certFile, keyFile, s)
	if err_conn != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(err_conn))
	}
}
