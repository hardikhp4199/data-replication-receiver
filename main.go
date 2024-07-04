package main

import (
	"datareplication_receiver/config"
	"datareplication_receiver/server"
	"datareplication_receiver/storage/logging"
	"datareplication_receiver/storage/sslcertificate"
	"time"

	"github.com/go-co-op/gocron"
)

func main() {
	var (
		serverPort             = config.GetString("HttpServer.Port")
		sslExpireCheckInterval = config.GetString("SSLExpireCheckInterval")
	)

	//check sslcertificate expired or not
	s := gocron.NewScheduler(time.UTC)
	s.Cron(sslExpireCheckInterval).Do(sslcertificate.CheckSSLCertificatesStatus)
	s.StartAsync()

	//started replication receiver application
	logging.DoLoggingLevelBasedLogs(logging.Info, "receiver was started at port: "+serverPort, nil)
	server.StartServerApp()
}
