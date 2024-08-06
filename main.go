package main

import (
	"datareplication_receiver/config"
	"datareplication_receiver/server"
	"datareplication_receiver/storage/logging"
)

func main() {
	var (
		serverPort = config.GetString("HttpServer.Port")
	)

	//started replication receiver application
	logging.DoLoggingLevelBasedLogs(logging.Info, "receiver was started at port: "+serverPort, nil)
	server.StartServerApp()
}
