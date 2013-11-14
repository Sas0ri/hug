package main

import (
	"hug/core"
	"hug/imserver"
	"hug/logs"
	"hug/webserver"
	"log"
	"runtime"
)

func main() {
	logs.InitLogger()
	//logs.Logger.Info("Starting server...")
	log.Println("Starting Server...")
	//log.Println("NumCPU =", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	core.Start()
	defer core.Stop()

	webserver.Start()
	defer webserver.Stop()

	imserver.Start()
	defer imserver.Stop()

}
