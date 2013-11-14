package webserver

import (
	"hug/logs"
	"log"
	"net/http"
)

const (
	TcpHostPort = "0.0.0.0:5222"
)

func Start() {
	log.Println("Starting web server...")
	logs.Logger.Info("Starting web server...")
	initUserReg()
	initFileUpload()
	go startListenUserPort()
	go startListenFilePort()
	log.Println("Starting web server successful.")
	logs.Logger.Info("Starting web server successful.")
}

func Stop() {

}

func startListenUserPort() {
	err := http.ListenAndServe(UserPort, nil) //设置监听的端口
	if err != nil {
		log.Fatal("start Listen web user port error: ", err)
	}
}

func startListenFilePort() {
	err := http.ListenAndServe(FilePort, nil) //设置监听的端口
	if err != nil {
		log.Fatal("start Listen web file port error: ", err)
	}
}
