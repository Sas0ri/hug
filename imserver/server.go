package imserver

import (
	"hug/imserver/cmdhandler"
	"hug/imserver/connections"
	"hug/logs"
	"log"
	"net"
	"os"
)

const (
	TcpHostPort = "0.0.0.0:5222"
)

func Start() {
	log.Println("Starting IM Server...")
	logs.Logger.Info("Starting IM Server...")

	listener, err := net.Listen("tcp", TcpHostPort)
	if err != nil {
		log.Fatal("Starting IM Server error!", err.Error())
		os.Exit(1)
	}

	connections.StartManagePresences()

	cmdhandlers := cmdhandler.NewCmdHanglers()

	log.Println("Starting IM server successful!")
	logs.Logger.Info("Starting IM Server successful.")
	for {
		conn, err := listener.Accept()
		if err != nil {
			logs.Logger.Critical("Client connect listener error!", err)
			continue
		}
		logs.Logger.Info("New client connected in:", conn.RemoteAddr())
		connections.New(conn, cmdhandlers.PacketQueue)
	}
}

func Stop() {

}
