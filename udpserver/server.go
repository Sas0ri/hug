package udpserver

import (
	"hug/logs"
	"hug/udpserver/udp_repacker"
	"net"
)

const (
	UdpPort = 5223
)

var (
	Conn *net.UDPConn
	rp   *udp_repacker.UDPRepacker
)

func Start() {
	logs.Logger.Info("Starting udp server...")
	go startListenUdpPort()
	logs.Logger.Info("Starting udp server successful.")
}

func Stop() {

}

func startListenUdpPort() {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: UdpPort,
	})
	if err != nil {
		logs.Logger.Info("Listen UDP failed:", err)
		return
	}
	defer socket.Close()

	Conn = socket
	rp = udp_repacker.New()

	for {
		// 读取数据
		data := make([]byte, 4096)
		len, remoteAddr, err := socket.ReadFromUDP(data)
		if err != nil {
			logs.Logger.Info("read udp failed!", err)
			continue
		}
		go rp.Repack(remoteAddr, data[:len])
	}
}
