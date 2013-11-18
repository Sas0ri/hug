package udpserver

import (
	"net"
)

const (
	UdpPort = 5223
)

var (
	conn *net.UDPConn
	rp   *IUDPRepacker
)

type IUDPRepacker interface {
	Repack(addr *net.UDPAddr, data []byte)
}

func Start() {
	log.Println("Starting udp server...")
	logs.Logger.Info("Starting udp server...")
	go startListenUserPort()
	log.Println("Starting udp server successful.")
	logs.Logger.Info("Starting udp server successful.")

	go startListenUdpPort()
}

func Stop() {

}

func startListenUdpPort() {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: UdpPort,
	})
	if err != nil {
		logs.Logger.info("Listen UDP failed:", err)
		return
	}
	defer socket.Close()

	conn = socket
	parser := UDPParser.New()
	rp = UDPRepacker.New(parser)

	for {
		// 读取数据
		data := make([]byte, 4096)
		len, remoteAddr, err := socket.ReadFromUDP(data)
		if err != nil {
			log.Logger.info("read udp failed!", err)
			continue
		}
		go rp.Repack(remoteAddr, data[:len])
	}
}