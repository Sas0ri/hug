package udp_parser

import (
	"hash/crc32"
	"hug/imserver/connections"
	"hug/udpserver/udp_packet"
	"net"
)

type IUDPParser interface {
	Parse(addr *net.UDPAddr, data []byte)
}

const (
	Cmd_Request uint8 = iota
	Cmd_Ack
)

type UDPPacket struct {
	D        uint32
	Cmd      uint8
	Length   uint32
	Data     []byte
	CheckSum []byte
}

var Conn *net.UDPConn

func Init(conn *net.UDPConn) {
	Conn = conn
	udp_packet.Init()
	return
}

func Parse(addr *net.UDPAddr, data []byte) {
	data = unescape(data)
	length := len(data)
	if length < 13 {
		return
	}
	pkt := readPacket(data)
	realData := data[:len(data)-4]

	checkSum := crc32.ChecksumIEEE(realData)
	pktCheckSum := (uint32)(pkt.CheckSum[0] << 24)
	pktCheckSum += (uint32)(pkt.CheckSum[0] << 16)
	pktCheckSum += (uint32)(pkt.CheckSum[0] << 8)
	pktCheckSum += (uint32)(pkt.CheckSum[0])
	if checkSum != pktCheckSum {
		return
	}
	go udp_packet.Parse(addr, data)
	resPkt := &UDPPacket{D: pkt.D, Cmd: Cmd_Ack, Length: 0}
	writePacket(addr, resPkt)
}

func readPacket(data []byte) (pkt *UDPPacket) {
	pkt = &UDPPacket{}
	pkt.D = (uint32)(data[0] << 24)
	pkt.D += (uint32)(data[1] << 16)
	pkt.D += (uint32)(data[2] << 8)
	pkt.D += (uint32)(data[3])
	pkt.Cmd = (uint8)(data[4])
	pkt.Length = (uint32)(data[5] << 24)
	pkt.Length += (uint32)(data[6] << 16)
	pkt.Length += (uint32)(data[7] << 8)
	pkt.Length += (uint32)(data[8])
	pkt.Data = data[9 : pkt.Length+9]
	pkt.CheckSum = data[len(pkt.Data)+9:]
	return
}

func unescape(data []byte) (buf []byte) {
	len := len(data)
	buf = []byte{}
	for i := 0; i < len; i++ {
		if data[i] != connections.Pkt_DLE {
			buf = append(buf, data[i])
		}
	}
	return
}

func escape(data []byte) (buf []byte) {
	buf = []byte{}
	for i := 0; i < len(data); i++ {
		if data[i] == connections.Pkt_STX || data[i] == connections.Pkt_ETX || data[i] == connections.Pkt_DLE {
			buf = append(buf, connections.Pkt_DLE)
		}
		buf = append(buf, data[i])
	}
	return
}

func writePacket(addr *net.UDPAddr, pkt *UDPPacket) {
	data := []byte{}
	data = append(data, byte(pkt.D>>24))
	data = append(data, byte(pkt.D>>16))
	data = append(data, byte(pkt.D>>8))
	data = append(data, byte(pkt.D))
	data = append(data, byte(pkt.Cmd))
	data = append(data, byte(pkt.Length>>24))
	data = append(data, byte(pkt.Length>>16))
	data = append(data, byte(pkt.Length>>8))
	data = append(data, byte(pkt.Length))
	data = append(data, pkt.Data...)

	checkSum := crc32.ChecksumIEEE(data)
	data = append(data, byte(checkSum>>24))
	data = append(data, byte(checkSum>>16))
	data = append(data, byte(checkSum>>8))
	data = append(data, byte(checkSum))
	data = escape(data)

	WriteUDP(addr, data)
}

func WriteUDP(addr *net.UDPAddr, data []byte) {
	Conn.WriteTo(data, addr)
}
