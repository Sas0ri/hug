package udp_parser

import (
	"crypto/md5"
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

type UDPParser struct {
	IUDPParser
}

type UDPPacket struct {
	D        uint32
	NextD    uint32
	Cmd      uint8
	Length   uint32
	Data     []byte
	CheckSum []byte
}

var Conn *net.UDPConn

func New(conn *net.UDPConn) *UDPParser {
	up := &UDPParser{}
	Conn = conn
	udp_packet.Init()
	return up
}

func (up *UDPParser) Parse(addr *net.UDPAddr, data []byte) {
	length := len(data)
	if length < 13 {
		return
	}
	pkt := readPacket(data)
	realData := data[:len(data)-4]

	h := md5.New()
	h.Write(realData)
	checkSumData := h.Sum(nil)
	if checkSumData[0] != pkt.CheckSum[0] || checkSumData[1] != pkt.CheckSum[1] || checkSumData[2] != pkt.CheckSum[2] || checkSumData[3] != pkt.CheckSum[3] {
		return
	}
	go udp_packet.Parse(addr, data)
	resPkt := &UDPPacket{D: pkt.D, NextD: pkt.NextD, Cmd: Cmd_Ack, Length: 0}
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
	pkt.Data = data[9:pkt.Length]
	pkt.CheckSum = data[len(pkt.Data)+9:]
	return
}

func writePacket(addr *net.UDPAddr, pkt *UDPPacket) {
	data := []byte{}
	data = append(data, byte(pkt.D>>24))
	data = append(data, byte(pkt.D>>16))
	data = append(data, byte(pkt.D>>8))
	data = append(data, byte(pkt.D))
	data = append(data, byte(pkt.NextD>>24))
	data = append(data, byte(pkt.NextD>>16))
	data = append(data, byte(pkt.NextD>>8))
	data = append(data, byte(pkt.NextD))
	data = append(data, byte(pkt.Cmd))
	data = append(data, byte(pkt.Length>>24))
	data = append(data, byte(pkt.Length>>16))
	data = append(data, byte(pkt.Length>>8))
	data = append(data, byte(pkt.Length))
	data = append(data, pkt.Data...)

	h := md5.New()
	h.Write(data)
	checkSum := h.Sum(nil)
	data = append(data, checkSum...)
	writeUDP(addr, data)
}

func WriteUDP(addr *net.UDPAddr, data []byte) {
	Conn.WriteTo(data, addr)
}