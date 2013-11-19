package udp_repacker

import (
	"hug/imserver/connections"
	"hug/udpserver/udp_parser"
	"hug/utils/beemap"
	"net"
)

type IUDPRepacker interface {
	Repack(addr *net.UDPAddr, data []byte)
}

type UDPRepacker struct {
	IUDPRepacker
	connMap *beemap
	parser  *IUDPParser
}

func New(parser *IUDPParser) *UDPRepacker {
	rp := &UDPRepacker{
		connMap: &beemap.NewBeeMap(),
		parser:  &parser,
	}
	return rp
}

func (rp *UDPRepacker) Repack(addr *net.UDPAddr, data []byte) {
	buf := rp.connMap.Get(addr)
	if buf == nil && data[0] == connections.Pkt_STX {
		buf = data
		rp.connMap.Set(addr, data)
	} else {
		append(buf, data)
	}
	length := len(buf)
	if length > 2 && (buf[length-2] != connections.Pkt_DLE && buf[length-1] == connections.Pkt_ETX) {
		rp.parser.Parse(addr, buf)
		rp.connMap.Delete(addr)
	}
}
