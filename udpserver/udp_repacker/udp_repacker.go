package udp_repacker

import (
	"hug/imserver/connections"
	"hug/udpserver/udp_parser"
	"hug/utils/beemap"
	"net"
)

type UDPRepacker struct {
	connMap *beemap.BeeMap
	parser  *udp_parser.IUDPParser
}

func New() *UDPRepacker {
	rp := &UDPRepacker{
		connMap: beemap.NewBeeMap(),
	}
	return rp
}

func (rp *UDPRepacker) Repack(addr *net.UDPAddr, data []byte) {
	buf, _ := rp.connMap.Get(addr).([]byte)
	if buf == nil && data[0] == connections.Pkt_STX {
		buf = data
		rp.connMap.Set(addr, data)
	} else {
		buf = append(buf, data...)
	}
	length := len(buf)
	if length > 2 && (buf[length-2] != connections.Pkt_DLE && buf[length-1] == connections.Pkt_ETX) {
		udp_parser.Parse(addr, buf)
		rp.connMap.Delete(addr)
	}
}
