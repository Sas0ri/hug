package udpserver

import (
	"net"
)

type UDPParser interface {
	IUDPParser
}

func New() {
	up := &UDPParser{}
}

func (up *UDPParser) Parse(addr *net.UDPAddr, data []byte) {

}
