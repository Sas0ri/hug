package udp_packet

import (
	"encoding/json"
	"hug/imserver/cmdhandler"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils/beemap"
	"math/rand"
	"net"
	"strconv"
)

var session *beemap

type UDPStreamPacket struct {
	From int64  `json:"fr"`
	To   int64  `json:"to"`
	Uid  int64  `json:"uid"`
	Fid  string `json:"fid"`
	Path string `json:"p"`
}

func Init() {
	session = beemap.NewBeeMap()
}

func Parse(addr *net.UDPAddr, data []byte) {
	stPkt := &UDPStreamPacket{}
	err := json.Unmarshal(data, &stPkt)
	if err != nil {
		logs.Logger.Warn("unmarshal UDP packet err", err)
		return
	}
	k := strconv.Itoa(stPkt.From)
	k += strconv.Itoa(stPkt.To)
	k += stPkt.Fid
	k += stPkt.Path
	addr1 := session.Get(k)
	if !addr1 {
		session.Set(k, addr)
		return
	}
	session.Delete(k)
	var fpkt cmdhandler.FileTransferStartNATRequestPkt
	fpkt.From = stPkt.From
	fpkt.To = stPkt.To
	fpkt.Fid = stPkt.Fid
	fpkt.Path = stPkt.Path
	sendTo := fpkt.From
	if stPkt.From == stPkt.Uid {
		sendto = fpkt.To
	}
	fpkt.Ip = addr.String()
	toPresence := connections.FindPresences(sendto)
	resData, err := json.Marshal(fpkt)
	for _, conn := range toPresence.Terminals {
		conn.WritePacket(cmdhandler.Cmd_FileTransferStartNat, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData)
	}
	fpkt.Ip = addr1.String()
	toPresence = connections.FindPresences(stPkt.Uid)
	resData, err = json.Marshal(fpkt)
	for _, conn := range toPresence {
		conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData)

	}
}
