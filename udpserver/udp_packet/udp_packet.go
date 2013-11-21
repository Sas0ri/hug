package udp_packet

import (
	"encoding/json"
	// "hug/imserver/cmdhandler"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils/beemap"
	"math/rand"
	"net"
	"time"
)

var session *beemap

const (
	Type_Request uint8 = iota
	Type_Ack
)

type UDPStreamPacket struct {
	Key        uint8
	Type       uint8
	Cmd        uint8
	JsonLength int16
	DataLength int64
	JsonData   []byte
	Data       []byte
}

func Init() {
	session = beemap.NewBeeMap()
}

func Parse(addr *net.UDPAddr, data []byte) {
	stPkt := &UDPStreamPacket{}
	stPkt.Key = data[0]
	for i := 0; i < len(data)-1; i++ {
		data[i] = data[i] ^ stPkt.Key
	}
	stPkt.Type = data[1]
	stPkt.Cmd = data[2]
	stPkt.JsonLength = (int16)(data[3] << 8)
	stPkt.JsonLength += (int16)(data[4])
	stPkt.DataLength = (int64)(data[5] << 24)
	stPkt.DataLength = (int64)(data[6] << 16)
	stPkt.DataLength = (int64)(data[7] << 8)
	stPkt.DataLength = (int64)(data[8])

	stPkt.JsonData = data[9:stPkt.JsonLength]
	stPkt.Data = data[9+stPkt.JsonLength:]
	handlePacket(stPkt)
}

type FileTransferLinkServerPkt struct {
	From         int64       `json:"fr,omitempty"`
	To           int64       `json:"to,omitempty"`
	Uid          int64       `json:"Uid"`
	FromTerminal int16       `json:"ft"`
	ToTerminal   int16       `json:"tt"`
	SessionId    string      `json:"sid"`
	Ip           net.UDPAddr `json:"ip,omitempty"`
}

type FileTransferStartNATRequestPkt struct {
	Ip        string `json:"ip"`
	SessionId string `json:"sid,omitempty"`
	Code      int8   `json:"code"`
}

const (
	FileTransferStartWanNatCode_None uint8 = iota
	FileTransferStartWanNatCode_Offline
)

func handlePacket(pkt *UDPStreamPacket) {
	var p1 FileTransferLinkServerPkt
	json.Unmarshal(pkt.JsonData, &p1)
	var p2 FileTransferLinkServerPkt = session.Get(p1.SessionId)
	if p2 == nil {
		session.Set(p1.SessionId, p1)
		go func() {
			select {
			case <-time.After(time.Second * 5):
				session.Delete(p1.SessionId)
				break
			}
		}()
	} else {
		offline := true
		var conn1 *net.Conn
		var conn2 *net.Conn
		presence := connections.FindPresences(p1.Uid)
		for terminal, conn := range presence.Terminals {
			if terminal == p1.FromTerminal {
				online = false
				conn1 = conn
				break
			}
		}
		presence = connections.FindPresences(p2.Uid)
		for terminal, conn := range presence.Terminals {
			if terminal == p1.FromTerminal {
				online = false
				conn2 = conn
				break
			}
		}
		if offline {
			var reqPkt FileTransferStartNATRequestPkt
			reqPkt.Code = FileTransferStartWanNatCode_Offline
			reqPkt.SessionId = p1.SessionId
			resData := json.Marshal(reqPkt)
			if conn1 != nil {
				conn1.WritePacket(Cmd_FileTransferStartWanNat, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData)
			}
			if conn2 != nil {
				conn2.WritePacket(Cmd_FileTransferStartWanNat, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData)
			}
			return
		}
		var reqPkt1 FileTransferStartNATRequestPkt
		reqPk1.SessionId = p1.SessionId
		reqPkt1.Ip = p2.Ip.String()
		resDat1, err := json.Marshal(reqPkt1)
		if err != nil {
			logs.Logger.Critical("json unmarshal err", err)
			return
		}
		var reqPkt2 FileTransferStartNATRequestPkt
		reqPkt2.SessionId = p2.SessionId
		reqPkt2.Ip = p1.Ip.String()
		resData2, err := json.Marshal(reqPkt1)
		if err != nil {
			logs.Logger.Critical("json unmarshal err", err)
			return
		}
		conn1.WritePacket(Cmd_FileTransferStartWanNat, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData1)
		conn2.WritePacket(Cmd_FileTransferStartWanNat, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), resData2)
	}
}
