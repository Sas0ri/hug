package cmdhandler

import (
	"encoding/json"
	"hug/core/corps"
	"hug/core/users"
	"hug/imserver/connections"
	"hug/logs"
)

const (
	FileTransferCode_None uint8 = iota
	FileTransferCode_Offline
)

const (
	HandleFileTransfer_Accept uint8 = iota
	HandleFileTransfer_Reject
)

type FileTransferRequestPkt struct {
	uid      int64  `json:"uid"`
	ip       string `json:"ip"`
	fid      string `json:"fid"`
	path     string `json:"p"`
	fileName string `json:"fn"`
	fileSize int64  `json:"fs"`
}

type HandleFileTransferRequestPkt struct {
	FileTransferRequestPkt
	handleType uint8 `json:"t"`
}

type FileTransferResPkt struct {
	Code int8 `json:"c,omitempty"`
}

type FileTransferRequestHandler interface {
	CmdHandler
}

func (h *FileTransferRequestHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_FileTransferRequest
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *FileTransferRequestHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferResPkt
	resPkt.Code = FileTransferCode_None
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		}
		err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt FileTransferRequestPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	toPresence := connections.FindPresences(reqPkt.uid)
	online := toPresence != nil
	if online {
		for terminal, conn := range toPresence.Terminals {
			if terminal == users.TerminalType_PC {
				online = true
				conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
			}
		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.uid)
		resPkt.Code = FileTransferCode_Offline
	}
	return
}

type HandleFileTransferRequestHandler interface {
	CmdHandler
}

func (h *HandleFileTransferRequestHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_HandleDirectFileTransferRequest
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *HandleFileTransferRequestHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferResPkt
	resPkt.Code = FileTransferCode_None
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		}
		err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt HandleFileTransferRequestPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	toPresence := connections.FindPresences(reqPkt.uid)
	conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)

	return
}
