package cmdhandler

import (
	"encoding/json"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

const (
	FileTransferCode_None int8 = iota
	FileTransferCode_Offline
)

const (
	HandleFileTransfer_Accept int8 = iota
	HandleFileTransfer_Reject
)

func NewFileTransferHandlers(cmdHandlers *CmdHandlers) {
	fileTransferRequestHandler := &FileTransferRequestHandler{}
	fileTransferRequestHandler.initHandler(cmdHandlers)

	handleFileTransferRequestHandler := &HandleFileTransferRequestHandler{}
	handleFileTransferRequestHandler.initHandler(cmdHandlers)

	fileTransferStartNATHandler := &FileTransferStartNATHandler{}
	fileTransferStartNATHandler.initHandler(cmdHandlers)

	fileTransferLocalNATFailedHandler := &FileTransferLocalNATFailedHandler{}
	fileTransferLocalNATFailedHandler.initHandler(cmdHandlers)
}

type FileTransferRequestPkt struct {
	From     int64  `json:"fr"`
	To       int64  `json:""to`
	Ip       string `json:"ip"`
	Fid      string `json:"fid"`
	Path     string `json:"p"`
	FileName string `json:"fn"`
	FileSize int64  `json:"fs"`
}

type FileTransferResPkt struct {
	Code int8 `json:"c,omitempty"`
}

type FileTransferRequestHandler struct {
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
	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	if online {
		for _, conn := range toPresence.Terminals {
			online = true
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)

		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
	return
}

type HandleFileTransferRequestPkt struct {
	FileTransferRequestPkt
	HandleType uint8 `json:"t"`
}

type HandleFileTransferRequestHandler struct {
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
	toPresence := connections.FindPresences(reqPkt.From)
	online := toPresence != nil
	for _, conn := range toPresence.Terminals {
		conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
	return
}

type FileTransferStartNATRequestPkt struct {
	From int64  `json:"fr"`
	To   int64  `json:"to"`
	Fid  string `json:"fid"`
	Path string `json:"p"`
	Ip   string `json:"ip" omitempty`
}

type FileTransferStartNATResPkt struct {
	Code int8 `json:"code"`
}

type FileTransferStartNATHandler struct {
	CmdHandler
}

func (h *FileTransferStartNATHandler) initHandler(CmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_FileTransferStartNat
	CmdHandlers.handlers[h.Cmd] = h
}

func (h *FileTransferStartNATHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferStartNATResPkt
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
	var reqPkt FileTransferStartNATRequestPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	for _, conn := range toPresence.Terminals {
		conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
	}
	if !online {
		resPkt.Code = FileTransferCode_Offline
	}
	return
}

type FileTransferLocalNatFailed struct {
	From int64  `json:"fr"`
	To   int64  `json:"to"`
	Fid  string `json:"fid"`
	Path string `json:"p"`
}

type FileTransferLocalNatResPkt struct {
	Code int8 `json:"code"`
}

type FileTransferLocalNATFailedHandler struct {
	CmdHandler
}

func (h *FileTransferLocalNATFailedHandler) initHandler(CmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_FileTransferDirectLocalConnectFailed
	CmdHandlers.handlers[h.Cmd] = h
}

func (h *FileTransferLocalNATFailedHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferLocalNatResPkt
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
	var reqPkt FileTransferLocalNatFailed
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	for _, conn := range toPresence.Terminals {
		conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
	}
	if !online {
		resPkt.Code = FileTransferCode_Offline
	}
	return
}
