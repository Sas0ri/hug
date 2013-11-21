package cmdhandler

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hug/imserver/connections"
	"hug/logs"
	"io"
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

	fileTransferStartLanNATHandler := &FileTransferStartLanNATHandler{}
	fileTransferStartLanNATHandler.initHandler(cmdHandlers)

	fileTransferLocalNATFailedHandler := &FileTransferLocalNATFailedHandler{}
	fileTransferLocalNATFailedHandler.initHandler(cmdHandlers)
}

type FileTransferRequestPkt struct {
	From         int64  `json:"fr,omitempty"`
	To           int64  `json:"to,omitempty"`
	Ip           string `json:"ip,omitempty"`
	Fid          string `json:"fid,omitempty"`
	Path         string `json:"p,omitempty"`
	FileName     string `json:"fn,omitempty"`
	FileSize     int64  `json:"fs,omitempty"`
	FromTerminal int16  `json:"ft,omitempty"`
	ToTerminal   int16  `json:"tt,omitempty"`
	SessionId    string `json:"sid,omitempty"`
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

	m := md5.New()
	io.WriteString(m, reqPkt.Fid)
	io.WriteString(m, reqPkt.Path)
	reqPkt.SessionId = fmt.Sprintf("%x", m.Sum(nil))
	reqPkt.FromTerminal = pkt.Conn.AuthInfo.TerminalType
	reqData, err := json.Marshal(reqPkt)
	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	if online {
		for _, conn := range toPresence.Terminals {
			online = true
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), reqData)

		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
	return
}

type HandleFileTransferRequestPkt struct {
	From         int64  `json:"fr,omitempty"`
	To           int64  `json:"to,omitempty"`
	Ip           string `json:"ip,omitempty"`
	SessionId    string `json:"sid,omitempty"`
	FromTerminal int16  `json:"ft,omitempty"`
	ToTerminal   int16  `json:"tt,omitempty"`
	HandleType   int8   `json:"t,omitempty"`
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
	reqPkt.ToTerminal = pkt.Conn.AuthInfo.TerminalType
	reqData, err := json.Marshal(reqPkt)
	toPresence := connections.FindPresences(reqPkt.From)
	online := toPresence != nil
	for terminalType, conn := range toPresence.Terminals {
		if terminalType == reqPkt.FromTerminal {
			online = true
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), reqData)
		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
	//给自己的其他在线客户端发送处理消息
	fromPresence := connections.FindPresences(reqPkt.To)
	for terminalType, conn := range fromPresence.Terminals {
		if terminalType != reqPkt.ToTerminal {
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), reqData)
		}
	}
	return
}

type FileTransferStartLanNATReqPkt struct {
	From         int64  `json:"fr"`
	To           int64  `json:"to"`
	FromTerminal int16  `json:"ft,omitempty"`
	ToTerminal   int16  `json:"tt,omitempty"`
	SessionId    string `json:"sid,omitempty"`
}

type FileTransferStartLanNATResPkt struct {
	SessionId string `json:"sid,omitempty"`
	Code      int8   `json:"code"`
}

type FileTransferStartLanNATHandler struct {
	CmdHandler
}

func (h *FileTransferStartLanNATHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_FileTransferStartLanNat
	cmdHandlers.handlers[Cmd_FileTransferStartLanNat] = h
}

func (h *FileTransferStartLanNATHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferStartLanNATResPkt
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
	var reqPkt FileTransferStartLanNATReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.SessionId = reqPkt.SessionId

	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	for terminalType, conn := range toPresence.Terminals {
		if terminalType == reqPkt.ToTerminal {
			online = true
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
}

type FileTransferLocalNatFailedReqPkt struct {
	From         int64  `json:"fr"`
	To           int64  `json:"to"`
	FromTerminal int16  `json:"ft,omitempty"`
	ToTerminal   int16  `json:"tt,omitempty"`
	SessionId    string `json:"sid"`
}

type FileTransferLocalNatFailedResPkt struct {
	SessionId string `json:"sid"`
	Code      int8   `json:"code"`
}

type FileTransferLocalNATFailedHandler struct {
	CmdHandler
}

func (h *FileTransferLocalNATFailedHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_FileTransferLocalNATFailed
	cmdHandlers.handlers[Cmd_FileTransferStartLanNat] = h
}

func (h *FileTransferLocalNATFailedHandler) packetIn(pkt connections.Packet) {
	var resPkt FileTransferLocalNatFailedResPkt
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
	var reqPkt FileTransferLocalNatFailedReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.SessionId = reqPkt.SessionId

	toPresence := connections.FindPresences(reqPkt.To)
	online := toPresence != nil
	for terminalType, conn := range toPresence.Terminals {
		if terminalType == reqPkt.ToTerminal {
			online = true
			conn.WritePacket(Cmd_FileTransferRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), pkt.Data)
		}
	}
	if !online {
		logs.Logger.Infof("PC User %lld is not online", reqPkt.To)
		resPkt.Code = FileTransferCode_Offline
	}
}
