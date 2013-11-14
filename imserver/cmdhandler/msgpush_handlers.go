package cmdhandler

import (
	"encoding/json"
	"hug/core/messages"
	"hug/imserver/connections"
	"hug/logs"
)

type SetMsgPushHandler struct {
	CmdHandler
}

func (h *SetMsgPushHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetMsgPush
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetMsgPushHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.SetMsgPushReqPkt
	var resPkt messages.SetMsgPushResPkt
	defer func() {
		if resPkt.Code != messages.SetMsgPushCode_None {
			logs.Logger.Info("set message push failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		}
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
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		resPkt.Code = messages.SetMsgPushCode_InvalidRequest
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt = messages.SetMsgPush(reqPkt)
	return
}

type GetMsgPushHandler struct {
	CmdHandler
}

func (h *GetMsgPushHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetMsgPush
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetMsgPushHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.GetMsgPushReqPkt
	var resPkt messages.GetMsgPushResPkt
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
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Uid = reqPkt.Uid
	resPkt.Contact = reqPkt.Contact
	resPkt.Push = messages.IsMsgPush(reqPkt.Uid, reqPkt.Contact)
	return
}

func NewMsgPushHandlers(cmdHandlers *CmdHandlers) {
	setMsgPushHandler := &SetMsgPushHandler{}
	setMsgPushHandler.initHandler(cmdHandlers)

	getMsgPushHandler := &GetMsgPushHandler{}
	getMsgPushHandler.initHandler(cmdHandlers)
}
