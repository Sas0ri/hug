package cmdhandler

import (
	"encoding/json"
	"hug/core/corps"
	"hug/imserver/connections"
	"hug/logs"
)

type CreateDeptHandler struct {
	CmdHandler
}

func (h *CreateDeptHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_CreateDept
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *CreateDeptHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.CreateDeptResPkt
	resPkt.Code = corps.CreateDeptCode_InvalidReq
	resPkt.Did = 0
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
	var reqPkt corps.CreateDeptReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Did, resPkt.Code = corps.CreateDept(reqPkt)
	return
}

type RemoveDeptHandler struct {
	CmdHandler
}

func (h *RemoveDeptHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveDept
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveDeptHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.RemoveDeptResPkt
	resPkt.Code = corps.RemoveDeptCode_InvalidReq
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
	var reqPkt corps.RemoveDeptReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Code = corps.RemoveDept(reqPkt)
	return
}

type GetDeptHandler struct {
	CmdHandler
}

func (h *GetDeptHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetDept
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetDeptHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.Dept
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
	var reqPkt corps.GetDeptReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	resPkt, err = corps.GetDept(reqPkt.Did)
	return
}

type SetDeptHandler struct {
	CmdHandler
}

func (h *SetDeptHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetDept
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetDeptHandler) packetIn(pkt connections.Packet) {
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt corps.Dept
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	corps.SetDept(reqPkt)
	return
}

func NewDeptHandlers(cmdHandlers *CmdHandlers) {
	createDeptHandler := &CreateDeptHandler{}
	createDeptHandler.initHandler(cmdHandlers)

	removeDeptHandler := &RemoveDeptHandler{}
	removeDeptHandler.initHandler(cmdHandlers)

	setDeptHandler := &SetDeptHandler{}
	setDeptHandler.initHandler(cmdHandlers)

	getDeptHandler := &GetDeptHandler{}
	getDeptHandler.initHandler(cmdHandlers)
}
