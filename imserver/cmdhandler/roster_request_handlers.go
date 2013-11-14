package cmdhandler

import (
	"encoding/json"
	"hug/core/rosters"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

type RosterRequestHandler struct {
	CmdHandler
}

func (h *RosterRequestHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RosterRequest
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RosterRequestHandler) packetIn(pkt connections.Packet) {
	logs.Logger.Infof("handle cmd = 0x%02x", h.Cmd)
	var reqPkt rosters.RosterRequest
	var resPkt rosters.RosterRequestResPkt
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
	request, code := rosters.CreateRosterRequest(reqPkt)
	resPkt.Code = code
	resPkt.RequestId = request.RequestId
	logs.Logger.Infof("code = %v requestId = %v", code, request.RequestId)
	if resPkt.Code == rosters.HandleRosterRequestCode_None {
		wtBytes, err := json.Marshal(request)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}

		presence := connections.FindPresences(request.ToUid)
		if presence != nil {
			for _, conn := range presence.Terminals {
				err = conn.WritePacket(Cmd_RosterRequest, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
				if err != nil {
					logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
					return
				}
			}
		}
	}
	return
}

type GetRosterRequestsHandler struct {
	CmdHandler
}

func (h *GetRosterRequestsHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetRosterRequests
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetRosterRequestsHandler) packetIn(pkt connections.Packet) {
	logs.Logger.Infof("handle cmd = 0x%02x", h.Cmd)
	var reqPkt rosters.GetRosterRequestReqPkt
	var resPkt rosters.GetRosterReqeustResPkt
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
	resPkt = rosters.GetRosterReqeusts(reqPkt)
	return
}

type HandleRosterRequestHandler struct {
	CmdHandler
}

func (h *HandleRosterRequestHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_HandleRosterRequest
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *HandleRosterRequestHandler) packetIn(pkt connections.Packet) {
	logs.Logger.Infof("handle cmd = 0x%02x", h.Cmd)
	var reqPkt rosters.HandleRosterRequestReqPkt
	var resPkt rosters.HandleRosterRequestResPkt
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
	resPkt = rosters.HandleRosterRequest(reqPkt)
	return
}

type SetIgnoreRosterRequestHandler struct {
	CmdHandler
}

func (h *SetIgnoreRosterRequestHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetIgnoreRosterRequest
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetIgnoreRosterRequestHandler) packetIn(pkt connections.Packet) {
	logs.Logger.Infof("handle cmd = 0x%02x", h.Cmd)
	var reqPkt rosters.IgnoreRequest
	var resPkt rosters.SetIgnoreRequestResPkt
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
	resPkt = rosters.SetIgnoreRequest(reqPkt)
	return
}

func SendHandleRosterRequestNotificationLoop() {
	var reqPkt rosters.HandleRosterRequestNotification
	for {
		reqPkt = <-rosters.HandleRosterRequestNotificationChan
		wtBytes, err := json.Marshal(reqPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}

		presence := connections.FindPresences(reqPkt.FromUid)
		if presence != nil {
			for _, conn := range presence.Terminals {
				err = conn.WritePacket(Cmd_HandleRosterRequestNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
				if err != nil {
					logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
					return
				}
			}
		}
	}
}

func NewRosterRequestHandlers(cmdHandlers *CmdHandlers) {
	rosterRequestHandler := &RosterRequestHandler{}
	rosterRequestHandler.initHandler(cmdHandlers)

	getRosterRequestsHandler := &GetRosterRequestsHandler{}
	getRosterRequestsHandler.initHandler(cmdHandlers)

	handleRosterRequestHandler := HandleRosterRequestHandler{}
	handleRosterRequestHandler.initHandler(cmdHandlers)

	setIgnoreRosterRequestHandler := &SetIgnoreRosterRequestHandler{}
	setIgnoreRosterRequestHandler.initHandler(cmdHandlers)

	go SendHandleRosterRequestNotificationLoop()
}
