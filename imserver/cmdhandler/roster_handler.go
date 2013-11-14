package cmdhandler

import (
	"encoding/json"
	"hug/core/rosters"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

type GetAllRosterHandler struct {
	CmdHandler
}

func (h *GetAllRosterHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetAllRoster
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetAllRosterHandler) packetIn(pkt connections.Packet) {
	var reqPkt rosters.GetAllRostersReqPkt
	var resPkt rosters.GetAllRostersResPkt
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
	resPkt.Rosters = rosters.GetRostersOfUid(reqPkt.Uid)
	resPkt.Uid = reqPkt.Uid
	return
}

type GetRostersHandler struct {
	CmdHandler
}

func (h *GetRostersHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetRosters
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetRostersHandler) packetIn(pkt connections.Packet) {
	var reqPkt rosters.GetRostersReqPkt
	var resPkt rosters.GetRostersResPkt
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
	resPkt.Rosters, _ = rosters.GetRosters(reqPkt.Uid, reqPkt.Ruids)
	resPkt.Uid = reqPkt.Uid
	return
}

type SetRosterHandler struct {
	CmdHandler
}

func (h *SetRosterHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetRoster
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetRosterHandler) packetIn(pkt connections.Packet) {
	var reqPkt rosters.Roster
	var resPkt rosters.SetRosterResPkt
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
	resPkt.Code = rosters.SetRoster(reqPkt)
	return
}

type RemoveRosterHandler struct {
	CmdHandler
}

func (h *RemoveRosterHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveRoster
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveRosterHandler) packetIn(pkt connections.Packet) {
	var reqPkt rosters.RemoveRosterReqPkt
	var resPkt rosters.RemoveRosterResPkt
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
	resPkt.Code = rosters.RemoveRoster(reqPkt)
	resPkt.Uid = reqPkt.Uid
	resPkt.Ruid = reqPkt.Ruid
	return
}

type GetRosterChangedHandler struct {
	CmdHandler
}

func (h *GetRosterChangedHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetRosterChanged
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetRosterChangedHandler) packetIn(pkt connections.Packet) {
	var resPkt rosters.GetRosterChangedResPkt
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
	var reqPkt rosters.GetRosterChangedReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Notifications, resPkt.Stamp = rosters.GetRosterChanges(reqPkt.Uid, reqPkt.Stamp)
	return
}

func SendRosterChangedNotificationLoop() {
	var reqPkt rosters.RosterChangedNotification
	for {
		reqPkt = <-rosters.RosterChangedNotificationChan
		wtBytes, err := json.Marshal(reqPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}

		presence := connections.FindPresences(reqPkt.Uid)
		if presence != nil {
			for _, conn := range presence.Terminals {
				err = conn.WritePacket(Cmd_RosterChangedNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
				if err != nil {
					logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
					return
				}
			}
		}
	}
}

func NewRosterHandlers(cmdHandlers *CmdHandlers) {
	getAllRosterHandler := &GetAllRosterHandler{}
	getAllRosterHandler.initHandler(cmdHandlers)

	getRostersHandler := &GetRostersHandler{}
	getRostersHandler.initHandler(cmdHandlers)

	setRosterHandler := &SetRosterHandler{}
	setRosterHandler.initHandler(cmdHandlers)

	removeRosterHandler := &RemoveRosterHandler{}
	removeRosterHandler.initHandler(cmdHandlers)

	getRosterChangedHandler := &GetRosterChangedHandler{}
	getRosterChangedHandler.initHandler(cmdHandlers)

	go SendRosterChangedNotificationLoop()

}
