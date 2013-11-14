package cmdhandler

import (
	"encoding/json"
	"hug/core/users"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

type GetUserInfosHandler struct {
	CmdHandler
}

func (h *GetUserInfosHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetUserInfos
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetUserInfosHandler) packetIn(pkt connections.Packet) {
	var resPkt users.GetUserInfosResPkt
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
	var reqPkt users.GetUserInfosReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Infos, _ = users.GetUserInfos(reqPkt.Uids)
	return
}

type SetUserInfosHandler struct {
	CmdHandler
}

func (h *SetUserInfosHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetUserInfo
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetUserInfosHandler) packetIn(pkt connections.Packet) {
	var resPkt users.SetUserInfoResPkt
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
	var reqPkt users.SetUserInfoReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	if reqPkt.Info.Uid != pkt.Conn.AuthInfo.Uid {
		resPkt.Code = users.SetUserInfoCOde_NoPermission
		return
	}
	resPkt.Code, resPkt.Stamp = users.UpdateUserInfo(reqPkt)
	return
}

type GetUserInfoChangedHandler struct {
	CmdHandler
}

func (h *GetUserInfoChangedHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetUserInfoChanged
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetUserInfoChangedHandler) packetIn(pkt connections.Packet) {
	var resPkt users.GetUserInfoChangedResPkt
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
	var reqPkt users.GetUserInfoChangedReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	uids := GetRelationUids(reqPkt.Uid)
	changedUids := make([]int64, 0, len(uids))
	for _, tempUid := range uids {
		if users.IsUserInfoChanged(tempUid, reqPkt.Stamp) {
			changedUids = append(changedUids, tempUid)
		}
	}
	resPkt.Infos, _ = users.GetUserInfos(changedUids)
	return
}

func SendUserInfoChangedNotificationLoop() {
	var reqPkt users.UserInfoChangedNotificationPkt
	for {
		reqPkt.Uid = <-users.UserInfoChangedNotificationChan
		wtBytes, err := json.Marshal(reqPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}
		uids := GetRelationUids(reqPkt.Uid)
		for _, uid := range uids {
			presence := connections.FindPresences(uid)
			if presence != nil {
				for _, conn := range presence.Terminals {
					err = conn.WritePacket(Cmd_UserInfoChangedNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
					if err != nil {
						logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
						return
					}
				}
			}
		}
	}
}

func NewUserInfoHandlers(cmdHandlers *CmdHandlers) {
	setUserInfoHandler := &SetUserInfosHandler{}
	setUserInfoHandler.initHandler(cmdHandlers)

	getUserInfosHandler := &GetUserInfosHandler{}
	getUserInfosHandler.initHandler(cmdHandlers)

	getUserInfoChangedHandler := &GetUserInfoChangedHandler{}
	getUserInfoChangedHandler.initHandler(cmdHandlers)

	go SendUserInfoChangedNotificationLoop()
}
