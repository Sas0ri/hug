package cmdhandler

import (
	"encoding/json"
	"hug/core/groups"
	"hug/core/users"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

type CreateGroupHandler struct {
	CmdHandler
}

func (h *CreateGroupHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_CreateGroup
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *CreateGroupHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.CreateGroupReqPkt
	var resPkt groups.CreateGroupResPkt
	resPkt.Code = groups.CreateGroupCode_InvalidReq
	resPkt.Gid = 0
	defer func() {
		if resPkt.Code != groups.CreateGroupCode_None {
			logs.Logger.Info("create failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
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
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt = groups.CreateGroup(reqPkt)
	return
}

type RemoveGroupHandler struct {
	CmdHandler
}

func (h *RemoveGroupHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveGroup
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveGroupHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.RemoveGroupReqPkt
	var resPkt groups.RemoveGroupResPkt
	resPkt.Code = groups.RemoveGroupCode_InvalidReq
	defer func() {
		if resPkt.Code != groups.RemoveGroupCode_None {
			logs.Logger.Info("create failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
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
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Code = groups.RemoveGroup(reqPkt)
	resPkt.Gid = reqPkt.Gid
	return
}

// type GetGroupsOfUserHandler struct {
// 	CmdHandler
// }

// func (h *GetGroupsOfUserHandler) initHandler(cmdHandlers *CmdHandlers) {
// 	h.Cmd = Cmd_GetGroupsOfUser
// 	cmdHandlers.handlers[h.Cmd] = h
// }

// func (h *GetGroupsOfUserHandler) packetIn(pkt connections.Packet) {
// 	var reqPkt groups.GetGroupsOfUserReqPkt
// 	var resPkt groups.GetGroupsOfUserResPkt
// 	defer func() {
// 		resData, err := json.Marshal(resPkt)
// 		if err != nil {
// 			log.Fatal("GetGroupsOfUserHandler json marshal respacket error:", err)
// 		}
// 		err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, resData)
// 		if err != nil {
// 			log.Println("GetGroupsOfUserHandler: Conn write response packet error =", err)
// 			return
// 		}
// 	}()
// 	err := json.Unmarshal(pkt.Data, &reqPkt)
// 	if err != nil {
// 		log.Println("GetGroupsOfUserHandler error: json unmarshal error:", err)
// 		return
// 	}
// 	resPkt = groups.GetGroupsOfUser(reqPkt)
// 	return
// }

type GetGidsHandler struct {
	CmdHandler
}

func (h *GetGidsHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetGids
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetGidsHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.GetGidsReqPkt
	var resPkt groups.GetGidsResPkt
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
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Gids = groups.GetGidsOfUid(reqPkt.Uid)
	return
}

type GetGroupsHandler struct {
	CmdHandler
}

func (h *GetGroupsHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetGroups
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetGroupsHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.GetGroupsReqPkt
	var resPkt groups.GetGroupsResPkt
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
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Groups = groups.GetGroups(reqPkt.Gids)
	return
}

type SetGroupHandler struct {
	CmdHandler
}

func (h *SetGroupHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetGroup
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetGroupHandler) packetIn(pkt connections.Packet) {
	var resPkt groups.SetGroupResPkt
	defer func() {
		if resPkt.Code != groups.SetGroupCode_None {
			logs.Logger.Info("set group failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
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
	var reqPkt groups.Group
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		resPkt.Code = groups.SetGroupCode_InvalidFormat
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	groups.SetGroup(reqPkt)
	return
}

type GetGroupChangedHandler struct {
	CmdHandler
}

func (h *GetGroupChangedHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetGroupChanged
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetGroupChangedHandler) packetIn(pkt connections.Packet) {
	var resPkt groups.GetGroupChangedResPkt
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
	var reqPkt groups.GetGroupChangedReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Notifications = make([]groups.GroupChangedNotification, 0, 10)
	resPkt.Stamp = 0
	for _, gid := range reqPkt.Gids {
		changes, tempStamp := groups.GetGroupChanges(gid, reqPkt.Stamp)
		resPkt.Notifications = append(resPkt.Notifications, changes...)
		if tempStamp > resPkt.Stamp {
			resPkt.Stamp = tempStamp
		}
	}
	return
}

func SendGroupChangedNotificationLoop() {
	var reqPkt groups.GroupChangedNotification
	for {
		reqPkt = <-groups.GroupChangedNotificationChan
		logs.Logger.Info("push group change notification: gid = ", reqPkt.Gid, " uid = ", reqPkt.Uid, " type = ", reqPkt.Type)
		wtBytes, err := json.Marshal(reqPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}
		uids := groups.GetGroupUids(reqPkt.Gid)
		if reqPkt.Type == groups.GroupChangedType_Removed && users.IsUidValid(reqPkt.Uid) {
			uids = append(uids, reqPkt.Uid)
		}
		for _, uid := range uids {
			presence := connections.FindPresences(uid)
			if presence != nil {
				for _, conn := range presence.Terminals {
					err = conn.WritePacket(Cmd_GroupChangedNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
					if err != nil {
						logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
						return
					}
				}
			}
		}
		if reqPkt.Type == groups.GroupChangedType_Removed && reqPkt.Uid == 0 {
			groups.DeleteAllMemberOfGroup(reqPkt.Gid)
		}
	}
}

func NewGroupHandlers(cmdHandlers *CmdHandlers) {
	createGroupHandler := &CreateGroupHandler{}
	createGroupHandler.initHandler(cmdHandlers)

	removeGroupHandler := &RemoveGroupHandler{}
	removeGroupHandler.initHandler(cmdHandlers)

	// getGroupsOfUserHandler := &GetGroupsOfUserHandler{}
	// getGroupsOfUserHandler.initHandler(cmdHandlers)

	getGidsHandler := &GetGidsHandler{}
	getGidsHandler.initHandler(cmdHandlers)

	getGroupsHandler := &GetGroupsHandler{}
	getGroupsHandler.initHandler(cmdHandlers)

	setGroupHandler := &SetGroupHandler{}
	setGroupHandler.initHandler(cmdHandlers)

	getGroupChangedHandler := &GetGroupChangedHandler{}
	getGroupChangedHandler.initHandler(cmdHandlers)

	go SendGroupChangedNotificationLoop()

}
