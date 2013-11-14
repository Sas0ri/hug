package cmdhandler

import (
	"encoding/json"
	"hug/core/groups"
	"hug/imserver/connections"
	"hug/logs"
)

type AddGroupMembersHandler struct {
	CmdHandler
}

func (h *AddGroupMembersHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_AddGroupMembers
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *AddGroupMembersHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.GroupMembersChangeReqPkt
	var resPkt groups.GroupMembersChangeResPkt
	resPkt.Code = groups.GroupMemberChangeCode_InvalidReq
	defer func() {
		if resPkt.Code != groups.GroupMemberChangeCode_None {
			logs.Logger.Warn("create failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
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
	resPkt.Code = groups.AddGroupMembers(reqPkt)
	return
}

type RemoveGroupMembersHandler struct {
	CmdHandler
}

func (h *RemoveGroupMembersHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveGroupMembers
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveGroupMembersHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.GroupMembersChangeReqPkt
	var resPkt groups.GroupMembersChangeResPkt
	resPkt.Code = groups.GroupMemberChangeCode_InvalidReq
	defer func() {
		if resPkt.Code != groups.GroupMemberChangeCode_None {
			logs.Logger.Warn("create failed. code =", resPkt.Code, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
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
	resPkt.Code = groups.RemoveGroupMembers(reqPkt)
	return
}

type GetGroupsMembersHandler struct {
	CmdHandler
}

func (h *GetGroupsMembersHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetGroupsMembers
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetGroupsMembersHandler) packetIn(pkt connections.Packet) {
	var reqPkt groups.GetGroupsMembersReqPkt
	var resPkt groups.GetGroupsMembersResPkt
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
	resPkt.Members = groups.GetGroupsMembers(reqPkt.Gids)
	return
}

type SetGroupMemberHandler struct {
	CmdHandler
}

func (h *SetGroupMemberHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetGroupMember
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetGroupMemberHandler) packetIn(pkt connections.Packet) {
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt groups.GroupMember
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	groups.SetGroupMember(reqPkt)
	return
}

func NewGroupMemberHandlers(cmdHandlers *CmdHandlers) {

	dddGroupMembersHandler := &AddGroupMembersHandler{}
	dddGroupMembersHandler.initHandler(cmdHandlers)

	removeGroupMembersHandler := &RemoveGroupMembersHandler{}
	removeGroupMembersHandler.initHandler(cmdHandlers)

	getGroupsMembersHandler := &GetGroupsMembersHandler{}
	getGroupsMembersHandler.initHandler(cmdHandlers)

	setGroupMemberHandler := &SetGroupMemberHandler{}
	setGroupMemberHandler.initHandler(cmdHandlers)

}
