package cmdhandler

import (
	"encoding/json"
	"hug/core/devices"
	"hug/core/groups"
	"hug/core/messages"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils/apns"
	"hug/utils/jpush"
	"math/rand"
)

type MsgHandler struct {
	CmdHandler
}

func (h *MsgHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_Msg
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *MsgHandler) packetIn(pkt connections.Packet) {
	//log.Println("Message:  parse received msg from :", pkt.Conn.AuthInfo.Account, "msg =", string(pkt.Data))

	var reqPkt messages.Message
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	var resPkt messages.MessageResPacket
	resPkt.OriginalId = reqPkt.Id
	resPkt.Mid = 0
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
	if reqPkt.From.Id == 0 {
		reqPkt.From = reqPkt.Author
	}
	reqPkt.Id = messages.CreateMsg(reqPkt)
	resPkt.Mid = reqPkt.Id
	if reqPkt.Id == 0 {
		logs.Logger.Critical("insert message id = 0", " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	sendPkt := reqPkt
	contacts := make([]messages.MessageContact, 0, 1+len(reqPkt.Ccs))
	contacts = append(contacts, sendPkt.To)
	if len(reqPkt.Ccs) > 0 {
		contacts = append(contacts, reqPkt.Ccs...)
	}

	for _, to := range contacts {
		sendPkt.From = sendPkt.Author
		sendPkt.To = to
		messages.CreateHistory(resPkt.Mid, sendPkt.Author.Id, to, messages.HistoryStatus_Sended, messages.MessageDir_Out)
		if to.Type == messages.MCT_User {
			if to.Id <= 0 {
				logs.Logger.Warn("to id <= 0", " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
				continue
			}
			sended := h.SendMessage(to.Id, sendPkt)
			if sended {
				messages.CreateHistory(resPkt.Mid, to.Id, sendPkt.Author, messages.HistoryStatus_Sended, messages.MessageDir_In)
			} else {
				messages.CreateHistory(resPkt.Mid, to.Id, sendPkt.Author, messages.HistoryStatus_WaitToSend, messages.MessageDir_In)
			}
		} else if to.Type == messages.MCT_Group {
			members, err := groups.GetGroupMembers(to.Id)
			if err == nil {
				for _, m := range members {
					if m.Uid != pkt.Conn.AuthInfo.Uid {
						sendPkt.To.Id = m.Uid
						sendPkt.To.Type = messages.MCT_User
						sendPkt.From = to

						sended := h.SendMessage(m.Uid, sendPkt)
						if sended {
							messages.CreateHistory(resPkt.Mid, m.Uid, to, messages.HistoryStatus_Sended, messages.MessageDir_In)
						} else {
							messages.CreateHistory(resPkt.Mid, m.Uid, to, messages.HistoryStatus_WaitToSend, messages.MessageDir_In)
						}
					}
				}
			}
		}
	}
	h.SyncSendedMessage(pkt.Conn, reqPkt)
	return
}

func (h *MsgHandler) SendMessage(uid int64, pkt messages.Message) (sended bool) {
	sended = false
	wtBytes, err := json.Marshal(pkt)
	if err != nil {
		logs.Logger.Critical("send packet json marshal error =", err, "from:", uid)
		return
	}

	presence := connections.FindPresences(uid)
	if presence != nil {
		for _, conn := range presence.Terminals {
			err = conn.WritePacket(Cmd_Msg, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
			sended = true
			if err != nil {
				logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
				return
			}
		}
	}
	if messages.IsMsgPush(uid, pkt.From) {
		//logs.Logger.Infof("Push message to %d", uid)
		PushIosNotification(uid, pkt)
		PushAndroidNotification(uid, pkt)
	}
	return

}

func (m *MsgHandler) SyncSendedMessage(sendConn *connections.ClientConnection, pkt messages.Message) {
	wtBytes, err := json.Marshal(pkt)
	if err != nil {
		logs.Logger.Critical("send packet json marshal error: ", err)
		return
	}

	presence := connections.FindPresences(sendConn.AuthInfo.Uid)
	if presence != nil {
		for terminal, conn := range presence.Terminals {
			if terminal != sendConn.AuthInfo.TerminalType {
				err = conn.WritePacket(Cmd_Msg, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
				if err != nil {
					logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
					return
				}
			}
		}
	}
}

func PushIosNotification(uid int64, msg messages.Message) {
	payload := apns.Payload{}
	payload.Aps.Alert.Body = "你收到一条新的消息"
	payload.Aps.Sound = "default"
	payload.Aps.Badge = messages.GetUnreadMsgCountOfUid(uid)
	payload.SetCustom("type", MobilePushType_Msg)
	payload.SetCustom("contact", msg.From)

	notification := apns.Notification{}
	notification.Payload = &payload
	devs := devices.GetIosDevicesOfUid(uid, devices.IosDeviceStatus_Background)
	//logs.Logger.Infof("Push apn to: %v", devs)
	for _, dev := range devs {
		if dev.IsValid() {
			notification.DeviceToken = dev.Token
			if dev.IsSandBox > 0 {
				go SendSandboxApn(notification)
			} else {
				go SendApn(notification)
			}
		}

	}
}

func PushAndroidNotification(uid int64, msg messages.Message) {
	payload := jpush.Payload{}
	payload.Alert = "你收到一条新的消息"
	payload.SetCustom("type", MobilePushType_Msg)
	payload.SetCustom("contact", msg.From)

	devs := devices.GetAndroidDevicesOfUid(uid, devices.AndroidDeviceStatus_Background)
	logs.Logger.Infof("Push android notification to: %v", devs)
	for _, dev := range devs {
		go SendAnroidNotification(dev.Alias, payload)
	}
}

type GetRecentContactHandler struct {
	CmdHandler
}

func (h *GetRecentContactHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetRecentContact
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetRecentContactHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.GetRencetContactsReqPacket
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt := messages.GetRecentContacts(reqPkt)
	resData, err := json.Marshal(resPkt)
	if err != nil {
		logs.Logger.Critical("json marshal respacket error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
	}
	err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
	if err != nil {
		logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	return
}

type GetMsgHistoryHandler struct {
	CmdHandler
}

func (h *GetMsgHistoryHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetMsgHistory
	cmdHandlers.handlers[h.Cmd] = h
}

func (g *GetMsgHistoryHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.GetMsgHistoryReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}

	resPkt := messages.GetMsgHistory(reqPkt)
	wtBytes, err := json.Marshal(resPkt)
	if err != nil {
		logs.Logger.Critical("Marshal message response data error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	err = pkt.Conn.WritePacket(g.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, wtBytes)
	if err != nil {
		logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}

	return
}

type GetMsgBodysHandler struct {
	CmdHandler
}

func (h *GetMsgBodysHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetMsgBodys
	cmdHandlers.handlers[h.Cmd] = h
}

func (g *GetMsgBodysHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.GetMsgBodysReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}

	resPkt := messages.GetMsgBodys(reqPkt)
	wtBytes, err := json.Marshal(resPkt)
	if err != nil {
		logs.Logger.Critical("Marshal message response data error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	err = pkt.Conn.WritePacket(g.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, wtBytes)
	if err != nil {
		logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}

	return
}

type RemoveHistoryHandler struct {
	CmdHandler
}

func (h *RemoveHistoryHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveHistory
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveHistoryHandler) packetIn(pkt connections.Packet) {
	var reqPkt messages.RemoveHistoryReqPkt
	var resPkt messages.RemoveHistoryResPkt
	resPkt.Code = messages.RemoveHistoryCode_InvalidFormat
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
	resPkt.Code = messages.RemoveHistory(reqPkt.Uid, reqPkt.Contact, reqPkt.Mids)
	return
}

func NewMsgHandlers(cmdHandlers *CmdHandlers) {
	msgHandler := &MsgHandler{}
	msgHandler.initHandler(cmdHandlers)

	getRecentContactHandler := &GetRecentContactHandler{}
	getRecentContactHandler.initHandler(cmdHandlers)

	getMsgHistoryHandler := &GetMsgHistoryHandler{}
	getMsgHistoryHandler.initHandler(cmdHandlers)

	getMsgBodysHandler := &GetMsgBodysHandler{}
	getMsgBodysHandler.initHandler(cmdHandlers)

	removeHistoryHandler := &RemoveHistoryHandler{}
	removeHistoryHandler.initHandler(cmdHandlers)
}
