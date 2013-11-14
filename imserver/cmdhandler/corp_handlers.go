package cmdhandler

import (
	"encoding/json"
	"hug/core/corps"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
)

type CreateCorpHandler struct {
	CmdHandler
}

func (h *CreateCorpHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_CreateCorp
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *CreateCorpHandler) packetIn(pkt connections.Packet) {
	var reqPkt corps.CreateCorpReqPkt
	var resPkt corps.CreateCorpResPkt
	resPkt.Code = corps.CreateCorpCode_InvalidReq
	resPkt.Cid = 0
	defer func() {
		if resPkt.Code != corps.CreateCorpCode_None {
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
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Cid, resPkt.Code = corps.CreateCorp(reqPkt)
	if resPkt.Code == corps.CreateCorpCode_None {
		var worker corps.CreateWorkerReqPkt
		worker.Name = reqPkt.OwnerName
		worker.Cid = resPkt.Cid
		worker.Post = reqPkt.OwnerPost
		worker.Uid = reqPkt.OwnerUid
		worker.Permission = corps.WorkerPermission_CorpOwner
		_, createWorkerCode := corps.CreateWorker(worker)
		if createWorkerCode != corps.CreateWorkerCode_None {
			resPkt.Code = corps.CreateCorpCode_CreateWorkerErr
			logs.Logger.Warn("create owner worker failed", " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
		// var change corps.CorpChangedNotification
		// change.Cid = resPkt.Cid
		// change.Type = corps.CorpChangedType_Create
		// corps.CreateCorpChange(change)
		// change.Cid = resPkt.Cid
		// change.Wid = wid
		// change.Type = corps.CorpChangedType_Create
		// corps.CreateCorpChange(change)
	}
	return
}

type GetCorpHandler struct {
	CmdHandler
}

func (h *GetCorpHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetCorp
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetCorpHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.Corp
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
	var reqPkt corps.GetCorpReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt, err = corps.GetCorp(reqPkt.Cid)
	return
}

type SetCorpHandler struct {
	CmdHandler
}

func (h *SetCorpHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetCorp
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetCorpHandler) packetIn(pkt connections.Packet) {
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt corps.Corp
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	corps.SetCorp(reqPkt)
	return
}

type RemoveCorpHandler struct {
	CmdHandler
}

func (h *RemoveCorpHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveCorp
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveCorpHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.RemoveCorpResPkt
	resPkt.Code = corps.RemoveCorpCode_InvalidReq
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
	var reqPkt corps.RemoveCorpReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), "reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Code = corps.RemoveCorp(reqPkt)
	return
}

// type GetCorpsOfUserHandler struct {
// 	CmdHandler
// }

// func (h *GetCorpsOfUserHandler) initHandler(cmdHandlers *CmdHandlers) {
// 	h.Cmd = Cmd_GetCorpsOfUser
// 	cmdHandlers.handlers[h.Cmd] = h
// }

// func (h *GetCorpsOfUserHandler) packetIn(pkt connections.Packet) {
// 	var resPkt corps.GetCorpsOfUserResPkt
// 	defer func() {
// 		resData, err := json.Marshal(resPkt)
// 		if err != nil {
// 			log.Fatal("GetCorpsOfUserHandler json marshal respacket error:", err)
// 		}
// 		err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, resData)
// 		if err != nil {
// 			log.Println("GetCorpsOfUserHandler: Conn write response packet error =", err)
// 			return
// 		}
// 	}()
// 	var reqPkt corps.GetCorpsOfUserReqPkt
// 	err := json.Unmarshal(pkt.Data, &reqPkt)
// 	if err != nil {
// 		log.Println("GetCorpsOfUserHandler error: json unmarshal error:", err)
// 		return
// 	}
// 	resPkt.Corps, resPkt.Workers, resPkt.Depts, err = corps.GetCorpsOfUser(reqPkt.Uid)
// 	if err != nil {
// 		log.Println("GetCorpsOfUserHandler error:", err)
// 	}
// 	return
// }

type GetCorpTreesHandler struct {
	CmdHandler
}

func (h *GetCorpTreesHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetCorpTrees
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetCorpTreesHandler) packetIn(pkt connections.Packet) {
	//log.Println("Start GetCorpTreesHandler...")
	var resPkt corps.GetCorpTreesResPkt
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
	var reqPkt corps.GetCorpTreesReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	//log.Println("Before corps.GetCorpTreesOfCids(...")
	resPkt.Workers, resPkt.Depts, err = corps.GetCorpTreesOfCids(reqPkt.Cids)
	if err != nil {
		logs.Logger.Critical(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
	}
	//log.Println("After corps.GetCorpTreesOfCids(...")
	return
}

type GetCidsHandler struct {
	CmdHandler
}

func (h *GetCidsHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetCids
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetCidsHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.GetCidsResPkt
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
	var reqPkt corps.GetCidsReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Cids, err = corps.GetCidsOfUid(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
	}
	return
}

type GetCorpChangedHandler struct {
	CmdHandler
}

func (h *GetCorpChangedHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetCorpChanged
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetCorpChangedHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.GetCorpChangedResPkt
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
	var reqPkt corps.GetCorpChangedReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Stamp = 0
	resPkt.Notifications = make([]corps.CorpChangedNotification, 0, 5)
	for _, cid := range reqPkt.Cids {
		changes, tempStamp := corps.GetCorpChanges(cid, reqPkt.Stamp)
		resPkt.Notifications = append(resPkt.Notifications, changes...)
		if tempStamp > resPkt.Stamp {
			resPkt.Stamp = tempStamp
		}
	}
	return
}

func SendCorpChangedNotificationLoop() {
	var reqPkt corps.CorpChangedNotification
	for {
		reqPkt = <-corps.CorpChangedNotificationChan
		wtBytes, err := json.Marshal(reqPkt)
		if err != nil {
			logs.Logger.Critical("json marshal respacket error:", err)
		}
		uids, err := corps.GetUidsOfCorp(reqPkt.Cid)
		if err != nil {
			logs.Logger.Critical(err)
		}
		for _, uid := range uids {
			presence := connections.FindPresences(uid)
			if presence != nil {
				for _, conn := range presence.Terminals {
					err = conn.WritePacket(Cmd_CorpChangedNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
					if err != nil {
						logs.Logger.Warn("Conn write response packet error =", err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
						return
					}
				}
			}
		}
	}
}

func NewCorpHandlers(cmdHandlers *CmdHandlers) {
	getCidsHandler := &GetCidsHandler{}
	getCidsHandler.initHandler(cmdHandlers)

	// getCorpsOfUserHandler := &GetCorpsOfUserHandler{}
	// getCorpsOfUserHandler.initHandler(cmdHandlers)

	getCorpTreesHandler := &GetCorpTreesHandler{}
	getCorpTreesHandler.initHandler(cmdHandlers)

	createCorpHandler := &CreateCorpHandler{}
	createCorpHandler.initHandler(cmdHandlers)

	getCorpHandler := &GetCorpHandler{}
	getCorpHandler.initHandler(cmdHandlers)

	setCorpHandler := &SetCorpHandler{}
	setCorpHandler.initHandler(cmdHandlers)

	removeCorpHandler := &RemoveCorpHandler{}
	removeCorpHandler.initHandler(cmdHandlers)

	getCorpChangedHandler := &GetCorpChangedHandler{}
	getCorpChangedHandler.initHandler(cmdHandlers)

	go SendCorpChangedNotificationLoop()
}
