package cmdhandler

import (
	"encoding/json"
	"hug/core/corps"
	"hug/imserver/connections"
	"hug/logs"
)

type CreateWorkerHandler struct {
	CmdHandler
}

func (h *CreateWorkerHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_CreateWorker
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *CreateWorkerHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.CreateWorkerResPkt
	resPkt.Code = corps.CreateWorkerCode_InvalidReq
	resPkt.Wid = 0
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
	var reqPkt corps.CreateWorkerReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Wid, resPkt.Code = corps.CreateWorker(reqPkt)
	return
}

// type ChangeWorkerDeptHandler struct {
// 	CmdHandler
// }

// func (h *ChangeWorkerDeptHandler) initHandler(cmdHandlers *CmdHandlers) {
// 	h.Cmd = Cmd_ChangeWorkerDept
// 	cmdHandlers.handlers[h.Cmd] = h
// }

// func (h *ChangeWorkerDeptHandler) packetIn(pkt connections.Packet) {
// 	var resPkt corps.ChangeWorkerDeptResPkt
// 	resPkt.Code = corps.ChangeWorkerDeptCode_InvalidReq
// 	defer func() {
// 		resData, err := json.Marshal(resPkt)
// 		if err != nil {
// 			log.Fatal("ChangeWorkerDeptHandler json marshal respacket error:", err)
// 		}
// 		err = pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, resData)
// 		if err != nil {
// 			log.Println("ChangeWorkerDeptHandler: Conn write response packet error =", err)
// 			return
// 		}
// 	}()
// 	var reqPkt corps.ChangeWorkerDeptReqPkt
// 	err := json.Unmarshal(pkt.Data, &reqPkt)
// 	if err != nil {
// 		log.Println("ChangeWorkerDeptHandler error: json unmarshal error:", err)
// 		return
// 	}
// 	resPkt.Code = corps.ChangeWorkerDept(reqPkt)
// 	return
// }

type RemoveWorkerHandler struct {
	CmdHandler
}

func (h *RemoveWorkerHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_RemoveWorker
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *RemoveWorkerHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.RemoveWorkerResPkt
	resPkt.Code = corps.RemoveWorkerCode_InvalidReq
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
	var reqPkt corps.RemoveWorkerReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Code = corps.RemoveWorker(reqPkt)
	return
}

type BindWorkerUserHandler struct {
	CmdHandler
}

func (h *BindWorkerUserHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_BindWokerUser
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *BindWorkerUserHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.BindWorkerUserResPkt
	resPkt.Code = corps.BindWorkerUserCode_InvalidReq
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
	var reqPkt corps.BindWorkerUserReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt.Code = corps.BindWorkerUser(reqPkt)
	return
}

type GetWorkerHandler struct {
	CmdHandler
}

func (h *GetWorkerHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_GetWorker
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *GetWorkerHandler) packetIn(pkt connections.Packet) {
	var resPkt corps.Worker
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
	var reqPkt corps.GetWorkerReqPkt
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	resPkt, err = corps.GetWorker(reqPkt.Wid)
	return
}

type SetWorkerHandler struct {
	CmdHandler
}

func (h *SetWorkerHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetWorker
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetWorkerHandler) packetIn(pkt connections.Packet) {
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn("Conn write response packet error =", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	var reqPkt corps.Worker
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn("json unmarshal error:", err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " reqpkt :", string(pkt.Data))
		return
	}
	corps.SetWorker(reqPkt)
	return
}

func NewWorkerHandlers(cmdHandlers *CmdHandlers) {
	createWorkerHandler := &CreateWorkerHandler{}
	createWorkerHandler.initHandler(cmdHandlers)

	bindWorkerUserHandler := &BindWorkerUserHandler{}
	bindWorkerUserHandler.initHandler(cmdHandlers)

	removeWorkerHandler := &RemoveWorkerHandler{}
	removeWorkerHandler.initHandler(cmdHandlers)

	setWorkerHandler := &SetWorkerHandler{}
	setWorkerHandler.initHandler(cmdHandlers)

	getWorkerHandler := &GetWorkerHandler{}
	getWorkerHandler.initHandler(cmdHandlers)

}
