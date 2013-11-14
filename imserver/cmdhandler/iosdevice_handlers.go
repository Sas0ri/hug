package cmdhandler

import (
	"encoding/json"
	"hug/core/devices"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils"
	"hug/utils/apns"
	"time"
)

const (
	MobilePushType_None int = iota
	MobilePushType_Msg
)

const (
	apn_cert_filename         = "cert.pem"
	apn_key_filename          = "key.unencrypted.pem"
	apn_sandbox_cert_filename = "sandbox_cert.pem"
	apn_sandbox_key_filename  = "sandbox_key.unencrypted.pem"
)

type SetIosDeviceStatusHandler struct {
	CmdHandler
}

func (h *SetIosDeviceStatusHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetIosDeviceStatus
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetIosDeviceStatusHandler) packetIn(pkt connections.Packet) {
	var reqPkt devices.IosDeviceStatusPkt
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
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
	if pkt.Conn.AuthInfo.IosDevice.IsValid() {
		devices.SetIosDeviceStatus(pkt.Conn.AuthInfo.IosDevice.Token, reqPkt.Status)
	}
	return
}

func NewIosDeviceHandlers(cmdHandlers *CmdHandlers) {
	setIosDeviceStatusHandler := &SetIosDeviceStatusHandler{}
	setIosDeviceStatusHandler.initHandler(cmdHandlers)

	startApn()
}

var apn *apns.Apn
var sandboxApn *apns.Apn

func startApn() {
	path := utils.ApplicationPath()
	var err error
	apn, err = apns.New(path+"/"+apn_cert_filename, path+"/"+apn_key_filename, "gateway.push.apple.com:2195", 10*time.Minute)
	if err != nil {
		logs.Logger.Critical("connect apns error:", err.Error())
		//log.Println("connect apns error:", err.Error())
		//os.Exit(1)
	} else {
		//go readApnError(apn.ErrorChan)
	}

	sandboxApn, err = apns.New(path+"/"+apn_sandbox_cert_filename, path+"/"+apn_sandbox_key_filename, "gateway.sandbox.push.apple.com:2195", 10*time.Minute)
	if err != nil {
		logs.Logger.Critical("connect snadbox apns error:", err.Error())
		//log.Println("connect apns error:", err.Error())
		//os.Exit(1)
	} else {
		//go readApnError(sandboxApn.ErrorChan)
	}
}

func SendApn(notification apns.Notification) {
	if apn != nil {
		logs.Logger.Infof("Send apn: device token = %s, payload = %v", notification.DeviceToken, notification.Payload.Aps)
		err := apn.Send(&notification)
		if err != nil {
			logs.Logger.Critical("Send apn error:", err)
		}
	}

}

func SendSandboxApn(notification apns.Notification) {
	if sandboxApn != nil {
		logs.Logger.Infof("Send sendbox apn: device token = %s, payload = %v", notification.DeviceToken, notification.Payload.Aps)
		err := sandboxApn.Send(&notification)
		if err != nil {
			logs.Logger.Critical("Send sandbox apn error:", err)
		}
	}

}

func readApnError(errorChan <-chan error) {
	for {
		apnerror := <-errorChan
		logs.Logger.Critical("apn error:", apnerror.Error())
	}
}
