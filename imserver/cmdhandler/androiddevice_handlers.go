package cmdhandler

import (
	"encoding/json"
	"hug/core/devices"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils/jpush"
)

type SetAndroidDeviceStatusHandler struct {
	CmdHandler
}

func (h *SetAndroidDeviceStatusHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SetAndroidDeviceStatus
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SetAndroidDeviceStatusHandler) packetIn(pkt connections.Packet) {
	var reqPkt devices.AndroidDeviceStatusPkt
	defer func() {
		var resData []byte
		err := pkt.Conn.WritePacket(h.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, resData)
		if err != nil {
			logs.Logger.Warn(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
			return
		}
	}()
	err := json.Unmarshal(pkt.Data, &reqPkt)
	if err != nil {
		logs.Logger.Warn(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " data:", string(pkt.Data))
		return
	}
	if pkt.Conn.AuthInfo.AndroidDevice.IsValid() {
		devices.SetAndroidDeviceStatus(pkt.Conn.AuthInfo.AndroidDevice.Alias, reqPkt.Status)
	}
	return
}

func NewAndroidDeviceHandlers(cmdHandlers *CmdHandlers) {
	setAndroidDeviceStatusHandler := &SetAndroidDeviceStatusHandler{}
	setAndroidDeviceStatusHandler.initHandler(cmdHandlers)

	startJPush()
}

const (
	app_key      = "54222ffe37348c561292192d"
	masterSecret = "7f2f62f0b5be856bfe08447e"
)

var androidJPush *jpush.JPush

func startJPush() {
	androidJPush = jpush.New(app_key, masterSecret)
}

func SendAnroidNotification(alias string, payload jpush.Payload) {
	err := androidJPush.Send(alias, payload)
	if err != nil {
		logs.Logger.Critical("Send android notification error:", err)
	}
}
