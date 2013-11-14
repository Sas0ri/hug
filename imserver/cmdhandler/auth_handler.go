package cmdhandler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hug/core/devices"
	"hug/core/users"
	"hug/imserver/connections"
	"hug/logs"
	"math/rand"
	"time"
)

type AuthReqPacket struct {
	User            string                `json:"u,omitempty"`
	Password        string                `json:"p,omitempty"`
	TerminalType    int16                 `json:"tt,omitempty"`
	TerminalSystem  string                `json:"ts,omitempty"`
	TerminalVersion string                `json:"tv,omitempty"`
	IosDevice       devices.IosDevice     `json:"ios,omitempty"`
	AndroidDevice   devices.AndroidDevice `json:"and,omitempty"`
}

type AuthResPacket struct {
	Code        int8       `json:"c,omitempty"`
	ServerStamp int64      `json:"ss,omitempty"`
	User        users.User `json:"usr,omitempty"`
}

type AuthHandler struct {
	CmdHandler
}

func (h *AuthHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_Auth
	cmdHandlers.handlers[h.Cmd] = h
}

func (a *AuthHandler) packetIn(pkt connections.Packet) {
	authInfo, err := Auth(pkt.Data)
	if err != nil {
		logs.Logger.Warn(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr(), " pkt:", string(pkt.Data))
	}
	var resData AuthResPacket
	if authInfo.AuthCode == users.AuthCode_None {
		resData.ServerStamp = time.Now().UnixNano() / 1000000
		usr, err := users.GetUser(authInfo.Account)
		if err != nil {
			logs.Logger.Critical(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		} else {
			resData.User = usr
			resData.User.Passwrod = ""
		}

	}
	resData.Code = authInfo.AuthCode

	wtData, err := json.Marshal(resData)
	if err != nil {
		logs.Logger.Critical(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}

	err = pkt.Conn.WritePacket(a.Cmd, connections.Pkt_Type_Response, 0, pkt.Sid, wtData)
	if err != nil {
		logs.Logger.Warn(err, " user:", pkt.Conn.AuthInfo.Account, " addr:", pkt.Conn.RemoteAddr())
		return
	}
	pkt.Conn.SetAuthResult(authInfo)
	if authInfo.AuthCode == users.AuthCode_None {
		if authInfo.IosDevice.IsValid() {
			devices.SetIosDeviceToken(authInfo.Uid, authInfo.IosDevice)
			devices.SetIosDeviceStatus(authInfo.IosDevice.Token, devices.IosDeviceStatus_Foreground)
		} else if authInfo.AndroidDevice.IsValid() {
			devices.SetAndroidDeviceAlias(authInfo.Uid, authInfo.AndroidDevice)
			devices.SetAndroidDeviceStatus(authInfo.AndroidDevice.Alias, devices.AndroidDeviceStatus_Foreground)
		}
	}
	return
}

type SignOutHandler struct {
	CmdHandler
}

func (h *SignOutHandler) initHandler(cmdHandlers *CmdHandlers) {
	h.Cmd = Cmd_SignOut
	cmdHandlers.handlers[h.Cmd] = h
}

func (h *SignOutHandler) packetIn(pkt connections.Packet) {
	//logs.Logger.Critical("Close connection account =", pkt.Conn.AuthInfo.Account)
	if pkt.Conn.AuthInfo.IosDevice.IsValid() {
		devices.SetIosDeviceToken(0, pkt.Conn.AuthInfo.IosDevice)
	} else if pkt.Conn.AuthInfo.AndroidDevice.IsValid() {
		devices.SetAndroidDeviceAlias(0, pkt.Conn.AuthInfo.AndroidDevice)
	}
	go pkt.Conn.Close()
	return
}

func NewAuthHandlers(cmdHandlers *CmdHandlers) {
	authHandler := &AuthHandler{}
	authHandler.initHandler(cmdHandlers)

	signoutHandler := &SignOutHandler{}
	signoutHandler.initHandler(cmdHandlers)
	go SendConflictNotificationLoop()
}

func SendConflictNotificationLoop() {
	for {
		conn := <-connections.ConflictConnChan
		if conn.AuthInfo.IosDevice.IsValid() {
			devices.SetIosDeviceToken(0, conn.AuthInfo.IosDevice)
		} else if conn.AuthInfo.AndroidDevice.IsValid() {
			devices.SetAndroidDeviceAlias(0, conn.AuthInfo.AndroidDevice)
		}
		var wtBytes []byte
		err := conn.WritePacket(Cmd_ConflictNotification, connections.Pkt_Type_Request, 0, uint16(rand.Intn(0xFFFF)), wtBytes)
		if err != nil {
			logs.Logger.Critical(err, "user:", conn.AuthInfo.Account, "addr:", conn.RemoteAddr())
		}
		go conn.Close()
	}
}

func Auth(authData []byte) (authInfo users.AuthInfo, authErr error) {
	//ACCOUNT := "zhongjun@jim.com"
	//PASSWORD := "test123"
	//fmt.Println("Auth string: ", authData)
	//log.Println("Auth: starting auth... auth string =", string(authData))
	authInfo.AuthCode = users.AuthCode_InvalidReq

	var reqPacket AuthReqPacket
	err := json.Unmarshal(authData, &reqPacket)
	if err != nil {
		authErr = errors.New(fmt.Sprintln("Auth data json unmarshal error", err))
		authInfo.AuthCode = users.AuthCode_InvalidReq
		return
	}
	var account, pwd string
	data, errBase64 := base64.StdEncoding.DecodeString(reqPacket.User)
	if errBase64 != nil {
		authErr = errors.New(fmt.Sprintln("User data base64 decode error:", errBase64))
		return
	} else {
		account = string(data)
	}

	data, errBase64 = base64.StdEncoding.DecodeString(reqPacket.Password)
	if errBase64 != nil {
		authErr = errors.New(fmt.Sprintln("Pwd data base64 decode error:", errBase64))
		return
	} else {
		pwd = string(data)
	}
	authInfo.AuthCode, authInfo.Uid = users.AuthUser(account, pwd)
	authInfo.Account = account
	authInfo.TerminalType = reqPacket.TerminalType
	authInfo.TerminalSystem = reqPacket.TerminalSystem
	authInfo.TerminalVersion = reqPacket.TerminalVersion
	authInfo.IosDevice = reqPacket.IosDevice
	authInfo.AndroidDevice = reqPacket.AndroidDevice
	return
}
