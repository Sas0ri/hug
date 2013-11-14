package cmdhandler

import (
	"fmt"
	"hug/core/corps"
	"hug/core/groups"
	"hug/core/users"
	"hug/imserver/connections"
	"hug/logs"
	"hug/utils"
)

const (
	Cmd_None uint8 = iota
	Cmd_Auth
	Cmd_ConflictNotification
	Cmd_SignOut
)
const (
	Cmd_Msg uint8 = 0x10 + iota
	Cmd_GetMsg
	Cmd_GetRecentContact
	Cmd_GetMsgHistory
	Cmd_GetMsgBodys
	Cmd_RemoveHistory
)

const (
	Cmd_GetUserInfos uint8 = 0x20 + iota
	Cmd_SetUserInfo
	Cmd_GetUserInfoChanged
	Cmd_UserInfoChangedNotification
)
const (
	Cmd_GetCids uint8 = 0x30 + iota
	Cmd_GetCorpTrees
	Cmd_CreateCorp
	Cmd_GetCorp
	Cmd_SetCorp
	Cmd_RemoveCorp
	Cmd_CreateDept
	Cmd_GetDept
	Cmd_SetDept
	Cmd_RemoveDept
	Cmd_CreateWorker
	Cmd_GetWorker
	Cmd_SetWorker
	Cmd_BindWokerUser
	Cmd_RemoveWorker
	Cmd_GetCorpChanged
	Cmd_CorpChangedNotification
)
const (
	Cmd_CreateGroup uint8 = 0x50 + iota
	Cmd_RemoveGroup
	Cmd_SetGroup
	Cmd_GetGroups
	Cmd_GetGids
	Cmd_AddGroupMembers
	Cmd_RemoveGroupMembers
	Cmd_SetGroupMember
	Cmd_GetGroupsMembers
	Cmd_GroupChangedNotification
	Cmd_GetGroupChanged
)
const (
	Cmd_SetMsgPush uint8 = 0x70 + iota
	Cmd_GetMsgPush
	Cmd_SetIosDeviceStatus
	Cmd_SetAndroidDeviceStatus
)

const (
	Cmd_GetAllRoster uint8 = 0x80 + iota
	Cmd_GetRosters
	Cmd_SetRoster
	Cmd_RemoveRoster
	Cmd_RosterChangedNotification
	Cmd_GetRosterChanged
	Cmd_RosterRequest
	Cmd_GetRosterRequests
	Cmd_HandleRosterRequest
	Cmd_SetIgnoreRosterRequest
	Cmd_HandleRosterRequestNotification
)

const (
	Cmd_FileTransferRequest uint8 = 0xA0 + iota
	Cmd_ChangeToOfflineFileTransfer
	Cmd_OffLineFileTransferSended
	Cmd_HandleOfflineFileTransfer
	Cmd_HandleDirectFileTransferRequest
	Cmd_FileTransferDirectLocalConnectFailed
	Cmd_FileTransferShakeHands
	Cmd_FileTransferRegistNat
	Cmd_FileTransferStartNat
	Cmd_FileTransferStartSend
	Cmd_FileTransferSending
	Cmd_FileTransferSendDone
	Cmd_FileTransferProxyTransfer
)

type IHandler interface {
	packetIn(pkt connections.Packet)
	initHandler(cmdHandlers *CmdHandlers)
}

type CmdHandler struct {
	Cmd uint8
}

type CmdHandlers struct {
	PacketQueue chan connections.Packet
	handlers    map[uint8](IHandler)
}

func NewCmdHanglers() (cmdHandlers *CmdHandlers) {
	cmdHandlers = &CmdHandlers{
		PacketQueue: make(chan connections.Packet, 512),
		handlers:    make(map[uint8](IHandler)),
	}

	NewMsgHandlers(cmdHandlers)
	NewAuthHandlers(cmdHandlers)
	NewCorpHandlers(cmdHandlers)
	NewDeptHandlers(cmdHandlers)
	NewWorkerHandlers(cmdHandlers)
	NewUserInfoHandlers(cmdHandlers)
	NewGroupHandlers(cmdHandlers)
	NewGroupMemberHandlers(cmdHandlers)
	NewIosDeviceHandlers(cmdHandlers)
	NewAndroidDeviceHandlers(cmdHandlers)
	NewMsgPushHandlers(cmdHandlers)
	NewRosterHandlers(cmdHandlers)
	NewRosterRequestHandlers(cmdHandlers)

	go cmdHandlers.handleLoop()
	return
}

func (cmdHandlers *CmdHandlers) handleLoop() {
	for {
		select {
		case packet := <-cmdHandlers.PacketQueue:
			if packet.Conn.AuthInfo.AuthCode == users.AuthCode_None || packet.Cmd == Cmd_Auth {
				hander, ok := cmdHandlers.handlers[packet.Cmd]
				if !ok {
					logs.Logger.Warn("Invalid cmd: ", fmt.Sprintf("0x%02x", packet.Cmd), " user:", packet.Conn.AuthInfo.Account, " addr:", packet.Conn.RemoteAddr())
				} else {
					//log.Println("Handle cmd:", packet.Cmd)
					go hander.packetIn(packet)
				}
			} else {
				if packet.Conn.AuthInfo.AuthCode == users.AuthCode_WaitAuth {
					logs.Logger.Warn("not accept cmd: ", fmt.Sprintf("0x%02x", packet.Cmd), " before authed", " addr:", packet.Conn.RemoteAddr())
					go packet.Conn.Close()
				}
			}

		}

	}

}

func GetRelationUids(uid int64) (uids []int64) {
	uids, err := corps.GetColleaguesOfUid(uid)
	if err != nil {
		logs.Logger.Critical("GetRelationUids error:", err)
	}
	groupUids := groups.GetSameGroupUids(uid)
	uids = append(uids, groupUids...)
	uids = utils.RemoveIntSliceDuplicate(uids)
	return
}
