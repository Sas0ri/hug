package groups

import (
	"errors"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/core/messages"
	"hug/core/users"
	"hug/logs"
	"time"
)

const (
	GroupMemberStatus_None int16 = iota
	GroupMemberStatus_Active
	GroupMemberStatus_Frozen
)

const (
	GroupMemberPermission_None int16 = iota
	GroupMemberPermission_Normal
	GroupMemberPermission_Admin
	GroupMemberPermission_Owner
)

type GroupMember struct {
	Gid        int64  `json:"gid"`
	Uid        int64  `json:"uid"`
	Name       string `json:"n"`
	Permission int16  `json:"p,omitempty"`
}

const (
	GroupMemberChangeCode_None int8 = iota
	GroupMemberChangeCode_InvalidReq
	GroupMemberChangeCode_DatabaseErr
)

type GroupMembersChangeReqPkt struct {
	Gid     int64         `json:"gid"`
	Members []GroupMember `json:"ms"`
}

type GroupMembersChangeResPkt struct {
	Code int8 `json:"code"`
}

type GetGidsReqPkt struct {
	Uid int64 `json:"uid,omitempty"`
}

type GetGidsResPkt struct {
	Gids []int64 `json:"gids,omitempty"`
}

type GetGroupsMembersReqPkt struct {
	Gids []int64 `json:"gids,omitempty"`
}

type GetGroupsMembersResPkt struct {
	Members []GroupMember `json:"ms,omitempty"`
}

const createUsersTableSql = `
	CREATE TABLE IF NOT EXISTS groupmembers
		(
		  Gid bigint NOT NULL,
		  Uid bigint NOT NULL,
		  Name character varying(50) NOT NULL default '',
		  Permission smallint NOT NULL default 1
		)
		WITH (OIDS=FALSE);
		`

// func GetGroupsOfUser(reqPkt GetGroupsOfUserReqPkt) (resPkt GetGroupsOfUserResPkt) {
// 	queryStr := fmt.Sprintf("SELECT Gid FROM groupmembers where uid = %d", reqPkt.Uid)
// 	rows, err := db.Query(queryStr)
// 	if err != nil {
// 		logs.Logger.Critical("GetGroupsOfUser error: database select error =", err)
// 		return
// 	}
// 	resPkt.Groups = make([]Group, 0, 10)
// 	resPkt.Members = make([]GroupMembers, 0, 10)
// 	for rows.Next() {
// 		var gid int64
// 		err = rows.Scan(&gid)
// 		if err != nil {
// 			logs.Logger.Critical("GetGroupsOfUser error:", err)
// 			continue
// 		}
// 		group, err := GetGroup(gid)
// 		if err != nil {
// 			logs.Logger.Critical("GetGroupsOfUser error:", err)
// 		} else {
// 			resPkt.Groups = append(resPkt.Groups, group)
// 			resPkt.Members = append(resPkt.Members, GetGroupMembers(group.Gid))
// 		}
// 	}
// 	return
// }

func GetGroupMemberPermission(gid, uid int64) (permission int16) {
	permission = GroupMemberPermission_None
	command := `
	SELECT Permission FROM groupmembers where gid = @gid AND uid = @uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err := gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, gidParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			_, err = res.ScanNext(&permission)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetGroupMembers(gid int64) (members []GroupMember, err error) {
	command := `
	SELECT Uid,Name,Permission FROM groupmembers where gid = @gid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			members = make([]GroupMember, 0, 50)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var m GroupMember
					err = res.Scan(&m.Uid, &m.Name, &m.Permission)
					if err != nil {
						logs.Logger.Critical(err)
						continue
					}
					m.Gid = gid
					members = append(members, m)

				} else {
					break
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)

	return
}

func GetGroupsMembers(gids []int64) (members []GroupMember) {
	members = make([]GroupMember, 0, 100)
	for _, gid := range gids {
		tempMembers, err := GetGroupMembers(gid)
		if err == nil {
			members = append(members, tempMembers...)
		}
	}
	return
}

func updateGroupMember(gid, uid int64, name string, permission int16) (err error) {
	command := `
	update groupmembers set Name=@name, Permission=@permission where gid=@gid AND uid=@uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	permissionParam := pgsql.NewParameter("@permission", pgsql.Smallint)
	err = permissionParam.SetValue(permission)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, nameParam, permissionParam, gidParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetGroupMember(m GroupMember) {
	err := updateGroupMember(m.Gid, m.Uid, m.Name, m.Permission)
	if err != nil {
		return
	}
	stamp := time.Now().UnixNano()
	SetGroupUpdateStamp(m.Gid, stamp)
	var change GroupChangedNotification
	change.Gid = m.Gid
	change.Uid = m.Uid
	change.Type = GroupChangedType_Updated
	CreateGroupChange(change, stamp)
}

func GetGroupUids(gid int64) (uids []int64) {
	command := `
	SELECT Uid FROM groupmembers where gid = @gid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err := gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			uids = make([]int64, 0, 100)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var uid int64
					err = res.Scan(&uid)
					if err != nil {
						logs.Logger.Critical(err)
						continue
					}
					uids = append(uids, uid)

				} else {
					break
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetGidsOfUid(uid int64) (gids []int64) {
	command := `
	SELECT gid FROM groupmembers where uid = @uid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			gids = make([]int64, 0, 20)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var gid int64
					err = res.Scan(&gid)
					if err != nil {
						logs.Logger.Critical(err)
						continue
					}
					gids = append(gids, gid)

				} else {
					break
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsMemberInGroup(gid, uid int64) (in bool, err error) {
	in = false
	n := 0
	command := `
	SELECT COUNT(*) AS nummembers FROM groupmembers where gid = @gid AND uid = @uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, gidParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			_, err = res.ScanNext(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else if n > 0 {
				in = true
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateGroupMember(gid, uid int64, name string, permission int16, pushnotification bool) (err error) {
	if !users.IsUidValid(uid) {
		err = errors.New("uid is invalid")
		return
	}
	exist, err := IsGidExist(gid)
	if err != nil {
		err = errors.New(fmt.Sprintln("check if gid eixst error =", err))
		return
	}
	if !exist {
		err = errors.New(fmt.Sprintln("gid =", gid, "is not exist"))
		return
	}
	areadyIn, err := IsMemberInGroup(gid, uid)
	if err != nil {
		err = errors.New(fmt.Sprintln("check if member aready in group error =", err))
		return
	}
	if areadyIn {
		err = errors.New(fmt.Sprintln("member aready in group uid =", uid, "gid =", gid))
		return
	}
	command := `
	INSERT INTO groupmembers(gid, uid, name,Permission) 
		VALUES(@gid,@uid,@name,@permission);
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	permissionParam := pgsql.NewParameter("@permission", pgsql.Smallint)
	err = permissionParam.SetValue(permission)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, gidParam, uidParam, nameParam, permissionParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			pool.Release(conn)
			if pushnotification {
				stamp := time.Now().UnixNano()
				SetGroupUpdateStamp(gid, stamp)
				var change GroupChangedNotification
				change.Gid = gid
				change.Uid = uid
				change.Type = GroupChangedType_Create
				CreateGroupChange(change, stamp)
			}
		}
	}
	return
}

func deleteMemberFromGroup(gid, uid int64, pushChange bool) (err error) {
	in, err := IsMemberInGroup(gid, uid)
	if err != nil || !in {
		return
	}
	command := `
	delete from groupmembers where gid = @gid AND uid = @uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, gidParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			if pushChange {
				stamp := time.Now().UnixNano()
				SetGroupUpdateStamp(gid, stamp)
				var change GroupChangedNotification
				change.Gid = gid
				change.Uid = uid
				change.Type = GroupChangedType_Removed
				CreateGroupChange(change, stamp)
			}

			var contact messages.MessageContact
			contact.Id = gid
			contact.Type = messages.MCT_Group
			messages.RemoveRecent(uid, contact)
		}
	}
	pool.Release(conn)
	return
}

func DeleteMemberFromGroup(gid, uid int64) (err error) {
	return deleteMemberFromGroup(gid, uid, true)
}

func DeleteAllMemberOfGroup(gid int64) {
	command := `
	delete from groupmembers where gid = @gid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err := gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			var contact messages.MessageContact
			contact.Id = gid
			contact.Type = messages.MCT_Group
			messages.RemoveContactRecent(contact)
		}
	}
	pool.Release(conn)
	return
}

func AddGroupMembers(reqPkt GroupMembersChangeReqPkt) (code int8) {
	code = GroupMemberChangeCode_DatabaseErr
	for _, m := range reqPkt.Members {
		err := CreateGroupMember(reqPkt.Gid, m.Uid, m.Name, m.Permission, true)
		if err != nil {
			logs.Logger.Critical(err)
			continue
		}
	}
	code = GroupMemberChangeCode_None
	return
}

func RemoveGroupMembers(reqPkt GroupMembersChangeReqPkt) (code int8) {
	code = GroupMemberChangeCode_DatabaseErr
	for _, m := range reqPkt.Members {
		err := DeleteMemberFromGroup(reqPkt.Gid, m.Uid)
		if err != nil {
			logs.Logger.Critical(err)
			continue
		}
	}
	code = GroupMemberChangeCode_None
	return
}
