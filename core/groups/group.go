package groups

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
	"time"
)

const (
	GroupType_None int16 = iota
	GroupType_Normal
	GroupType_Super
)

const (
	CreateGroupCode_None int8 = iota
	CreateGroupCode_InvalidReq
	CreateGroupCode_DatabaseErr
	CreateGroupCode_CreateMemberErr
)

const (
	RemoveGroupCode_None int8 = iota
	RemoveGroupCode_InvalidReq
	RemoveGroupCode_GroupNotExist
	RemoveGroupCode_HasNoPermission
	RemoveGroupCode_DatabaseErr
)

const (
	SetGroupCode_None int8 = iota
	SetGroupCode_InvalidFormat
	SetGroupCode_GroupNotExist
	SetGroupCode_DatabaseErr
)

type Group struct {
	Gid         int64  `json:"gid"`
	Name        string `json:"n"`
	Type        int16  `json:"t,omitempty"`
	CreateStamp int64  `json:"ct,omitempty"`
	UpdateStamp int64  `json:"ut,omitempty"`
}

type CreateGroupReqPkt struct {
	Name       string  `json:"name"`
	MemberUids []int64 `json:"memberuids"`
	Type       int16   `json:"type,omitempty"`
	OwnerUid   int64   `json:"owner,omitempty"`
}

type CreateGroupResPkt struct {
	Code  int8  `json:"code"`
	Gid   int64 `json:"gid"`
	Stamp int64 `json:"stamp"`
}

type RemoveGroupReqPkt struct {
	Gid        int64 `json:"gid"`
	RequestUid int64 `json:"uid"`
}

type RemoveGroupResPkt struct {
	Gid  int64 `json:"gid"`
	Code int8  `json:"code"`
}

type SetGroupResPkt struct {
	Code int8 `json:"code"`
}

type GetGroupsReqPkt struct {
	Gids []int64 `json:"gids"`
}

type GetGroupsResPkt struct {
	Groups []Group `json:"gs"`
}

const (
	GroupChangedType_None int16 = iota
	GroupChangedType_Create
	GroupChangedType_Updated
	GroupChangedType_Removed
)

type GroupChangedNotification struct {
	Gid   int64 `json:"gid,omitempty"`
	Uid   int64 `json:"uid,omitempty"`
	Type  int16 `json:"t,omitempty"`
	Stamp int64 `json:"st,omitempty"`
}

type GetGroupChangedReqPkt struct {
	Stamp int64   `json:"st,omitempty"`
	Gids  []int64 `json:"gids,omitempty"`
}

type GetGroupChangedResPkt struct {
	Stamp         int64                      `json:"st,omitempty"`
	Notifications []GroupChangedNotification `json:"ns,omitempty"`
}

const createGroupChangesTableSql = `
CREATE TABLE IF NOT EXISTS groupchanges
		(
		  gid bigint NOT NULL default 0,
		  uid bigint NOT NULL default 0,
		  type smallint NOT NULL default 0,
		  stamp bigint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
		`

const createGroupsTableSql = `
	CREATE TABLE IF NOT EXISTS groups
		(
		  Gid serial NOT NULL unique,
		  Name character varying(50) NOT NULL,
		  Type smallint NOT NULL default 1,
		  UpdateStamp bigint NOT NULL default 1,
		  CreateStamp bigint NOT NULL default 1,
		  CONSTRAINT groups_pkey PRIMARY KEY (Gid)
		)
		WITH (OIDS=FALSE);
		`

func GetGroup(gid int64) (group Group, err error) {
	command := `
	SELECT Name, Type, UpdateStamp FROM groups where gid = @gid;
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
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&group.Name, &group.Type, &group.UpdateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				group.Gid = gid
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetGroups(gids []int64) (groups []Group) {
	groups = make([]Group, 0, len(gids))
	for _, gid := range gids {
		group, err := GetGroup(gid)
		if err != nil {
			logs.Logger.Critical(err)
		} else if IsGidValid(group.Gid) {
			groups = append(groups, group)
		}
	}
	return
}

func GetSameGroupUids(uid int64) (uids []int64) {
	gids := GetGidsOfUid(uid)
	uids = make([]int64, 0, 100)
	for _, gid := range gids {
		tempUids := GetGroupUids(gid)
		uids = append(uids, tempUids...)
	}
	return
}

func IsGidExist(gid int64) (exist bool, err error) {
	exist = false

	command := `
	SELECT COUNT(*) AS numgroups FROM groups where gid = @gid;
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
		} else if hasRow, _ := res.FetchNext(); hasRow {
			n := 0
			err = res.Scan(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				if n > 0 {
					exist = true
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func deleteGroup(gid int64) (err error) {
	command := `
	delete from groups where gid = @gid;
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
		_, err := conn.Execute(command, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func RemoveGroup(reqPkt RemoveGroupReqPkt) (code int8) {
	code = RemoveGroupCode_DatabaseErr
	permission := GetGroupMemberPermission(reqPkt.Gid, reqPkt.RequestUid)
	if permission != GroupMemberPermission_Admin && permission != GroupMemberPermission_Owner {
		code = RemoveGroupCode_HasNoPermission
		return
	}
	err := deleteGroup(reqPkt.Gid)
	if err != nil {
		return
	}
	//DeleteAllMemberOfGroup(reqPkt.Gid)
	code = RemoveGroupCode_None
	stamp := time.Now().UnixNano()
	var change GroupChangedNotification
	change.Gid = reqPkt.Gid
	change.Type = GroupChangedType_Removed
	CreateGroupChange(change, stamp)
	return
}

func updateGroup(gid int64, name string, groupType int16, stamp int64) (err error) {
	command := `
	update groups set Name=@name,type=@type,UpdateStamp=@updatestamp where gid=@gid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(gid)
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
	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(groupType)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, nameParam, typeParam, updatestampParam, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetGroup(group Group) (code int8) {
	code = SetGroupCode_DatabaseErr
	exist, err := IsGidExist(group.Gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		logs.Logger.Critical("group not exist")
		code = SetGroupCode_GroupNotExist
		return
	}
	stamp := time.Now().UnixNano()
	err = updateGroup(group.Gid, group.Name, group.Type, stamp)
	if err != nil {
		return
	}
	var change GroupChangedNotification
	change.Gid = group.Gid
	change.Type = GroupChangedType_Updated
	CreateGroupChange(change, stamp)
	code = SetGroupCode_None
	return
}

func insertGroup(name string, groupType int16, stamp int64) (gid int64) {
	command := `
	INSERT INTO groups(name,type,CreateStamp,updatestamp) 
		VALUES(@name,@type,@createstamp,@updatestamp) RETURNING gid;
	`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err := nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(groupType)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	createstampParam := pgsql.NewParameter("@createstamp", pgsql.Bigint)
	err = createstampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, nameParam, typeParam, createstampParam, updatestampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			_, err = res.ScanNext(&gid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateGroup(reqPkt CreateGroupReqPkt) (resPkt CreateGroupResPkt) {
	if len(reqPkt.Name) <= 0 {
		resPkt.Code = CreateGroupCode_InvalidReq
		return
	}
	if reqPkt.OwnerUid <= 0 {
		resPkt.Code = CreateGroupCode_InvalidReq
		return
	}
	resPkt.Code = CreateGroupCode_DatabaseErr
	stamp := time.Now().UnixNano()
	resPkt.Gid = insertGroup(reqPkt.Name, reqPkt.Type, stamp)
	if resPkt.Gid == 0 {
		return
	}
	err := CreateGroupMember(resPkt.Gid, reqPkt.OwnerUid, "", GroupMemberPermission_Owner, false)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	for _, uid := range reqPkt.MemberUids {
		if uid == reqPkt.OwnerUid {
			continue
		}
		err = CreateGroupMember(resPkt.Gid, uid, "", GroupMemberPermission_Normal, false)
		if err != nil {
			logs.Logger.Critical(err)
		}
	}
	resPkt.Code = CreateGroupCode_None
	var change GroupChangedNotification
	change.Gid = resPkt.Gid
	change.Type = GroupChangedType_Create
	CreateGroupChange(change, stamp)
	resPkt.Stamp = stamp

	SetGroupUpdateStamp(resPkt.Gid, stamp)
	return
}

func GetGroupUpdateStamp(gid int64) (stamp int64) {
	command := `
	SELECT updatestamp from groups where gid = @gid;
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
			_, err = res.ScanNext(&stamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)

	return
}

func SetGroupUpdateStamp(gid, stamp int64) {
	command := `
	update groups set updatestamp = @updatestamp where gid = @gid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err := gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, updatestampParam, gidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func GetGroupChanges(gid, stamp int64) (changes []GroupChangedNotification, lastStamp int64) {
	command := `
	SELECT uid, type, stamp from groupchanges where gid = @gid AND stamp > @stamp ORDER BY stamp ASC;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err := gidParam.SetValue(gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, gidParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			changes = make([]GroupChangedNotification, 0, 5)
			lastStamp = 0
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var change GroupChangedNotification
					change.Gid = gid
					var tempStamp int64
					err = res.Scan(&change.Uid, &change.Type, &tempStamp)
					if err != nil {
						logs.Logger.Critical("database error =", err)
					} else {
						if tempStamp > lastStamp {
							lastStamp = tempStamp
						}
						changes = append(changes, change)
					}

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

func IsGroupChangeExist(change GroupChangedNotification) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS num FROM groupchanges WHERE gid = @gid AND uid = @uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(change.Gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
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
		} else if hasRow, _ := res.FetchNext(); hasRow {
			n := 0
			err = res.Scan(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				if n > 0 {
					exist = true
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func insertGroupChange(change GroupChangedNotification, stamp int64) (err error) {
	command := `
	INSERT INTO groupchanges(gid,uid,type,stamp) 
			VALUES(@gid,@uid,@type,@stamp);
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(change.Gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(change.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, gidParam, uidParam, typeParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateGroupChange(change GroupChangedNotification, stamp int64) (err error) {
	command := `
	update groupchanges set type=@type, stamp=@stamp where gid = @gid AND uid = @uid;
	`
	gidParam := pgsql.NewParameter("@gid", pgsql.Bigint)
	err = gidParam.SetValue(change.Gid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(change.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, typeParam, stampParam, gidParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func CreateGroupChange(change GroupChangedNotification, stamp int64) {
	if !IsGidValid(change.Gid) {
		logs.Logger.Critical("gid is invalid")
		return
	}
	exist, err := IsGroupChangeExist(change)
	if err != nil {
		return
	}
	if exist {
		err = updateGroupChange(change, stamp)
	} else {
		err = insertGroupChange(change, stamp)
	}

	if err == nil {
		change.Stamp = stamp
		GroupChangedNotificationChan <- change
		logs.Logger.Info("create group change successful: gid = ", change.Gid, " uid = ", change.Uid, " type = ", change.Type)

	} else {
		logs.Logger.Critical("create group change failed error: ", err, " gid = ", change.Gid, " uid = ", change.Uid, " type = ", change.Type)
	}

}

func IsGidValid(gid int64) (valid bool) {
	if gid <= 0 {
		return false
	}
	return true
}
