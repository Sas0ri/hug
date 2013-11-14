package rosters

import (
	"errors"
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
	"time"
)

type Roster struct {
	Uid    int64  `json:"uid"`
	Ruid   int64  `json:"ruid"`
	Remark string `json:"r, omitempty"`
	Group  string `json:"g, omitempty"`
	Stamp  int64  `json:"st, omitempty"`
}

const createRosterTableSql = `
CREATE TABLE IF NOT EXISTS roster
		(
		  uid bigint NOT NULL,
		  ruid bigint NOT NULL,
		  remark character varying(50) default '',
		  groupname  character varying(50) default '',
		  stamp bigint NOT NULL
		)
		WITH (OIDS=FALSE);
`

const (
	RosterChangeType_None int16 = iota
	RosterChangeType_Added
	RosterChangeType_Updated
	RosterChangeType_Removed
)

type RosterChangedNotification struct {
	Uid   int64 `json:"uid,omitempty"`
	Ruid  int64 `json:"ruid,omitempty"`
	Type  int16 `json:"t,omitempty"`
	Stamp int64 `json:"st,omitempty"`
}

const createRosterChangesTableSql = `
CREATE TABLE IF NOT EXISTS rosterchanges
		(
		  uid bigint NOT NULL default 0,
		  ruid bigint NOT NULL default 0,
		  type smallint NOT NULL default 0,
		  stamp bigint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
		`

const (
	RosterChangeCode_None int8 = iota
	ROsterChangeCode_InvalidFormat
	RosterChangeCode_RosterNotExist
	RosterChangeCode_DatabaseErr
)

type RemoveRosterReqPkt struct {
	Uid  int64 `json:"uid"`
	Ruid int64 `json:"ruid"`
}

type RemoveRosterResPkt struct {
	Uid  int64 `json:"uid"`
	Ruid int64 `json:"ruid"`
	Code int8  `json:"code"`
}

type GetAllRostersReqPkt struct {
	Uid int64 `json:"uid"`
}

type GetAllRostersResPkt struct {
	Uid     int64    `json:"uid"`
	Rosters []Roster `json:"rs, omitempty"`
}

type GetRostersReqPkt struct {
	Uid   int64   `json:"uid"`
	Ruids []int64 `json:"ruids"`
}

type GetRostersResPkt struct {
	Uid     int64    `json:"uid"`
	Rosters []Roster `json:"rs, omitempty"`
}

type SetRosterResPkt struct {
	Code int8 `json:"code"`
}

type GetRosterChangedReqPkt struct {
	Stamp int64 `json:"st,omitempty"`
	Uid   int64 `json:"uid,omitempty"`
}

type GetRosterChangedResPkt struct {
	Stamp         int64                       `json:"st,omitempty"`
	Notifications []RosterChangedNotification `json:"ns,omitempty"`
}

func GetRosters(uid int64, ruids []int64) (rosters []Roster, err error) {
	rosters = make([]Roster, 0, len(ruids))
	for _, ruid := range ruids {
		roster, err := GetRoster(uid, ruid)
		if err != nil {
			return rosters, err
		}
		rosters = append(rosters, roster)
	}
	return
}

func GetRoster(uid, ruid int64) (roster Roster, err error) {
	command := `
	SELECT remark, groupname, stamp FROM roster where uid = @uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, uidParam, ruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&roster.Remark, &roster.Group, &roster.Stamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				roster.Uid = uid
				roster.Ruid = ruid
			}
		} else {
			err = errors.New("roster not exist")
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetRostersOfUid(uid int64) (rosters []Roster) {
	command := `
	SELECT ruid, remark, groupname, stamp FROM roster where uid = @uid;
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
			rosters = make([]Roster, 0, 100)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var roster Roster
					//roster.Uid = uid
					err = res.Scan(&roster.Ruid, &roster.Remark, &roster.Group, &roster.Stamp)
					if err != nil {
						logs.Logger.Critical("database error =", err)
					} else {
						rosters = append(rosters, roster)
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

func IsRosterExist(uid, ruid int64) (exist bool, err error) {
	exist = false

	command := `
	SELECT COUNT(*) AS num FROM roster where uid = @uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, uidParam, ruidParam)
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

func deleteRoster(uid, ruid int64) (err error) {
	command := `
	delete from roster where uid = @uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, ruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	stamp := time.Now().UnixNano()
	var change RosterChangedNotification
	change.Uid = uid
	change.Ruid = ruid
	change.Type = RosterChangeType_Removed
	CreateRosterChange(change, stamp)
	return
}

func RemoveRoster(reqPkt RemoveRosterReqPkt) (code int8) {
	exist, _ := users.IsUidExist(reqPkt.Uid)
	if !exist {
		code = RosterChangeCode_RosterNotExist
		return
	}
	exist, _ = users.IsUidExist(reqPkt.Ruid)
	if !exist {
		code = RosterChangeCode_RosterNotExist
		return
	}

	code = RosterChangeCode_DatabaseErr
	err := deleteRoster(reqPkt.Uid, reqPkt.Ruid)
	if err != nil {
		return
	}
	err = deleteRoster(reqPkt.Ruid, reqPkt.Uid)
	if err != nil {
		return
	}
	code = RosterChangeCode_None
	return
}

func updateRoster(roster Roster, stamp int64) (err error) {
	command := `
	update roster set remark=@remark,groupname=@group,stamp=@stamp where uid=@uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(roster.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(roster.Ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	remarkParam := pgsql.NewParameter("@remark", pgsql.Text)
	err = remarkParam.SetValue(roster.Remark)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	groupParam := pgsql.NewParameter("@group", pgsql.Text)
	err = groupParam.SetValue(roster.Group)
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
		_, err := conn.Execute(command, remarkParam, groupParam, stampParam, uidParam, ruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetRoster(roster Roster) (code int8) {
	code = RosterChangeCode_DatabaseErr
	exist, err := IsRosterExist(roster.Uid, roster.Ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		logs.Logger.Critical("roster not exist")
		code = RosterChangeCode_RosterNotExist
		return
	}
	stamp := time.Now().UnixNano()
	err = updateRoster(roster, stamp)
	if err != nil {
		return
	}
	var change RosterChangedNotification
	change.Uid = roster.Uid
	change.Ruid = roster.Ruid
	change.Type = RosterChangeType_Updated
	CreateRosterChange(change, stamp)
	code = RosterChangeCode_None
	return
}

func insertRoster(roster Roster, stamp int64) (err error) {
	command := `
	INSERT INTO roster(uid,ruid,remark,groupname,stamp) 
		VALUES(@uid,@ruid,@remark,@group,@stamp);
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(roster.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(roster.Ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	remarkParam := pgsql.NewParameter("@remark", pgsql.Text)
	err = remarkParam.SetValue(roster.Remark)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	groupParam := pgsql.NewParameter("@group", pgsql.Text)
	err = groupParam.SetValue(roster.Group)
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
		_, err := conn.Execute(command, uidParam, ruidParam, remarkParam, groupParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	var change RosterChangedNotification
	change.Uid = roster.Uid
	change.Ruid = roster.Ruid
	change.Type = RosterChangeType_Added
	CreateRosterChange(change, stamp)
	return
}

func GetRosterChanges(uid, stamp int64) (changes []RosterChangedNotification, lastStamp int64) {
	command := `
	SELECT ruid, type, stamp from rosterchanges where uid = @uid AND stamp > @stamp ORDER BY stamp ASC;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
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
		res, err := conn.Query(command, uidParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			changes = make([]RosterChangedNotification, 0, 5)
			lastStamp = 0
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var change RosterChangedNotification
					change.Uid = uid
					var tempStamp int64
					err = res.Scan(&change.Ruid, &change.Type, &tempStamp)
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

func IsRosterChangeExist(change RosterChangedNotification) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS num FROM rosterchanges WHERE uid = @uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(change.Ruid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, uidParam, ruidParam)
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

func insertRosterChange(change RosterChangedNotification, stamp int64) (err error) {
	command := `
	INSERT INTO rosterchanges(uid,ruid,type,stamp) 
			VALUES(@uid,@ruid,@type,@stamp);
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(change.Ruid)
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
		_, err := conn.Execute(command, uidParam, ruidParam, typeParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateRosterChange(change RosterChangedNotification, stamp int64) (err error) {
	command := `
	update rosterchanges set type=@type, stamp=@stamp where uid = @uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(change.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(change.Ruid)
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
		_, err := conn.Execute(command, typeParam, stampParam, uidParam, ruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func CreateRosterChange(change RosterChangedNotification, stamp int64) {
	exist, _ := users.IsUidExist(change.Uid)
	if !exist {
		logs.Logger.Criticalf("uid %v is not exist", change.Uid)
		return
	}
	exist, _ = users.IsUidExist(change.Ruid)
	if !exist {
		logs.Logger.Criticalf("ruid %v is not exist", change.Ruid)
		return
	}

	logs.Logger.Infof("ruid = %v uid = %v type = %v", change.Ruid, change.Uid, change.Type)

	exist, err := IsRosterChangeExist(change)
	if err != nil {
		return
	}
	if exist {
		err = updateRosterChange(change, stamp)
	} else {
		err = insertRosterChange(change, stamp)
	}

	if err == nil {
		change.Stamp = stamp
		RosterChangedNotificationChan <- change
	}

}
