package rosters

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

const createIgnoreRequestTableSql = `
		CREATE TABLE IF NOT EXISTS ignore
		(
		  uid bigint NOT NULL,
		  ruid bigint NOT NULL,
		  type smallint NOT NULL default 1
		)
		WITH (OIDS=FALSE);
		`

const (
	IgnoreRequestType_None int16 = iota
	IgnoreRequestType_Ignore
)

type IgnoreRequest struct {
	Uid  int64 `json:"uid,omitempty"`
	RUid int64 `json:"ruid,omitempty"`
	Type int16 `json:"ruid,omitempty"`
}

const (
	SetIgnoreRequestCode_None int8 = iota
	SetIgnoreRequestCode_InvalidRequest
	SetIgnoreRequestCode_DatabaseErr
)

type SetIgnoreRequestResPkt struct {
	Code int8 `json:"code"`
}

func SetIgnoreRequest(reqPkt IgnoreRequest) (resPkt SetIgnoreRequestResPkt) {
	if reqPkt.Type == IgnoreRequestType_None {
		err := deleteIgnoreRequest(reqPkt.Uid, reqPkt.RUid)
		if err != nil {
			resPkt.Code = SetIgnoreRequestCode_DatabaseErr
			return
		}
	} else {
		exist, err := IsIgnoreRequestExist(reqPkt.Uid, reqPkt.RUid)
		if err != nil {
			resPkt.Code = SetIgnoreRequestCode_DatabaseErr
			return
		}
		if exist {
			err = updateIgnoreRequest(reqPkt)
			if err != nil {
				resPkt.Code = SetIgnoreRequestCode_DatabaseErr
				return
			}
		} else {
			err = insertIgnoreRequest(reqPkt)
			if err != nil {
				resPkt.Code = SetIgnoreRequestCode_DatabaseErr
				return
			}
		}
	}
	resPkt.Code = SetIgnoreRequestCode_None
	return
}

func deleteIgnoreRequest(uid, ruid int64) (err error) {
	command := `
		delete from ignore where uid = @uid AND ruid = @ruid;
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
		return
	}
	_, err = conn.Execute(command, uidParam, ruidParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func insertIgnoreRequest(reqPkt IgnoreRequest) (err error) {
	command := `
		INSERT INTO ignore(uid,ruid,type) 
		VALUES(@uid, @ruid, @type);
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(reqPkt.RUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(reqPkt.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, uidParam, ruidParam, typeParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func updateIgnoreRequest(reqPkt IgnoreRequest) (err error) {
	command := `
	update ignore set type = @type where uid=@uid AND ruid = @ruid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	ruidParam := pgsql.NewParameter("@ruid", pgsql.Bigint)
	err = ruidParam.SetValue(reqPkt.RUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	typeParam := pgsql.NewParameter("@type", pgsql.Smallint)
	err = typeParam.SetValue(reqPkt.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, typeParam, uidParam, ruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func GetIgnoreRequest(uid, ruid int64) (ignoreRequest IgnoreRequest, err error) {
	logs.Logger.Infof("uid = %v ruid = %v", uid, ruid)
	command := `
	SELECT type FROM ignore where uid = @uid AND ruid = @ruid;
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
			err = res.Scan(&ignoreRequest.Type)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				ignoreRequest.Uid = uid
				ignoreRequest.RUid = ruid
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsIgnoreRequestExist(uid, ruid int64) (exist bool, err error) {
	exist = false

	command := `
	SELECT COUNT(*) AS num FROM ignore where uid = @uid AND ruid = @ruid;
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
