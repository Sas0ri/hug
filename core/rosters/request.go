package rosters

import (
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
	"time"
)

const (
	RosterRequestStatus_None int16 = iota
	RosterRequestStatus_Waitting
	RosterRequestStatus_Accept
	RosterRequestStatus_Rject
)

const createRequestTableSql = `
CREATE TABLE IF NOT EXISTS rosterrequest
		(
		  rrid serial NOT NULL,
		  fromuid bigint NOT NULL,
		  touid bigint NOT NULL,
		  verifytext character varying(300) default '',
		  remark character varying(50) default '',
		  groupname  character varying(50) default '',
		  status smallint NOT NULL default 1,
		  stamp bigint NOT NULL,
		  CONSTRAINT rosterrequest_pkey PRIMARY KEY (rrid)
		)
		WITH (OIDS=FALSE);
		`

const (
	RosterRequestCode_None int8 = iota
	RosterRequestCode_InvalidFormat
	RosterRequestCode_FromUidNotExist
	RosterRequestCode_ToUidNotExist
	RosterRequestCode_RosterAlready
	RosterRequestCode_RosterIgnore
	RosterRequestCode_DatabaseErr
)

type RosterRequest struct {
	RequestId  int64  `json:"rrid,omitempty"`
	FromUid    int64  `json:"fuid"`
	ToUid      int64  `json:"tuid"`
	VerifyText string `json:"verify,omitempty"`
	Status     int16  `json:"status"`
	Stamp      int64  `json:"stamp"`
	Remark     string `json:"remark, omitempty"`
	Group      string `json:"group, omitempty"`
}

type RosterRequestResPkt struct {
	Code      int8  `json:"code"`
	RequestId int64 `json:"rrid"`
}

type GetRosterRequestReqPkt struct {
	Uid   int64 `json:"uid,omitempty"`
	Stamp int64 `json:"st,omitempty"`
	Size  int   `json:"sz,omitempty"`
}

type GetRosterReqeustResPkt struct {
	Requests []RosterRequest `json:"rs, omitempty"`
}

const (
	HandleRosterRequestType_None int8 = iota
	HandleRosterRequestType_Accept
	HandleRosterRequestType_Reject
)

const (
	HandleRosterRequestCode_None int8 = iota
	HandleRosterRequestCode_InvalidRequest
	HandleRosterRequestCode_RequestNotExist
	HandleRosterRequestCode_HandleAlready
	HandleRosterReqeustCode_DatabaseErr
)

type HandleRosterRequestReqPkt struct {
	RequestId int64  `json:"rrid"`
	Type      int8   `json:"type"`
	Remark    string `json:"remark, omitempty"`
	Group     string `json:"group, omitempty"`
}

type HandleRosterRequestResPkt struct {
	Code      int8  `json:"code"`
	RequestId int64 `json:"rrid"`
}

type HandleRosterRequestNotification struct {
	FromUid   int64 `json:"fuid"`
	ToUid     int64 `json:"tuid"`
	Type      int8  `json:"type"`
	RequestId int64 `json:"rrid"`
}

func insertRosterRequest(reqPkt RosterRequest) (request RosterRequest, err error) {
	logs.Logger.Infof("fromuid = %v touid = %v", reqPkt.FromUid, reqPkt.ToUid)
	command := `
	INSERT INTO rosterrequest(fromuid,touid,verifytext,remark,groupname,status,stamp) 
		VALUES(@fromuid,@touid,@verifytext,@remark,@group,@status,@stamp) RETURNING rrid;
	`
	fromParam := pgsql.NewParameter("@fromuid", pgsql.Bigint)
	err = fromParam.SetValue(reqPkt.FromUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	toParam := pgsql.NewParameter("@touid", pgsql.Bigint)
	err = toParam.SetValue(reqPkt.ToUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	verifyParam := pgsql.NewParameter("@verifytext", pgsql.Text)
	err = verifyParam.SetValue(reqPkt.VerifyText)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	remarkParam := pgsql.NewParameter("@remark", pgsql.Text)
	err = remarkParam.SetValue(reqPkt.Remark)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	groupParam := pgsql.NewParameter("@group", pgsql.Text)
	err = groupParam.SetValue(reqPkt.Group)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(RosterRequestStatus_Waitting)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stamp := time.Now().UnixNano()
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
		res, err := conn.Query(command, fromParam, toParam, verifyParam, remarkParam, groupParam, statusParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			var rrid int64
			_, err = res.ScanNext(&rrid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				request.FromUid = reqPkt.FromUid
				request.ToUid = reqPkt.ToUid
				request.VerifyText = reqPkt.VerifyText
				request.Stamp = stamp
				request.Status = RosterRequestStatus_Waitting
				request.RequestId = rrid
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func updateRosterRequest(reqPkt RosterRequest, status int16) (stamp int64, err error) {
	logs.Logger.Infof("fromuid = %v touid = %v", reqPkt.FromUid, reqPkt.ToUid)
	command := `
	update rosterrequest set verifytext=@verifytext,remark=@remark,groupname=@groupname,status=@status,stamp=@stamp  
		where fromuid=@fromuid AND touid=@touid;
	`
	fromParam := pgsql.NewParameter("@fromuid", pgsql.Bigint)
	err = fromParam.SetValue(reqPkt.FromUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	toParam := pgsql.NewParameter("@touid", pgsql.Bigint)
	err = toParam.SetValue(reqPkt.ToUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	verifyParam := pgsql.NewParameter("@verifytext", pgsql.Text)
	err = verifyParam.SetValue(reqPkt.VerifyText)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	remarkParam := pgsql.NewParameter("@remark", pgsql.Text)
	err = remarkParam.SetValue(reqPkt.Remark)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	groupParam := pgsql.NewParameter("@groupname", pgsql.Text)
	err = groupParam.SetValue(reqPkt.Group)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stamp = time.Now().UnixNano()
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
		_, err := conn.Execute(command, verifyParam, remarkParam, groupParam, statusParam, stampParam, fromParam, toParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func RequestId(fromUid, toUid int64) (rrid int64, err error) {
	logs.Logger.Infof("fromuid = %v touid = %v", fromUid, toUid)
	command := `
	SELECT rrid FROM rosterrequest where fromuid = @fromuid AND touid = @touid;
	`
	fromParam := pgsql.NewParameter("@fromuid", pgsql.Bigint)
	err = fromParam.SetValue(fromUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	toParam := pgsql.NewParameter("@touid", pgsql.Bigint)
	err = toParam.SetValue(toUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, fromParam, toParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&rrid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateRosterRequest(reqPkt RosterRequest) (request RosterRequest, code int8) {
	logs.Logger.Infof("fromuid = %v touid = %v", reqPkt.FromUid, reqPkt.ToUid)
	if !users.IsUidValid(reqPkt.FromUid) {
		code = RosterRequestCode_FromUidNotExist
		return
	}
	if !users.IsUidValid(reqPkt.ToUid) {
		code = RosterRequestCode_ToUidNotExist
		return
	}
	exist, _ := users.IsUidExist(reqPkt.FromUid)
	if !exist {
		code = RosterRequestCode_FromUidNotExist
		return
	}

	exist, _ = users.IsUidExist(reqPkt.ToUid)
	if !exist {
		code = RosterRequestCode_ToUidNotExist
		return
	}

	ignoreRequest, err := GetIgnoreRequest(reqPkt.ToUid, reqPkt.FromUid)
	if err != nil {
		code = RosterRequestCode_DatabaseErr
		return
	}
	if ignoreRequest.Type == IgnoreRequestType_Ignore {
		code = RosterRequestCode_RosterIgnore
		return
	}

	exist, err = IsRosterExist(reqPkt.FromUid, reqPkt.ToUid)
	if err != nil {
		code = RosterRequestCode_DatabaseErr
		return
	}
	if exist {
		code = RosterRequestCode_RosterAlready
		return
	}

	rrid, err := RequestId(reqPkt.FromUid, reqPkt.ToUid)
	if err != nil {
		code = RosterRequestCode_DatabaseErr
		return
	}
	if rrid > 0 {
		stamp, err := updateRosterRequest(reqPkt, RosterRequestStatus_Waitting)
		if err != nil {
			code = RosterRequestCode_DatabaseErr
			return
		}
		request.FromUid = reqPkt.FromUid
		request.ToUid = reqPkt.ToUid
		request.VerifyText = reqPkt.VerifyText
		request.Stamp = stamp
		request.Status = RosterRequestStatus_Waitting
		request.RequestId = rrid
		code = RosterRequestCode_None
		return
	}

	request, err = insertRosterRequest(reqPkt)
	if err != nil {
		code = RosterRequestCode_DatabaseErr
		return
	}

	code = RosterRequestCode_None
	return
}

func GetRosterReqeusts(reqPkt GetRosterRequestReqPkt) (resPkt GetRosterReqeustResPkt) {
	if reqPkt.Stamp > 0 {
		command := `
		SELECT rrid, fromuid, verifytext, status, stamp FROM rosterrequest where touid = @touid AND stamp > @stamp ORDER BY stamp ASC;
		`
		toParam := pgsql.NewParameter("@touid", pgsql.Bigint)
		err := toParam.SetValue(reqPkt.Uid)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}
		stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
		err = stampParam.SetValue(reqPkt.Stamp)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}

		conn, err := pool.Acquire()
		if err != nil {
			logs.Logger.Critical("Error acquiring connection: ", err)
		} else {
			res, err := conn.Query(command, toParam, stampParam)
			if err != nil {
				logs.Logger.Critical("Error execute query: ", err)
			} else {
				resPkt.Requests = make([]RosterRequest, 0, 20)
				for {
					hasRow, _ := res.FetchNext()
					if hasRow {
						var request RosterRequest
						request.ToUid = reqPkt.Uid
						err = res.Scan(&request.RequestId, &request.FromUid, &request.VerifyText, &request.Status, &request.Stamp)
						if err != nil {
							logs.Logger.Critical("database error =", err)
						} else {
							resPkt.Requests = append(resPkt.Requests, request)
							if len(resPkt.Requests) >= reqPkt.Size {
								return
							}
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
	} else {
		command := `
		SELECT rrid, fromuid, verifytext, stamp FROM rosterrequest where touid = @touid AND status = @status ORDER BY stamp ASC;
		`
		toParam := pgsql.NewParameter("@touid", pgsql.Bigint)
		err := toParam.SetValue(reqPkt.Uid)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}
		statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
		err = statusParam.SetValue(RosterRequestStatus_Waitting)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}
		conn, err := pool.Acquire()
		if err != nil {
			logs.Logger.Critical("Error acquiring connection: ", err)
		} else {
			res, err := conn.Query(command, toParam, statusParam)
			if err != nil {
				logs.Logger.Critical("Error execute query: ", err)
			} else {
				resPkt.Requests = make([]RosterRequest, 0, 20)
				for {
					hasRow, _ := res.FetchNext()
					if hasRow {
						var request RosterRequest
						request.ToUid = reqPkt.Uid
						request.Status = RosterRequestStatus_Waitting
						err = res.Scan(&request.RequestId, &request.FromUid, &request.VerifyText, &request.Stamp)
						if err != nil {
							logs.Logger.Critical("database error =", err)
						} else {
							resPkt.Requests = append(resPkt.Requests, request)
							if len(resPkt.Requests) >= reqPkt.Size {
								return
							}
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
}

func updateRequestStatus(rrid int64, status int16) (err error) {
	command := `
	update rosterrequest set status=@status,stamp=@stamp  
		where rrid = @rrid;
	`
	rridParam := pgsql.NewParameter("@rrid", pgsql.Bigint)
	err = rridParam.SetValue(rrid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stamp := time.Now().UnixNano()
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
		_, err := conn.Execute(command, statusParam, stampParam, rridParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func GetRequest(rrid int64) (request RosterRequest, err error) {
	command := `
		SELECT fromuid, touid, verifytext, remark, groupname, status, stamp FROM rosterrequest where rrid=@rrid;
		`
	rridParam := pgsql.NewParameter("@rrid", pgsql.Bigint)
	err = rridParam.SetValue(rrid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, rridParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&request.FromUid, &request.ToUid, &request.VerifyText, &request.Remark, &request.Group, &request.Status, &request.Stamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				request.RequestId = rrid
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func HandleRosterRequest(reqPkt HandleRosterRequestReqPkt) (resPkt HandleRosterRequestResPkt) {
	resPkt.Code = HandleRosterReqeustCode_DatabaseErr
	resPkt.RequestId = reqPkt.RequestId
	request, err := GetRequest(reqPkt.RequestId)
	if err != nil {
		return
	}
	if request.RequestId == 0 || request.Status != RosterRequestStatus_Waitting {
		resPkt.Code = HandleRosterRequestCode_RequestNotExist
		return
	}
	exist, err := IsRosterExist(request.ToUid, request.FromUid)
	if err != nil {
		return
	}
	if exist {
		resPkt.Code = HandleRosterRequestCode_HandleAlready
		return
	}

	if reqPkt.Type == HandleRosterRequestType_Reject {
		updateRequestStatus(reqPkt.RequestId, RosterRequestStatus_Rject)
	} else if reqPkt.Type == HandleRosterRequestType_Accept {
		updateRequestStatus(reqPkt.RequestId, RosterRequestStatus_Accept)
		stamp := time.Now().UnixNano()
		var roster Roster
		roster.Uid = request.FromUid
		roster.Ruid = request.ToUid
		roster.Remark = request.Remark
		roster.Group = request.Group
		roster.Stamp = stamp
		err = insertRoster(roster, stamp)
		if err != nil {
			resPkt.Code = HandleRosterReqeustCode_DatabaseErr
			return
		}
		roster.Uid = request.ToUid
		roster.Ruid = request.FromUid
		roster.Remark = reqPkt.Remark
		roster.Group = reqPkt.Group
		err = insertRoster(roster, stamp)
		if err != nil {
			resPkt.Code = HandleRosterReqeustCode_DatabaseErr
			return
		}
	}
	resPkt.Code = HandleRosterRequestCode_None
	var notification HandleRosterRequestNotification
	notification.Type = reqPkt.Type
	notification.FromUid = request.FromUid
	notification.ToUid = request.ToUid
	notification.RequestId = reqPkt.RequestId
	HandleRosterRequestNotificationChan <- notification
	return

}
