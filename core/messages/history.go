package messages

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

const (
	HistoryStatus_None int16 = iota
	HistoryStatus_WaitToSend
	HistoryStatus_Sended
	HistoryStatus_Removed
)

const createHistoryTableSql = `
CREATE TABLE IF NOT EXISTS history
		(
		  mid bigint NOT NULL,
		  uid bigint NOT NULL default 0,
		  contactid bigint NOT NULL default 0,
		  contacttype smallint NOT NULL default 1,
		  dir smallint NOT NULL default 0,
		  status smallint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
`

type GetMsgHistoryReqPkt struct {
	Uid          int64          `json:"u,omitempty"`
	Contact      MessageContact `json:"c,omitempty"`
	MinMessageId int64          `json:"min,omitempty"`
	MaxMessageId int64          `json:"max,omitempty"`
	Size         int            `json:"sz,omitempty"`
}

type GetMsgHistoryResPkt struct {
	InMids  []int64        `json:"is,omitempty"`
	OutMids []int64        `json:"os,omitempty"`
	Contact MessageContact `json:"c,omitempty"`
}

const (
	RemoveHistoryCode_None int8 = iota
	RemoveHistoryCode_InvalidFormat
	RemoveHistoryCode_DatabaseErr
)

type RemoveHistoryReqPkt struct {
	Uid     int64          `json:"u,omitempty"`
	Contact MessageContact `json:"c,omitempty"`
	Mids    []int64        `json:"mids,omitempty"`
}

type RemoveHistoryResPkt struct {
	Code int8 `json:"code"`
}

func GetMsgHistory(reqPkt GetMsgHistoryReqPkt) (resPkt GetMsgHistoryResPkt) {
	if reqPkt.Size == 0 {
		return
	}
	if reqPkt.Uid == 0 {
		return
	}
	if reqPkt.MaxMessageId < 0 {
		return
	}
	var command string
	if reqPkt.MaxMessageId == 0 {
		command = `
		SELECT mid, dir FROM history where uid = @uid AND contactid = @contactid 
		AND contacttype = @contacttype AND status != @status AND mid > @minMid ORDER BY mid ASC;
		`
	} else {
		command = `
		SELECT mid, dir FROM history where  uid = @uid AND contactid = @contactid 
		AND contacttype = @contacttype AND status != @status AND mid > @minMid AND mid < @maxMid  ORDER BY mid ASC;
		`
	}

	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(reqPkt.Contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(reqPkt.Contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(HistoryStatus_Removed)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	minMidParam := pgsql.NewParameter("@minMid", pgsql.Bigint)
	err = minMidParam.SetValue(reqPkt.MinMessageId)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	maxMidParam := pgsql.NewParameter("@maxMid", pgsql.Bigint)
	err = maxMidParam.SetValue(reqPkt.MaxMessageId)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	var res *pgsql.ResultSet
	if reqPkt.MaxMessageId == 0 {
		res, err = conn.Query(command, uidParam, contactIdParam, contactTypeParam, statusParam, minMidParam)
	} else {
		res, err = conn.Query(command, uidParam, contactIdParam, contactTypeParam, statusParam, minMidParam, maxMidParam)
	}

	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		resPkt.InMids = make([]int64, 0, 20)
		resPkt.OutMids = make([]int64, 0, 20)
		resPkt.Contact = reqPkt.Contact
		count := 0
		for {
			hasRow, _ := res.FetchNext()
			if hasRow {
				var mid int64
				var dir int16
				err = res.Scan(&mid, &dir)
				if err != nil {
					logs.Logger.Critical("database scan error =", err)
					continue
				}
				if dir == MessageDir_In {
					resPkt.InMids = append(resPkt.InMids, mid)
				} else {
					resPkt.OutMids = append(resPkt.OutMids, mid)
				}
				count++
				if count >= reqPkt.Size {
					break
				}
			} else {
				break
			}

		}
		res.Close()
	}

	// Return the connection back to the pool.
	pool.Release(conn)

	ClearUnreadHistory(reqPkt.Uid, reqPkt.Contact)
	return
}

func CreateHistory(mid, uid int64, contact MessageContact, status, dir int16) {
	command := `
		INSERT INTO history(mid,uid,contactid,contacttype,status,dir) 
		VALUES(@mid,@uid,@contactid,@contacttype,@status,@dir);
		`
	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)
	err := midParam.SetValue(mid)
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
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
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

	dirParam := pgsql.NewParameter("@dir", pgsql.Smallint)
	err = dirParam.SetValue(dir)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err = conn.Execute(command, midParam, uidParam, contactIdParam, contactTypeParam, statusParam, dirParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}

	pool.Release(conn)

	UpdateRecent(mid, uid, contact, dir)
}

func GetUnreadMsgCount(uid int64, contact MessageContact) (count int32) {
	count = 0
	command := `
	SELECT COUNT(*) AS numunread FROM history where uid = @uid AND contactid = @contactid 
	AND contacttype = @contacttype AND status = @status AND dir = @dir;
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(HistoryStatus_WaitToSend)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	dirParam := pgsql.NewParameter("@dir", pgsql.Smallint)
	err = dirParam.SetValue(MessageDir_In)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, uidParam, contactIdParam, contactTypeParam, statusParam, dirParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&count)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	res.Close()
	pool.Release(conn)
	return
}

func GetUnreadMsgCountOfUid(uid int64) (count int) {
	count = 0
	command := `
	SELECT COUNT(*) AS numunread FROM history where  uid = @uid AND status = @status AND dir = @dir;
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(HistoryStatus_WaitToSend)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	dirParam := pgsql.NewParameter("@dir", pgsql.Smallint)
	err = dirParam.SetValue(MessageDir_In)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, uidParam, statusParam, dirParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&count)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	res.Close()
	pool.Release(conn)
	return
}

func SetMsgHistoryStatus(mid, uid int64, contact MessageContact, status int16) {
	command := `
	update history set status=@status where mid = @mid AND uid = @uid AND contactid = @contactid 
	AND contacttype = @contacttype;
		`
	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)
	err := midParam.SetValue(mid)
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
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, statusParam, midParam, uidParam, contactIdParam, contactTypeParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func removeSpecificHistory(uid int64, contact MessageContact, mids []int64) (err error) {
	command := `
	update history set status=@status where mid = @mid AND uid = @uid AND contactid = @contactid 
	AND contacttype = @contacttype;
	`

	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)

	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(HistoryStatus_Removed)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	for _, mid := range mids {
		err = midParam.SetValue(mid)
		if err != nil {
			logs.Logger.Critical(err)
			continue
		}
		_, err = conn.Execute(command, statusParam, midParam, uidParam, contactIdParam, contactTypeParam)
		if err != nil {
			logs.Logger.Critical("Error executing query: ", err)
		}
	}
	pool.Release(conn)
	return

}

func removeContactHistory(uid int64, contact MessageContact) (err error) {
	command := `
	update history set status=@status where uid = @uid AND contactid = @contactid 
	AND contacttype = @contacttype;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(HistoryStatus_Removed)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, statusParam, uidParam, contactIdParam, contactTypeParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}

	pool.Release(conn)
	return

}

func RemoveHistory(uid int64, contact MessageContact, mids []int64) (code int8) {
	if len(mids) > 0 {
		err := removeSpecificHistory(uid, contact, mids)
		if err != nil {
			code = RemoveHistoryCode_DatabaseErr
			return
		}
	} else {
		RemoveRecent(uid, contact)
		err := removeContactHistory(uid, contact)
		if err != nil {
			code = RemoveHistoryCode_DatabaseErr
			return
		}
	}
	return RemoveHistoryCode_None
}

func ClearUnreadHistory(uid int64, contact MessageContact) {
	command := `
	update history set status=@newstatus where uid = @uid AND contactid = @contactid 
	AND contacttype = @contacttype AND status = @oldstatus;
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@contacttype", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	oldStatusParam := pgsql.NewParameter("@oldstatus", pgsql.Smallint)
	err = oldStatusParam.SetValue(HistoryStatus_WaitToSend)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	newStatusParam := pgsql.NewParameter("@newstatus", pgsql.Smallint)
	err = newStatusParam.SetValue(HistoryStatus_Sended)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, newStatusParam, uidParam, contactIdParam, contactTypeParam, oldStatusParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}
