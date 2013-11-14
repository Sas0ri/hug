package messages

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

const createMsgPushTableSql = `
	CREATE TABLE IF NOT EXISTS msgpush
		(
		  uid bigint NOT NULL,
		  ContactId bigint NOT NULL default 0,
		  ContactType smallint NOT NULL default 1
		)
		WITH (OIDS=FALSE);
		`

const (
	SetMsgPushCode_None int8 = iota
	SetMsgPushCode_InvalidRequest
	SetMsgPushCode_DatabaseErr
)

type SetMsgPushReqPkt struct {
	Uid     int64          `json:"uid,omitempty"`
	Contact MessageContact `json:"c,omitempty"`
	Push    bool           `json:"np,omitempty"`
}

type SetMsgPushResPkt struct {
	Code int8 `json:"code"`
}

type GetMsgPushReqPkt struct {
	Uid     int64          `json:"uid,omitempty"`
	Contact MessageContact `json:"c,omitempty"`
}

type GetMsgPushResPkt struct {
	Uid     int64          `json:"uid,omitempty"`
	Contact MessageContact `json:"c,omitempty"`
	Push    bool           `json:"np,omitempty"`
}

func SetMsgPush(reqPkt SetMsgPushReqPkt) (resPkt SetMsgPushResPkt) {
	if reqPkt.Push {
		if !IsMsgPush(reqPkt.Uid, reqPkt.Contact) {
			err := deleteMsgPush(reqPkt.Uid, reqPkt.Contact)
			if err != nil {
				resPkt.Code = SetMsgPushCode_DatabaseErr
				return
			}
		}
	} else {
		if IsMsgPush(reqPkt.Uid, reqPkt.Contact) {
			err := insertMsgPush(reqPkt.Uid, reqPkt.Contact)
			if err != nil {
				resPkt.Code = SetMsgPushCode_DatabaseErr
				return
			}
		}
	}
	resPkt.Code = SetMsgPushCode_None
	return
}

func deleteMsgPush(uid int64, contact MessageContact) (err error) {
	command := `
		delete from msgpush where uid = @uid AND ContactId = @ContactId AND ContactType = @ContactType;
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@ContactId", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@ContactType", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, uidParam, contactIdParam, contactTypeParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func insertMsgPush(uid int64, contact MessageContact) (err error) {
	command := `
		INSERT INTO msgpush(uid,ContactId,ContactType) 
		VALUES(@uid, @ContactId, @ContactType);
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@ContactId", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@ContactType", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, uidParam, contactIdParam, contactTypeParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func IsMsgPush(uid int64, contact MessageContact) (push bool) {
	command := `
		SELECT COUNT(*) as nums from msgpush where uid = @uid AND ContactId = @ContactId AND ContactType = @ContactType;
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactIdParam := pgsql.NewParameter("@ContactId", pgsql.Bigint)
	err = contactIdParam.SetValue(contact.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	contactTypeParam := pgsql.NewParameter("@ContactType", pgsql.Smallint)
	err = contactTypeParam.SetValue(contact.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	count := 0

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, uidParam, contactIdParam, contactTypeParam)
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
	if count > 0 {
		return false
	} else {
		return true
	}
}
