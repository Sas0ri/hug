package messages

import (
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

type RecentContact struct {
	UnreadCount int32   `json:"uc,omitempty"`
	LastMessage Message `json:"lm,omitempty"`
}

type GetRencetContactsReqPacket struct {
	Uid     int64 `json:"uid,omitempty"`
	LastMid int64 `json:"lmid,omitempty"`
	Size    int   `json:"sz,omitempty"`
}

type GetRencetContactsResPacket struct {
	Contacts []RecentContact `json:"cs,omitempty"`
}

const createRecentTableSql = `
CREATE TABLE IF NOT EXISTS recent
		(
		  mid bigint NOT NULL default 0,
		  uid bigint NOT NULL default 0,
		  contactid bigint NOT NULL default 0,
		  contacttype smallint NOT NULL default 1,
		  dir smallint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
`

func GetRecentContacts(reqPkt GetRencetContactsReqPacket) (resPkt GetRencetContactsResPacket) {
	if reqPkt.Size == 0 {
		return
	}
	if reqPkt.Uid == 0 {
		return
	}
	command := `
		SELECT mid, contactid, contacttype, dir FROM recent where uid = @uid AND mid > @mid ORDER BY mid ASC;
		`
	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)
	err := midParam.SetValue(reqPkt.LastMid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, uidParam, midParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		resPkt.Contacts = make([]RecentContact, 0, reqPkt.Size)
		for {
			hasRow, _ := res.FetchNext()
			if hasRow {
				var recentContact RecentContact
				var contact MessageContact
				var dir int16
				err = res.Scan(&recentContact.LastMessage.Id, &contact.Id, &contact.Type, &dir)
				if err != nil {
					logs.Logger.Critical(fmt.Sprintln("database scan error =", err))
					continue
				}
				body, err := GetMsgBody(recentContact.LastMessage.Id)
				if err != nil {
					logs.Logger.Critical(err)
					continue
				}
				recentContact.LastMessage.Author = body.Author
				recentContact.LastMessage.AuthorTerminal = body.AuthorTerminal
				recentContact.LastMessage.Items = body.Items
				recentContact.LastMessage.Stamp = body.Stamp
				if dir == MessageDir_Out {
					recentContact.LastMessage.From.Id = reqPkt.Uid
					recentContact.LastMessage.From.Type = MCT_User
					recentContact.LastMessage.To = contact
					recentContact.UnreadCount = 0
				} else {
					recentContact.LastMessage.To.Id = reqPkt.Uid
					recentContact.LastMessage.To.Type = MCT_User
					recentContact.LastMessage.From = contact
					recentContact.UnreadCount = GetUnreadMsgCount(reqPkt.Uid, contact)
				}
				resPkt.Contacts = append(resPkt.Contacts, recentContact)
				if len(resPkt.Contacts) >= reqPkt.Size {
					break
				}
			} else {
				break
			}

		}
	}
	res.Close()
	pool.Release(conn)
	return
}

func CreateRecent(mid, uid int64, contact MessageContact, dir int16) {
	command := `
		INSERT INTO recent(mid,uid,contactid,contacttype,dir) 
		VALUES(@mid,@uid,@contactid,@contacttype,@dir);
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
	dirParam := pgsql.NewParameter("@dir", pgsql.Smallint)
	err = dirParam.SetValue(dir)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, midParam, uidParam, contactIdParam, contactTypeParam, dirParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
}

func RemoveRecent(uid int64, contact MessageContact) {
	command := `
	delete from recent where uid = @uid AND contactid = @contactid AND contacttype = @contacttype;
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err = conn.Execute(command, uidParam, contactIdParam, contactTypeParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}

	pool.Release(conn)
}

func RemoveContactRecent(contact MessageContact) {
	command := `
	delete from recent where contactid = @contactid AND contacttype = @contacttype;
	`
	contactIdParam := pgsql.NewParameter("@contactid", pgsql.Bigint)
	err := contactIdParam.SetValue(contact.Id)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err = conn.Execute(command, contactIdParam, contactTypeParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}

	pool.Release(conn)
}

func UpdateRecent(mid, uid int64, contact MessageContact, dir int16) {
	RemoveRecent(uid, contact)
	CreateRecent(mid, uid, contact, dir)
	return
}
