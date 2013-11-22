package messages

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
)

const (
	MIT_None        int16 = iota
	MIT_Text              //"t"
	MIT_Image             //"i"
	MIT_Emoticons         //"e"
	MIT_Voice             //"v"
	MIT_Gif               //"g"
	MIT_OfflineFile       //"of"
)

type MessageItem struct {
	Count         int16       `json:"c,omitempty"`
	ItemType      int16       `json:"t,omitempty"`
	Data          interface{} `json:"d,omitempty"`
	VoiceDuration int32       `json:"vd,omitempty"` //second
}

const (
	MCT_None int16 = iota
	MCT_User
	MCT_Group
)

const (
	MessageDir_Out int16 = iota
	MessageDir_In
)

type MessageContact struct {
	Id   int64 `json:"id,omitempty"`
	Type int16 `json:"t,omitempty"`
}

type Message struct {
	From           MessageContact   `json:"fr,omitempty"`
	To             MessageContact   `json:"to,omitempty"`
	Ccs            []MessageContact `json:"cc,omitempty"`
	Author         MessageContact   `json:"ar,omitempty"`
	AuthorTerminal int16            `json:"at,omitempty"`
	Stamp          int64            `json:"st,omitempty"`
	Id             int64            `json:"id,omitempty"`
	Items          []MessageItem    `json:"bd,omitempty"`
}

type MessageBody struct {
	Author         MessageContact `json:"ar,omitempty"`
	AuthorTerminal int16          `json:"at,omitempty"`
	Stamp          int64          `json:"st,omitempty"`
	Id             int64          `json:"id,omitempty"`
	Items          []MessageItem  `json:"bd,omitempty"`
}

type MessageResPacket struct {
	OriginalId int64 `json:"oid,omitempty"`
	Mid        int64 `json:"mid,omitempty"`
}

const createMessagesTableSql = `
CREATE TABLE IF NOT EXISTS messages
		(
		  mid serial NOT NULL unique,
		  stamp bigint NOT NULL default 0,
		  AuthorId bigint NOT NULL default 0,
		  AuthorType smallint NOT NULL default 0,
		  AuthorTerminal smallint NOT NULL default 1,
		  body character varying(2000) default '', 
		  CONSTRAINT messages_pkey PRIMARY KEY (mid)
		)
		WITH (OIDS=FALSE);
		`

type MessageResponsePacket struct {
	OriginalId int64 `json:"o,omitempty"`
	NewId      int64 `json:"n,omitempty"`
}

// type GetMsgReqPacket struct {
// 	Uid          int64          `json:"u,omitempty"`
// 	Contact      MessageContact `json:"c,omitempty"`
// 	MinMessageId int64          `json:"min,omitempty"`
// 	MaxMessageId int64          `json:"max,omitempty"`
// 	Size         int            `json:"sz,omitempty"`
// }

// type GetMsgResPacket struct {
// 	Messages []Message `json:"ms,omitempty"`
// }

type GetMsgBodysReqPkt struct {
	Mids []int64 `json:"ids,omitempty"`
}

type GetMsgBodysResPkt struct {
	MessageBodys []MessageBody `json:"ms,omitempty"`
}

func GetMsgBody(mid int64) (msgBody MessageBody, err error) {
	command := `
		SELECT * FROM messages where mid = @mid;
		`
	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)
	err = midParam.SetValue(mid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, midParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		var body string
		fetched, err := res.ScanNext(&msgBody.Id, &msgBody.Stamp, &msgBody.Author.Id, &msgBody.Author.Type, &msgBody.AuthorTerminal, &body)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		} else if fetched {
			bodyBytes, err := base64.StdEncoding.DecodeString(body)
			if err != nil {
				logs.Logger.Critical(fmt.Sprintln("base64 decodestring err =", err))
			}
			err = json.Unmarshal(bodyBytes, &msgBody.Items)
			if err != nil {
				logs.Logger.Critical(fmt.Sprintln("json unmarshal err =", err))
			}
		}
	}
	res.Close()
	pool.Release(conn)
	return
}

func GetMsgBodys(reqPkt GetMsgBodysReqPkt) (resPkt GetMsgBodysResPkt) {
	resPkt.MessageBodys = make([]MessageBody, 0, len(reqPkt.Mids))
	for _, mid := range reqPkt.Mids {
		msg, err := GetMsgBody(mid)
		if err == nil {
			resPkt.MessageBodys = append(resPkt.MessageBodys, msg)
		}
	}
	return
}

// func GetMessages(reqPkt GetMsgReqPacket) (resPkt GetMsgResPacket) {
// 	if reqPkt.Size == 0 {
// 		return
// 	}
// 	if reqPkt.Uid == 0 {
// 		return
// 	}
// 	queryStr := `SELECT * FROM msghistory where
// 				((fromId = $1 AND fromType = $2 AND toId = $3 AND toType = $4)
// 				OR (fromId = $5 AND fromType = $6 AND toId = $7 AND toType = $8) ) AND mid > $9 AND mid < $10 ORDER BY mid ASC`
// 	stmt, err := db.Prepare(queryStr)
// 	if err != nil {
// 		logs.Logger.Critical(err)
// 		return
// 	}
// 	defer func() {
// 		stmt.Close()
// 	}()
// 	rows, err := stmt.Query(reqPkt.Uid, MCT_User, reqPkt.Contact.Id, reqPkt.Contact.Type, reqPkt.Contact.Id, reqPkt.Contact.Type, reqPkt.Uid, MCT_User, reqPkt.MinMessageId, reqPkt.MaxMessageId)
// 	if err != nil {
// 		logs.Logger.Critical(fmt.Sprintln("database select error =", err))
// 	}
// 	defer func() {
// 		rows.Close()
// 	}()
// 	resPkt.Messages = make([]Message, 0, reqPkt.Size)
// 	for rows.Next() {
// 		var msg Message
// 		var to MessageContact
// 		var sendedTerminal int16
// 		err = rows.Scan(&msg.Id, &msg.From.Id, &msg.From.Type, &to.Id, &to.Type, &sendedTerminal)
// 		if err != nil {
// 			logs.Logger.Critical(fmt.Sprintln("database scan error =", err))
// 		}
// 		msg.To = to
// 		body, err := GetMsgBody(msg.Id)
// 		if err != nil {
// 			logs.Logger.Critical(err)
// 			continue
// 		}
// 		msg.Author = body.Author
// 		msg.AuthorTerminal = body.AuthorTerminal
// 		msg.Items = body.Items
// 		msg.Stamp = body.Stamp
// 		resPkt.Messages = append(resPkt.Messages, msg)
// 		if reqPkt.Size > 0 && len(resPkt.Messages) >= reqPkt.Size {
// 			return
// 		}
// 	}
// 	ClearUnreadHistory(reqPkt.Contact.Id, reqPkt.Uid, reqPkt.Contact.Type, MCT_User)
// 	return
// }

func IsMidExist(mid int64) (exist bool, err error) {
	exist = false
	n := 0
	command := `
		SELECT COUNT(*) AS nummsgs FROM messages where mid = @mid;
		`
	midParam := pgsql.NewParameter("@mid", pgsql.Bigint)
	err = midParam.SetValue(mid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, midParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&n)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	res.Close()
	pool.Release(conn)

	if n >= 1 {
		exist = true
	} else {
		exist = false
	}
	return
}

func CreateMsg(msg Message) (mid int64) {
	defer func() {
		if err := recover(); err != nil {
			logs.Logger.Critical(err)
		}
	}()

	mid = 0
	if len(msg.Items) <= 0 {
		logs.Logger.Critical("message body is empty")
		return
	}
	if msg.Author.Id <= 0 {
		logs.Logger.Critical(fmt.Sprintln("message author id =", msg.Author.Id))
		return
	}
	if msg.To.Id == 0 {
		logs.Logger.Critical("message to is empty")
		return
	}
	if msg.From.Id <= 0 {
		logs.Logger.Critical(fmt.Sprintln("message from id =", msg.From.Id))
		return
	}
	if msg.Author.Type == MCT_User {
		exist, err := users.IsUidExist(msg.Author.Id)
		if err != nil {
			logs.Logger.Critical(fmt.Sprintln("check uid exist error =", err))
			return
		}
		if !exist {
			logs.Logger.Critical("author uid is not exist")
			return
		}
	}
	bodyBytes, err := json.Marshal(msg.Items)
	if err != nil {
		logs.Logger.Critical(fmt.Sprintln("json marshal body items error =", err))
		return
	}
	bodyStr := base64.StdEncoding.EncodeToString(bodyBytes)
	command := `
		INSERT INTO messages(AuthorId,AuthorType,AuthorTerminal,body,stamp) 
		VALUES(@AuthorId, @AuthorType, @AuthorTerminal, @body, @stamp) RETURNING mid;
		`
	authorIdParam := pgsql.NewParameter("@AuthorId", pgsql.Bigint)
	err = authorIdParam.SetValue(msg.Author.Id)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	authorTypeParam := pgsql.NewParameter("@AuthorType", pgsql.Smallint)
	err = authorTypeParam.SetValue(msg.Author.Type)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	authorTerminalParam := pgsql.NewParameter("@AuthorTerminal", pgsql.Smallint)
	err = authorTerminalParam.SetValue(msg.AuthorTerminal)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	bodyParam := pgsql.NewParameter("@body", pgsql.Text)
	err = bodyParam.SetValue(bodyStr)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(msg.Stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, authorIdParam, authorTypeParam, authorTerminalParam, bodyParam, stampParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&mid)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	res.Close()

	pool.Release(conn)
	return
}
