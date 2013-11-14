package corps

import (
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
	"time"
)

const (
	CorpStatus_None int16 = iota
	CorpStatus_Created
	CorpStatus_Verifyed
	CorpStatus_Frozen
)

const (
	CreateCorpCode_None int8 = iota
	CreateCorpCode_InvalidReq
	CreateCorpCode_UidNotExist
	CreateCorpCode_UserNotAllowCreateCorpMore
	CreateCorpCode_CorpExist
	CreateCorpCode_InvalidCorpFullName
	CreateCorpCode_DatabaseErr
	CreateCorpCode_CreateWorkerErr
)

const (
	RemoveCorpCode_None int8 = iota
	RemoveCorpCode_InvalidReq
	RemoveCorpCode_CorpNotExist
	RemoveCorpCode_DatabaseErr
)

type Corp struct {
	Cid            int64  `json:"id"`
	FullName       string `json:"fn"`
	Status         int16  `json:"st"`
	ShortName      string `json:"sn,omitempty"`
	LogoFile       string `json:"lf,omitempty"`
	HomePage       string `json:"hp,omitempty"`
	Location       string `json:"lo,omitempty"`
	Addr           string `json:"ar,omitempty"`
	MaxWorkerAllow int32  `json:"mwa,omitempty"`
	OwnerUid       int64  `json:"ou,omitempty"`
	CreateStamp    int64  `json:"cs,omitempty"`
	UpdateStamp    int64  `json:"us,omitempty"`
}

type CreateCorpReqPkt struct {
	FullName  string `json:"fullname"`
	ShortName string `json:"shortname, omitempty"`
	HomePage  string `json:"homepage, omitempty"`
	Location  string `json:"location,omitempty"`
	Addr      string `json:"addr,omitempty"`
	OwnerUid  int64  `json:"owneruid,omitempty"`
	OwnerName string `json:"ownername,omitempty"`
	OwnerPost string `json:"ownerpost,omitempty"`
}

type CreateCorpResPkt struct {
	Code int8  `json:"code"`
	Cid  int64 `json:"cid"`
}

type RemoveCorpReqPkt struct {
	Cid int64 `json:"cid"`
}

type RemoveCorpResPkt struct {
	Code int8 `json:"code"`
}

type GetCorpTreesReqPkt struct {
	Cids []int64 `json:"cids"`
}

type GetCorpTreesResPkt struct {
	Workers []Worker `json:"w,omitempty"`
	Depts   []Dept   `json:"d,omitempty"`
}

type GetCorpsOfUserReqPkt struct {
	Uid int64 `json:"uid"`
}

type GetCorpsOfUserResPkt struct {
	Corps   []Corp   `json:"c,omitempty"`
	Workers []Worker `json:"w,omitempty"`
	Depts   []Dept   `json:"d,omitempty"`
}

type GetCorpReqPkt struct {
	Cid int64 `json:"cid"`
}

type GetCidsReqPkt struct {
	Uid int64 `json:"uid"`
}

type GetCidsResPkt struct {
	Cids []int64 `json:"cids"`
}

const (
	CorpChangedType_None int16 = iota
	CorpChangedType_Create
	CorpChangedType_Updated
	CorpChangedType_Removed
)

type CorpChangedNotification struct {
	Cid   int64 `json:"cid,omitempty"`
	Wid   int64 `json:"wid,omitempty"`
	Did   int64 `json:"did,omitempty"`
	Type  int16 `json:"t,omitempty"`
	Stamp int64 `json:"st,omitempty"`
}

type GetCorpChangedReqPkt struct {
	Stamp int64   `json:"st,omitempty"`
	Cids  []int64 `json:"cids,omitempty"`
}

type GetCorpChangedResPkt struct {
	Stamp         int64                     `json:"st,omitempty"`
	Notifications []CorpChangedNotification `json:"ns,omitempty"`
}

const createCorpChangesTableSql = `
CREATE TABLE IF NOT EXISTS corpchanges
		(
		  cid bigint NOT NULL default 0,
		  did bigint NOT NULL default 0,
		  wid bigint NOT NULL default 0,
		  type smallint NOT NULL default 0,
		  stamp bigint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
`

const createCorpsTableSql = `
CREATE TABLE IF NOT EXISTS corps
		(
		  CID serial NOT NULL,
		  FullName character varying(200) NOT NULL unique,
		  Status smallint NOT NULL default 1,
		  ShortName  character varying(50) NOT NULL default '',
		  LogoFile	character varying(200) default '',
		  HomePage character varying(200) default '',
		  Location character varying(12) default '11|01|01',
		  Addr character varying(200) default '',
		  MaxWorkerAllow int NOT NULL default 100,
		  OwnerUid bigint NOT NULL default 0,
		  CreateStamp bigint NOT NULL default 0,
		  UpdateStamp bigint NOT NULL default 0,
		  CONSTRAINT corps_pkey PRIMARY KEY (CID)
		)
		WITH (OIDS=FALSE);
`

func GetCorpsOfUser(uid int64) (corps []Corp, workers []Worker, depts []Dept, err error) {
	cids, err := GetCidsOfUid(uid)
	if len(cids) > 0 {
		corps = make([]Corp, 0, len(cids))
		workers = make([]Worker, 0, 500)
		depts = make([]Dept, 0, 50)
	} else {
		return
	}
	for _, cid := range cids {
		corp, err := GetCorp(cid)
		if err != nil {
			return corps, workers, depts, err
		}
		corps = append(corps, corp)
		ws, err := GetCorpWorkers(cid)
		if err != nil {
			return corps, workers, depts, err
		}
		if len(ws) > 0 {
			workers = append(workers, ws...)
		}
		ds, err := GetCorpDepts(cid)
		if err != nil {
			return corps, workers, depts, err
		}
		if len(ds) > 0 {
			depts = append(depts, ds...)
		}
	}
	return
}

func GetColleaguesOfUid(uid int64) (uids []int64, err error) {
	cids, err := GetCidsOfUid(uid)
	if err != nil {
		return
	}
	uids = make([]int64, 0, 500)
	for _, cid := range cids {
		tempUids, err := GetUidsOfCorp(cid)
		if err != nil {
			continue
		}
		uids = append(uids, tempUids...)
	}
	return
}

func GetCorpTreesOfCids(cids []int64) (workers []Worker, depts []Dept, err error) {
	workers = make([]Worker, 0, 500)
	depts = make([]Dept, 0, 50)
	for _, cid := range cids {
		ws, err := GetCorpWorkers(cid)
		if err != nil {
			return workers, depts, err
		}
		if len(ws) > 0 {
			workers = append(workers, ws...)
		}
		ds, err := GetCorpDepts(cid)
		if err != nil {
			return workers, depts, err
		}
		if len(ds) > 0 {
			depts = append(depts, ds...)
		}
	}
	return
}

func IsCorpExist(fullname string) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numcorps FROM corps where FullName = @fullname;
	`
	fullnameParam := pgsql.NewParameter("@fullname", pgsql.Text)
	err = fullnameParam.SetValue(fullname)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, fullnameParam)
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

func IsCidExist(cid int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numcorps FROM corps where cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam)
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

func CorpNumOfUid(uid int64) (n int16, err error) {
	n = 0
	command := `
	SELECT COUNT(*) AS numcorps FROM corps where OwnerUid = @owneruid;
	`
	owneruidParam := pgsql.NewParameter("@owneruid", pgsql.Bigint)
	err = owneruidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, owneruidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
				n = 0
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetCorp(cid int64) (corp Corp, err error) {
	command := `
	SELECT * FROM corps where cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&corp.Cid, &corp.FullName, &corp.Status, &corp.ShortName, &corp.LogoFile, &corp.HomePage, &corp.Location, &corp.Addr, &corp.MaxWorkerAllow, &corp.OwnerUid, &corp.CreateStamp, &corp.UpdateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func updateCorp(corp Corp) (stamp int64, err error) {
	command := `
	update corps set FullName=@fullname, status=@status,ShortName=@shortname,HomePage=@homepage,Location=@location,Addr=@addr,UpdateStamp=@updatestamp where cid=@cid;
	`
	fullnameParam := pgsql.NewParameter("@fullname", pgsql.Text)
	err = fullnameParam.SetValue(corp.FullName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(corp.Status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	shortnameParam := pgsql.NewParameter("@shortname", pgsql.Text)
	err = shortnameParam.SetValue(corp.ShortName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	homepageParam := pgsql.NewParameter("@homepage", pgsql.Text)
	err = homepageParam.SetValue(corp.HomePage)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	locationParam := pgsql.NewParameter("@location", pgsql.Text)
	err = locationParam.SetValue(corp.Location)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	addrParam := pgsql.NewParameter("@addr", pgsql.Text)
	err = addrParam.SetValue(corp.Addr)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stamp = time.Now().UnixNano()
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(corp.Cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, fullnameParam, statusParam, shortnameParam, homepageParam, locationParam, addrParam, updatestampParam, cidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetCorp(corp Corp) {
	exist, err := IsCidExist(corp.Cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		logs.Logger.Critical("corp not exist")
		return
	}
	stamp, err := updateCorp(corp)
	if err != nil {
		return
	}
	var change CorpChangedNotification
	change.Cid = corp.Cid
	change.Type = CorpChangedType_Updated
	CreateCorpChange(change, stamp)
}

func deleteCorp(cid int64) (err error) {
	command := `
	delete from corps where cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, cidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func RemoveCorp(reqPkt RemoveCorpReqPkt) (code int8) {
	exist, err := IsCidExist(reqPkt.Cid)
	if err != nil {
		code = RemoveCorpCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	} else if !exist {
		code = RemoveCorpCode_CorpNotExist
		logs.Logger.Critical("corp with cid = ", reqPkt.Cid, " is not exist")
		return
	}
	err = RemoveAllWorkersOfCorp(reqPkt.Cid)
	if err != nil {
		code = RemoveCorpCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	}
	err = RemoveAllDeptsOfCorp(reqPkt.Cid)
	if err != nil {
		code = RemoveCorpCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	}
	err = deleteCorp(reqPkt.Cid)
	if err != nil {
		return
	}
	var change CorpChangedNotification
	change.Cid = reqPkt.Cid
	change.Type = CorpChangedType_Removed
	CreateCorpChange(change, time.Now().UnixNano())
	return RemoveCorpCode_None
}

func insertCorp(fullname, shortname, homepage, location, addr string, ownerUid int64) (cid int64, err error) {
	command := `
	INSERT INTO corps(FullName,status,ShortName,HomePage,Location,Addr,OwnerUid,CreateStamp,UpdateStamp) 
		VALUES(@fullname,@status,@shortname,@homepage,@location,@addr,@owneruid,@createstamp,@updatestamp) RETURNING cid;
	`
	fullnameParam := pgsql.NewParameter("@fullname", pgsql.Text)
	err = fullnameParam.SetValue(fullname)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(CorpStatus_Created)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	shortnameParam := pgsql.NewParameter("@shortname", pgsql.Text)
	err = shortnameParam.SetValue(shortname)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	homepageParam := pgsql.NewParameter("@homepage", pgsql.Text)
	err = homepageParam.SetValue(homepage)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	locationParam := pgsql.NewParameter("@location", pgsql.Text)
	err = locationParam.SetValue(location)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	addrParam := pgsql.NewParameter("@addr", pgsql.Text)
	err = addrParam.SetValue(addr)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	owneruidParam := pgsql.NewParameter("@owneruid", pgsql.Bigint)
	err = owneruidParam.SetValue(ownerUid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	stamp := time.Now().UnixNano()
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
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
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, fullnameParam, statusParam, shortnameParam, homepageParam, locationParam, addrParam, owneruidParam, createstampParam, updatestampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&cid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateCorp(reqPkt CreateCorpReqPkt) (cid int64, code int8) {
	cid = 0
	code = CreateCorpCode_DatabaseErr
	if !IsCorpFullnameValid(reqPkt.FullName) {
		code = CreateCorpCode_InvalidCorpFullName
		logs.Logger.Critical("invalid fullname: ", reqPkt.FullName)
		return
	}
	exist, err := IsCorpExist(reqPkt.FullName)
	if err != nil {
		logs.Logger.Critical("check if corp is exist error= ", err)
		return
	}
	if exist {
		code = CreateCorpCode_CorpExist
		logs.Logger.Critical("corp with fullname ", reqPkt.FullName, " already exist")
		return
	}
	exist, err = users.IsUidExist(reqPkt.OwnerUid)
	if err != nil {
		logs.Logger.Critical("check if uid exist error = ", err)
		return
	}
	if !exist {
		code = CreateCorpCode_UidNotExist
		logs.Logger.Critical("uid not exist")
		return
	}
	maxCorpAllow, err := users.MaxCorpAllow(reqPkt.OwnerUid)
	if err != nil {
		logs.Logger.Critical("check if allow create corp error = ", err)
		return
	}
	corpNumOfUid, err := CorpNumOfUid(reqPkt.OwnerUid)
	if err != nil {
		logs.Logger.Critical("CreateCorp error: ", err)
		return
	}
	if corpNumOfUid >= maxCorpAllow {
		code = CreateCorpCode_UserNotAllowCreateCorpMore
		logs.Logger.Critical("User not allow create corp more")
		return
	}
	cid, err = insertCorp(reqPkt.FullName, reqPkt.ShortName, reqPkt.HomePage, reqPkt.Location, reqPkt.Addr, reqPkt.OwnerUid)
	if err != nil {
		code = CreateCorpCode_DatabaseErr
		return
	}
	code = CreateCorpCode_None
	return
}

func GetCorpUpdateStamp(cid int64) (stamp int64) {
	command := `
	SELECT updatestamp FROM corps where cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err := cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&stamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func SetCorpUpdateStamp(cid, stamp int64) {
	command := `
	update corps set updatestamp = @updatestamp where cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err := cidParam.SetValue(cid)
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
		_, err := conn.Execute(command, updatestampParam, cidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func GetCorpChanges(cid, stamp int64) (changes []CorpChangedNotification, lastStamp int64) {
	command := `
	SELECT did, wid, type, stamp from corpchanges where cid = @cid AND stamp > @stamp ORDER BY stamp ASC;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err := cidParam.SetValue(cid)
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
		res, err := conn.Query(command, cidParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			lastStamp = 0
			changes = make([]CorpChangedNotification, 0, 5)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var change CorpChangedNotification
					change.Cid = cid
					var tempStamp int64
					err = res.Scan(&change.Did, &change.Wid, &change.Type, &tempStamp)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					if tempStamp > lastStamp {
						lastStamp = tempStamp
					}
					changes = append(changes, change)
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

func IsCorpChangeExist(change CorpChangedNotification) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS num FROM corpchanges WHERE cid = @cid AND did = @did AND wid = @wid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(change.Cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(change.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(change.Wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam, didParam, widParam)
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

func insertCorpChange(change CorpChangedNotification, stamp int64) (err error) {
	command := `
	INSERT INTO corpchanges(cid,did,wid,type,stamp) 
			VALUES(@cid,@did,@wid,@type,@stamp);
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(change.Cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(change.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(change.Wid)
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
		_, err := conn.Execute(command, cidParam, didParam, widParam, typeParam, stampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateCorpChange(change CorpChangedNotification, stamp int64) (err error) {
	command := `
	update corpchanges set type=@type, stamp=@stamp where cid=@cid AND did=@did AND wid=@wid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(change.Cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(change.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(change.Wid)
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
		_, err := conn.Execute(command, typeParam, stampParam, cidParam, didParam, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func CreateCorpChange(change CorpChangedNotification, stamp int64) {
	if !IsCidValid(change.Cid) {
		logs.Logger.Critical("cid is invalid")
		return
	}

	exist, err := IsCorpChangeExist(change)
	if err != nil {
		return
	}
	if exist {
		err = updateCorpChange(change, stamp)
	} else {
		err = insertCorpChange(change, stamp)
	}

	if err != nil {
		change.Stamp = stamp
		CorpChangedNotificationChan <- change
	}
	return
}

func IsCorpFullnameValid(fullname string) (valid bool) {
	if len(fullname) < 6 {
		return false
	}
	return true
}

func IsCidValid(cid int64) (valid bool) {
	if cid <= 0 {
		return false
	}
	return true
}

func IsDidValid(did int64) (valid bool) {
	if did <= 0 {
		return false
	}
	return true
}

func IsWidValid(wid int64) (valid bool) {
	if wid <= 0 {
		return false
	}
	return true
}
