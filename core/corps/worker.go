package corps

import (
	"encoding/base64"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
	"strconv"
	"strings"
	"time"
)

const (
	WorkerPermission_None int16 = iota
	WorkerPermission_Normal
	WorkerPermission_DeptAdmin
	WorkerPermission_CorpAdmin
	WorkerPermission_CorpOwner
)

const (
	WorkerStatus_None int16 = iota
	WorkerStatus_Normal
	WorkerStatus_Frozen
	WorkerStatus_Removed
)

const (
	CreateWorkerCode_None int8 = iota
	CreateWorkerCode_InvalidReq
	CreateWorkerCode_InvalidName
	CreateWorkerCode_CorpNotExist
	CreateWorkerCode_CorpNotAllowWorkerMore
	CreateWorkerCode_DeptNotExist
	CreateWorkerCode_DatabaseErr
)

const (
	RemoveWorkerCode_None int8 = iota
	RemoveWorkerCode_InvalidReq
	RemoveWorkerCode_WorkerNotExist
	RemoveWorkerCode_DatabaseErr
)

const (
	BindWorkerUserCode_None int8 = iota
	BindWorkerUserCode_InvalidCode
	BindWorkerUserCode_InvalidReq
	BindWorkerUserCode_WorkerNotExist
	BindWorkerUserCode_DeptNotExist
	BindWorkerUserCode_CorpNotExist
	BindWorkerUserCode_UserNotExist
	BindWorkerUserCode_DatabaseErr
)

const CreateWorkersTableSql = `
CREATE TABLE IF NOT EXISTS workers
		(
		  wid serial NOT NULL,
		  Name character varying(50) NOT NULL,
		  Status smallint NOT NULL default 1,
		  Cid bigint NOT NULL default 0,
		  did bigint NOT NULL default 0,
		  Permission smallint NOT NULL default 1,
		  Post character varying(50) default '',
		  Mobile character varying(20) default '',
		  Tel   character varying(29) default '',
		  Uid bigint NOT NULL default 0,
		  CreateStamp bigint NOT NULL default 0,
		  UpdateStamp bigint NOT NULL default 0,
		  CONSTRAINT workers_pkey PRIMARY KEY (wid)
		)
		WITH (OIDS=FALSE);
		`

type Worker struct {
	Wid         int64  `json:"id"`
	Name        string `json:"fn"`
	Status      int16  `json:"st"`
	Cid         int64  `json:"cid"`
	Did         int64  `json:"pdid,omitempty"`
	Permission  int16  `json:"pm"`
	Post        string `json:"pt,omitempty"`
	Mobile      string `json:"mb,omitempty"`
	Tel         string `json:"tel,omitempty"`
	Uid         int64  `json:"uid,omitempty"`
	UserAccount string `json:"ua,omitempty"`
	//BaseInfo    users.UserBaseInfo `json:"bi,omitempty"`
	CreateStamp int64 `json:"cs,omitempty"`
	UpdateStamp int64 `json:"us,omitempty"`
}

type CreateWorkerReqPkt struct {
	Name       string `json:"name"`
	Cid        int64  `json:"cid"`
	Permission int16  `json:"permission"`
	Post       string `json:"post,omitempty"`
	Mobile     string `json:"mobile,omitempty"`
	Tel        string `json:"tel,omitempty"`
	Uid        int64  `json:"uid,omitempty"`
	Did        int64  `json:"did,omitempty"`
}

type CreateWorkerResPkt struct {
	Wid  int64 `json:"wid"`
	Code int8  `json:"code"`
}

type RemoveWorkerReqPkt struct {
	Wid int64 `json:"wid"`
}

type RemoveWorkerResPkt struct {
	Code int8 `json:"code"`
}

type BindWorkerUserReqPkt struct {
	Uid            int64  `json:"uid"`
	InvitationCode string `json:"code"`
}

type BindWorkerUserResPkt struct {
	Code int8 `json:"code"`
}

type GetWorkerReqPkt struct {
	Wid int64 `json:"wid"`
}

func IsWidExist(wid int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numworkers FROM workers where wid = @wid;
	`
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, widParam)
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

func IsWidOfCorpExist(cid, wid int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numworkers FROM workers where wid = @wid AND cid = @cid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, widParam, cidParam)
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

func GetWorker(wid int64) (worker Worker, err error) {
	command := `
	SELECT * FROM workers where wid = @wid;
	`
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&worker.Wid, &worker.Name, &worker.Status, &worker.Cid, &worker.Did, &worker.Permission, &worker.Post, &worker.Mobile, &worker.Tel, &worker.Uid, &worker.CreateStamp, &worker.UpdateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
			if users.IsUidValid(worker.Uid) {
				worker.UserAccount, err = users.GetUserAccount(worker.Uid)
				if err != nil {
					logs.Logger.Critical("GetCorpWorkers getuser account error:", err)
				}
			} else {
				str := fmt.Sprintf("%d/%d/%d", worker.Cid, worker.Did, worker.Wid)
				worker.UserAccount = base64.StdEncoding.EncodeToString([]byte(str))
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func updateWorker(worker Worker) (stamp int64, err error) {
	command := `
	update workers set Name=@name,status=@status,did=@did,Permission=@permission, post=@post, mobile=@mobile, tel=@tel,UpdateStamp=@updatestamp where wid=@wid;
	`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(worker.Name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(worker.Status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(worker.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	permissionParam := pgsql.NewParameter("@permission", pgsql.Smallint)
	err = permissionParam.SetValue(worker.Permission)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	postParam := pgsql.NewParameter("@post", pgsql.Text)
	err = postParam.SetValue(worker.Post)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	mobileParam := pgsql.NewParameter("@mobile", pgsql.Text)
	err = mobileParam.SetValue(worker.Mobile)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	telParam := pgsql.NewParameter("@tel", pgsql.Text)
	err = telParam.SetValue(worker.Tel)
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
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(worker.Wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, nameParam, statusParam, didParam, permissionParam, postParam, mobileParam, telParam, updatestampParam, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetWorker(worker Worker) {
	exist, err := IsWidExist(worker.Wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		logs.Logger.Critical("dept not exist")
		return
	}
	stamp, err := updateWorker(worker)
	if err != nil {
		return
	}
	SetCorpUpdateStamp(worker.Cid, stamp)
	var change CorpChangedNotification
	change.Cid = worker.Cid
	change.Wid = worker.Wid
	change.Type = CorpChangedType_Updated
	CreateCorpChange(change, stamp)
}

func GetCidsOfUid(uid int64) (cids []int64, err error) {
	command := `
	SELECT Cid FROM workers where uid = @uid;
	`
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
		res, err := conn.Query(command, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			cids = make([]int64, 0, 1)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var cid int64
					err = res.Scan(&cid)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					if IsCidValid(cid) {
						cids = append(cids, cid)
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

func GetUidsOfCorp(cid int64) (uids []int64, err error) {
	command := `
	SELECT uid FROM workers where cid = @cid;
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
		} else {
			uids = make([]int64, 0, 50)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var uid int64
					err = res.Scan(&uid)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					if users.IsUidValid(uid) {
						uids = append(uids, uid)
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

func GetCorpWorkerCount(cid int64) (n int32, err error) {
	n = 0
	command := `
	SELECT COUNT(*) AS workernum FROM workers where Cid = @cid AND Status = @status;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(WorkerStatus_Normal)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam, statusParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetCorpWorkers(cid int64) (workers []Worker, err error) {
	command := `
	SELECT * FROM workers where cid = @cid;
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
		} else {
			workers = make([]Worker, 0, 50)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var worker Worker
					err = res.Scan(&worker.Wid, &worker.Name, &worker.Status, &worker.Cid, &worker.Did, &worker.Permission, &worker.Post, &worker.Mobile, &worker.Tel, &worker.Uid, &worker.CreateStamp, &worker.UpdateStamp)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					worker.CreateStamp = 0
					if users.IsUidValid(worker.Uid) {
						worker.UserAccount, err = users.GetUserAccount(worker.Uid)
						if err != nil {
							logs.Logger.Critical("GetCorpWorkers getuser account error:", err)
						}
					} else {
						str := fmt.Sprintf("%d/%d/%d", worker.Cid, worker.Did, worker.Wid)
						worker.UserAccount = base64.StdEncoding.EncodeToString([]byte(str))
					}
					workers = append(workers, worker)
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

func removeWokerOfDept(did int64) (stamp int64, err error) {
	command := `
	update workers set did=0,updatestamp=@updatestamp where did = @did;
	`
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(did)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, updatestampParam, didParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateWorkerStatus(wid int64, status int16) (stamp int64, err error) {
	command := `
	update workers set status=@status,updatestamp=@updatestamp where wid = @wid;
	`
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
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

	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, statusParam, updatestampParam, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func RemoveWorker(reqPkt RemoveWorkerReqPkt) (code int8) {
	code = RemoveWorkerCode_DatabaseErr
	stamp, err := updateWorkerStatus(reqPkt.Wid, WorkerStatus_Removed)
	if err != nil {
		return
	}
	cid := GetCidOfWid(reqPkt.Wid)
	SetCorpUpdateStamp(cid, stamp)
	var change CorpChangedNotification
	change.Cid = cid
	change.Wid = reqPkt.Wid
	change.Type = CorpChangedType_Removed
	CreateCorpChange(change, stamp)
	code = RemoveWorkerCode_None
	return
}

func RemoveAllWorkersOfCorp(cid int64) (err error) {
	command := `
	delete from workers where cid=@cid;
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

func updateWorkerDid(wid, did int64) (stamp int64, err error) {
	command := `
	update workers set did=@did,updatestamp=@updatestamp where wid = @wid;
	`
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
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

	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, didParam, updatestampParam, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func BindWorkerUser(reqPkt BindWorkerUserReqPkt) (code int8) {
	code = BindWorkerUserCode_DatabaseErr
	var cid, did, wid int64
	data, err := base64.StdEncoding.DecodeString(reqPkt.InvitationCode)
	if err != nil {
		logs.Logger.Critical("invitation code base64 decode error: ", err)
		return BindWorkerUserCode_InvalidCode
	} else {
		strs := strings.Split(string(data), "/")
		if len(strs) != 3 {
			logs.Logger.Critical("invitation code invalid: ", string(data))
			return BindWorkerUserCode_InvalidCode
		}
		cid, err = strconv.ParseInt(strs[0], 10, 64)
		if err != nil {
			logs.Logger.Critical("invitation code cid invalid: ", strs[0], " error: ", err)
			return BindWorkerUserCode_InvalidCode
		}
		did, err = strconv.ParseInt(strs[1], 10, 64)
		if err != nil {
			logs.Logger.Critical("invitation code did invalid: ", strs[1], " error: ", err)
			return BindWorkerUserCode_InvalidCode
		}
		wid, err = strconv.ParseInt(strs[2], 10, 64)
		if err != nil {
			logs.Logger.Critical("invitation code wid invalid: ", strs[2], " error: ", err)
			return BindWorkerUserCode_InvalidCode
		}

	}
	exist, err := IsWidExist(wid)
	if err != nil {
		logs.Logger.Critical("check if wid is exist error: ", err)
		return
	} else if !exist {
		code = BindWorkerUserCode_WorkerNotExist
		logs.Logger.Critical("worker not exist")
		return
	}
	exist, err = users.IsUidExist(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical("check if uid exist error: ", err)
		return
	} else if !exist {
		logs.Logger.Critical("user not exist")
		return
	}
	exist, err = IsCidExist(cid)
	if err != nil {
		logs.Logger.Critical("check if cid exist error: ", err)
		return
	} else if !exist {
		code = BindWorkerUserCode_CorpNotExist
		logs.Logger.Critical("corp not exist")
		return
	}
	if IsDidValid(did) {
		exist, err = IsDidExist(did)
		if err != nil {
			logs.Logger.Critical("check if did exist error: ", err)
			return
		} else if !exist {
			logs.Logger.Critical("dept not exist")
			code = BindWorkerUserCode_DeptNotExist
			return
		}
	}

	command := `
	update workers set uid=@uid,updatestamp=@updatestamp where wid = @wid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(reqPkt.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err = widParam.SetValue(wid)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, updatestampParam, widParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	if err != nil {
		code = BindWorkerUserCode_DatabaseErr
		return
	}

	SetCorpUpdateStamp(cid, stamp)
	var change CorpChangedNotification
	change.Cid = cid
	change.Wid = wid
	change.Type = CorpChangedType_Updated
	CreateCorpChange(change, stamp)
	code = BindWorkerUserCode_None
	return
}

func GetCidOfWid(wid int64) (cid int64) {
	command := `
	SELECT cid FROM workers where wid = @wid;
	`
	widParam := pgsql.NewParameter("@wid", pgsql.Bigint)
	err := widParam.SetValue(wid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, widParam)
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

func insertWorker(name, post, mobile, tel string, cid, did, uid int64, permission int16) (wid, stamp int64, err error) {
	command := `
	INSERT INTO workers(Name,status,Cid,did,Permission,Post,Mobile,Tel,Uid,CreateStamp,UpdateStamp) 
		VALUES(@name,@status,@cid,@did,@permission,@post,@mobile,@tel,@uid,@createstamp,@updatestamp) RETURNING wid;
	`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(WorkerStatus_Normal)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err = didParam.SetValue(did)
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
	postParam := pgsql.NewParameter("@post", pgsql.Text)
	err = postParam.SetValue(post)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	mobileParam := pgsql.NewParameter("@mobile", pgsql.Text)
	err = mobileParam.SetValue(mobile)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	telParam := pgsql.NewParameter("@tel", pgsql.Text)
	err = telParam.SetValue(tel)
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
	stamp = time.Now().UnixNano()
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
		res, err := conn.Query(command, nameParam, statusParam, cidParam, didParam, permissionParam, postParam, mobileParam, telParam, uidParam, createstampParam, updatestampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&wid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateWorker(reqPkt CreateWorkerReqPkt) (wid int64, code int8) {
	if len(reqPkt.Name) <= 0 {
		code = CreateWorkerCode_InvalidName
		logs.Logger.Critical("Invalid worker name: ", reqPkt.Name)
		return
	}
	wid = 0
	code = CreateWorkerCode_DatabaseErr
	exist, err := IsCidExist(reqPkt.Cid)
	if err != nil {
		logs.Logger.Critical("check if corp is exist error: ", err)
		return
	}
	if !exist {
		code = CreateWorkerCode_CorpNotExist
		logs.Logger.Critical("corp with cid: ", reqPkt.Cid, " not exist")
		return
	}
	corp, err := GetCorp(reqPkt.Cid)
	if err != nil {
		logs.Logger.Critical("get corp error: ", err)
		return
	}
	workerNumNow, err := GetCorpWorkerCount(reqPkt.Cid)
	if err != nil {
		logs.Logger.Critical("get worker count error: ", err)
		return
	}
	if workerNumNow >= corp.MaxWorkerAllow {
		code = CreateWorkerCode_CorpNotAllowWorkerMore
		logs.Logger.Critical("no more worker can be create of corp")
		return
	}
	if IsDidValid(reqPkt.Did) {
		exist, err = IsDidExist(reqPkt.Did)
		if err != nil {
			logs.Logger.Critical("check if dept is exist error: ", err)
			return
		}
		if !exist {
			code = CreateWorkerCode_DeptNotExist
			logs.Logger.Critical("corp with did: ", reqPkt.Did, " not exist")
			return
		}
	}

	wid, stamp, err := insertWorker(reqPkt.Name, reqPkt.Post, reqPkt.Mobile, reqPkt.Tel, reqPkt.Cid, reqPkt.Did, reqPkt.Uid, reqPkt.Permission)
	if err != nil {
		return
	}
	SetCorpUpdateStamp(reqPkt.Cid, stamp)
	var change CorpChangedNotification
	change.Cid = reqPkt.Cid
	change.Wid = wid
	change.Type = CorpChangedType_Create
	CreateCorpChange(change, stamp)
	code = CreateWorkerCode_None
	return
}
