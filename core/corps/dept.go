package corps

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
	"time"
)

const (
	DeptStatus_None int16 = iota
	DeptStatus_Normal
	DeptStatus_Frozen
)

const (
	CreateDeptCode_None int8 = iota
	CreateDeptCode_InvalidReq
	CreateDeptCode_CorpNotExist
	CreateDeptCode_DeptExist
	CreateDeptCode_InvalidDeptName
	CreateDeptCode_ParentDeptNotExist
	CreateDeptCode_DatabaseErr
)

const (
	RemoveDeptCode_None int8 = iota
	RemoveDeptCode_InvalidReq
	RemoveDeptCOde_DeptNotExist
	RemoveDeptCode_RemoveWorkersErr
	RemoveDeptCode_DatabaseErr
)

const CreateDeptsTableSql = `
CREATE TABLE IF NOT EXISTS depts
		(
		  did serial NOT NULL,
		  Name character varying(200) NOT NULL,
		  Status smallint NOT NULL default 0,
		  Cid bigint NOT NULL default 0,
		  pdid bigint NOT NULL default 0,
		  CreateStamp bigint NOT NULL default 0,
		  UpdateStamp bigint NOT NULL default 0,
		  CONSTRAINT depts_pkey PRIMARY KEY (did)
		)
		WITH (OIDS=FALSE);
`

type Dept struct {
	Did         int64  `json:"id"`
	Name        string `json:"fn"`
	Status      int16  `json:"st"`
	Cid         int64  `json:"cid,omitempty"`
	Pdid        int64  `json:"pdid, omitempty"`
	CreateStamp int64  `json:"cs,omitempty"`
	UpdateStamp int64  `json:"us,omitempty"`
}

type CreateDeptReqPkt struct {
	Name string `json:"name"`
	Cid  int64  `json:"cid"`
	Pdid int64  `json:"parentDid"`
}

type CreateDeptResPkt struct {
	Code int8  `json:"code"`
	Did  int64 `json:"did"`
}

type RemoveDeptReqPkt struct {
	Did int64 `json:"did"`
}

type RemoveDeptResPkt struct {
	Code int8 `json:"code"`
}

type GetDeptReqPkt struct {
	Did int64 `json:"did"`
}

func IsDidExist(did int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numdepts FROM depts where did = @did;
	`
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
		res, err := conn.Query(command, didParam)
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

func IsSameDeptExist(name string, cid int64, pdid int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numdepts FROM depts where cid = @cid AND name = @name AND pdid = @pdid;
	`
	cidParam := pgsql.NewParameter("@cid", pgsql.Bigint)
	err = cidParam.SetValue(cid)
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

	pdidParam := pgsql.NewParameter("@pdid", pgsql.Bigint)
	err = pdidParam.SetValue(pdid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, cidParam, nameParam, pdidParam)
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

func GetDept(did int64) (dept Dept, err error) {
	command := `
	SELECT * FROM depts where did = @did;
	`
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
		res, err := conn.Query(command, didParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&dept.Did, &dept.Name, &dept.Status, &dept.Cid, &dept.Pdid, &dept.CreateStamp, &dept.UpdateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func updateDept(dept Dept) (stamp int64, err error) {
	command := `
	update depts set Name=@name,status=@status,pdid=@pdid,UpdateStamp=@updatestamp where did=@did;
	`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(dept.Name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(dept.Status)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	pdidParam := pgsql.NewParameter("@pdid", pgsql.Bigint)
	err = pdidParam.SetValue(dept.Pdid)
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
	err = didParam.SetValue(dept.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, nameParam, statusParam, pdidParam, updatestampParam, didParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetDept(dept Dept) {
	exist, err := IsDidExist(dept.Did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		logs.Logger.Critical("dept not exist")
		return
	}
	stamp, err := updateDept(dept)
	if err != nil {
		return
	}
	SetCorpUpdateStamp(dept.Cid, stamp)
	var change CorpChangedNotification
	change.Cid = dept.Cid
	change.Did = dept.Did
	change.Type = CorpChangedType_Updated
	CreateCorpChange(change, stamp)
}

func GetDeptPdid(did int64) (pdid int64, err error) {
	command := `
	SELECT pdid FROM depts where did = @did;
	`
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
		res, err := conn.Query(command, didParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&pdid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsDidOfCorpExist(cid, did int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS numdepts FROM depts where did = @did AND cid = @cid;
	`
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
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, didParam, cidParam)
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

func RemoveDept(reqPkt RemoveDeptReqPkt) (code int8) {
	return RemoveDeptWithDid(reqPkt.Did)
}

func deleteDept(did int64) (err error) {
	command := `
	delete from depts where did = @did;
	`
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
		_, err := conn.Execute(command, didParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func RemoveDeptWithDid(did int64) (code int8) {
	code = RemoveDeptCode_DatabaseErr
	exist, err := IsDidExist(did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		code = RemoveDeptCOde_DeptNotExist
		return
	}
	_, err = removeWokerOfDept(did)
	if err != nil {
		return RemoveDeptCode_RemoveWorkersErr
	}

	childDids, err := GetChildDepts(did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	for _, cdid := range childDids {
		code = RemoveDeptWithDid(cdid)
		if code != RemoveDeptCode_None {
			return
		}
	}
	stamp := time.Now().UnixNano()
	cid := GetCidOfDid(did)
	if cid == 0 {
		code = RemoveDeptCode_DatabaseErr
		return
	}
	err = deleteDept(did)
	if err != nil {
		code = RemoveDeptCode_DatabaseErr
		return
	}
	SetCorpUpdateStamp(cid, stamp)
	var change CorpChangedNotification
	change.Cid = cid
	change.Did = did
	change.Type = CorpChangedType_Removed
	CreateCorpChange(change, stamp)
	code = RemoveDeptCode_None
	return
}

func GetCidOfDid(did int64) (cid int64) {
	command := `
	SELECT cid FROM depts where did = @did;
	`
	didParam := pgsql.NewParameter("@did", pgsql.Bigint)
	err := didParam.SetValue(did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, didParam)
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

func GetChildDepts(did int64) (childDids []int64, err error) {
	command := `
	SELECT did FROM depts where pdid = @pdid;
	`
	pdidParam := pgsql.NewParameter("@pdid", pgsql.Bigint)
	err = pdidParam.SetValue(did)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, pdidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			childDids = make([]int64, 0, 10)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var cdid int64
					err = res.Scan(&cdid)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					childDids = append(childDids, cdid)
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

func GetCorpDepts(cid int64) (depts []Dept, err error) {
	command := `
	SELECT * FROM depts where Cid = @cid;
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
			depts = make([]Dept, 0, 20)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var dept Dept
					err = res.Scan(&dept.Did, &dept.Name, &dept.Status, &dept.Cid, &dept.Pdid, &dept.CreateStamp, &dept.UpdateStamp)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					dept.CreateStamp = 0
					depts = append(depts, dept)
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

func GetCorpDids(cid int64) (dids []int64, err error) {
	command := `
	SELECT did FROM depts where Cid = @cid;
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
			dids = make([]int64, 0, 20)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var did int64
					err = res.Scan(&did)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					}
					dids = append(dids, did)
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

func RemoveAllDeptsOfCorp(cid int64) (err error) {
	command := `
	delete from depts where Cid = @cid;
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

func insertDept(name string, cid, pdid int64) (did, stamp int64, err error) {
	command := `
	INSERT INTO depts(Name,status,Cid,pdid,CreateStamp,UpdateStamp) 
		VALUES(@name,@status,@cid,@pdid,@createstamp,@updatestamp) RETURNING did;
	`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	statusParam := pgsql.NewParameter("@status", pgsql.Smallint)
	err = statusParam.SetValue(DeptStatus_Normal)
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
	pdidParam := pgsql.NewParameter("@pdid", pgsql.Bigint)
	err = pdidParam.SetValue(pdid)
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
		res, err := conn.Query(command, nameParam, statusParam, cidParam, pdidParam, createstampParam, updatestampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&did)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func CreateDept(reqPkt CreateDeptReqPkt) (did int64, code int8) {
	did = 0
	code = CreateDeptCode_DatabaseErr
	if !IsDeptNameValid(reqPkt.Name) {
		code = CreateDeptCode_InvalidDeptName
		logs.Logger.Critical("invalid name: ", reqPkt.Name)
		return
	}

	exist, err := IsCidExist(reqPkt.Cid)
	if err != nil {
		logs.Logger.Critical("check if corp is exist error: ", err)

		return
	}
	if !exist {
		code = CreateDeptCode_CorpNotExist
		logs.Logger.Critical("corp with cid: ", reqPkt.Cid, " not exist")
		return
	}
	if IsDidValid(reqPkt.Pdid) {
		exist, err = IsDidExist(reqPkt.Pdid)
		if err != nil {
			logs.Logger.Critical("check if parent dept is exist error: ", err)
			return
		}
		if exist == false {
			code = CreateDeptCode_ParentDeptNotExist
			logs.Logger.Critical("parent dept: ", reqPkt.Pdid, " cid: ", reqPkt.Cid, " not exist")
			return
		}
	}

	exist, err = IsSameDeptExist(reqPkt.Name, reqPkt.Cid, reqPkt.Pdid)
	if err != nil {
		logs.Logger.Critical("check if dept is exist error: ", err)
		return
	}
	if exist {
		code = CreateDeptCode_DeptExist
		logs.Logger.Critical("dept with name: ", reqPkt.Name, " cid: ", reqPkt.Cid, " pdid: ", reqPkt.Pdid, "already exist")
		return
	}
	did, stamp, err := insertDept(reqPkt.Name, reqPkt.Cid, reqPkt.Pdid)
	if err != nil {
		code = CreateDeptCode_DatabaseErr
		return
	}
	SetCorpUpdateStamp(reqPkt.Cid, stamp)
	var change CorpChangedNotification
	change.Cid = reqPkt.Cid
	change.Did = did
	change.Type = CorpChangedType_Create
	CreateCorpChange(change, stamp)
	code = CreateDeptCode_None
	return
}

func IsDeptNameValid(name string) (valid bool) {
	if len(name) <= 1 {
		return false
	}
	return true
}
