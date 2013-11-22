package files

import (
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/core/users"
	"hug/logs"
	"time"
)

const createFileTableSql = `
CREATE TABLE IF NOT EXISTS files
		(
		  fid serial NOT NULL unique,
		  uid bigint NOT NULL,
		  name character varying(100) default '',
		  path character varying(300) default '',
		  stamp bigint NOT NULL,
		  CONSTRAINT file_pkey PRIMARY KEY (fid)
		)
		WITH (OIDS=FALSE);
		`

func CreateFile(uid int64, name string, path string) {
	defer func() {
		if err := recover(); err != nil {
			logs.Logger.Critical(err)
		}
	}()
	command := `
		INSERT INTO file(uid,name,path,stamp) 
		VALUES(@uid, @name, @path, @stamp) RETURNING fid;
		`

	fid := 0
	exitst, _ := users.IsUidExist(uid)
	if !exitst {
		logs.Logger.Warnf("offline file uid not exist uid =%v", uid)
		return
	}
	if len(name) == 0 {
		logs.Logger.Critical(fmt.Sprintln("offline file name is empty"))
		return
	}
	if len(path) == 0 {
		logs.Logger.Critical(fmt.Sprintln("offline file path is empty"))
		return
	}

	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
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
	pathParam := pgsql.NewParameter("@path", pgsql.Text)
	err = pathParam.SetValue(path)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(time.Now().Unix())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, uidParam, nameParam, pathParam, stampParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&fid)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	res.Close()

	pool.Release(conn)
	return
}

func IsFileExists(name string) (exists bool, err error) {
	exists = false
	n := 0
	command := `
		SELECT COUNT(*) AS numfiles FROM files where name = @name;
		`
	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err = nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(command, nameParam)
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
		exists = true
	} else {
		exists = false
	}
	return
}

func UpdateFileStamp(name string) {
	defer func() {
		if err := recover(); err != nil {
			logs.Logger.Critical(err)
		}
	}()

	if len(name) == 0 {
		logs.Logger.Critical(fmt.Sprintln("offline file name is empty"))
		return
	}
	command := `update files set stamp = @stamp where name = @name`

	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err := nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	stampParam := pgsql.NewParameter("@stamp", pgsql.Bigint)
	err = stampParam.SetValue(time.Now().Unix())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, stampParam, nameParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}

	pool.Release(conn)
	return
}

func GetFilePath(name string) (path string) {
	defer func() {
		if err := recover(); err != nil {
			logs.Logger.Critical(err)
		}
	}()
	if len(name) == 0 {
		logs.Logger.Critical(fmt.Sprintln("GetFilePath is empty"))
		return
	}
	commond := `select path from files where name = @name`

	nameParam := pgsql.NewParameter("@name", pgsql.Text)
	err := nameParam.SetValue(name)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	res, err := conn.Query(commond, nameParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		_, err := res.ScanNext(&path)
		if err != nil {
			logs.Logger.Critical("Error scan mid: ", err)
		}
	}
	return
}
