package devices

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

type IosDevice struct {
	Token     string `json:"token"`
	IsSandBox int16  `json:"issandbox"`
}

const (
	IosDeviceStatus_Background int16 = iota
	IosDeviceStatus_Foreground
)

type IosDeviceStatusPkt struct {
	Status int16 `json:"s,omitempty"`
}

const createIosDevicesTableSql = `
	CREATE TABLE IF NOT EXISTS iosdevices
		(
		  token character varying(100) NOT NULL unique,
		  uid bigint NOT NULL default 0,
		  isSandBox smallint NOT NULL default 0,
		  status smallint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
		`

func GetIosDevicesOfUid(uid int64, status int16) (devs []IosDevice) {
	command := `
	SELECT token, isSandBox from iosdevices where uid = @uid AND status = @status;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
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
		res, err := conn.Query(command, uidParam, statusParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else {
			devs = make([]IosDevice, 0, 3)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var dev IosDevice
					err = res.Scan(&dev.Token, &dev.IsSandBox)
					if err != nil {
						logs.Logger.Critical("Error scan: ", err)
						continue
					} else {
						devs = append(devs, dev)
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

func IsIosTokenExist(token string) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS num FROM iosdevices WHERE token = @token;
	`
	tokenParam := pgsql.NewParameter("@token", pgsql.Text)
	err = tokenParam.SetValue(token)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, tokenParam)
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

func insertIosDevice(uid int64, dev IosDevice) {
	command := `
	INSERT INTO iosdevices(uid,token,isSandBox) 
			VALUES(@uid, @token, @isSandBox);
	`
	tokenParam := pgsql.NewParameter("@token", pgsql.Text)
	err := tokenParam.SetValue(dev.Token)
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
	isSandBoxParam := pgsql.NewParameter("@isSandBox", pgsql.Smallint)
	err = isSandBoxParam.SetValue(dev.IsSandBox)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, tokenParam, isSandBoxParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateIosDevice(uid int64, dev IosDevice) {
	command := `
	update iosdevices set uid=@uid,isSandBox=@isSandBox  where token=@token;
	`
	tokenParam := pgsql.NewParameter("@token", pgsql.Text)
	err := tokenParam.SetValue(dev.Token)
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
	isSandBoxParam := pgsql.NewParameter("@isSandBox", pgsql.Smallint)
	err = isSandBoxParam.SetValue(dev.IsSandBox)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, isSandBoxParam, tokenParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetIosDeviceToken(uid int64, dev IosDevice) {
	exist, err := IsIosTokenExist(dev.Token)
	if err != nil {
		return
	}
	if exist {
		updateIosDevice(uid, dev)
	} else {
		insertIosDevice(uid, dev)
	}
}

func SetIosDeviceStatus(token string, status int16) {
	command := `
	update iosdevices set status=@status  where token = @token;
	`
	tokenParam := pgsql.NewParameter("@token", pgsql.Text)
	err := tokenParam.SetValue(token)
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
		_, err := conn.Execute(command, statusParam, tokenParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)

}

func (dev IosDevice) IsValid() (valid bool) {
	if len(dev.Token) > 40 {
		return true
	} else {
		return false
	}

}
