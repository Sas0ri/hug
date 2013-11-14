package devices

import (
	"github.com/lxn/go-pgsql"
	"hug/logs"
)

type AndroidDevice struct {
	Alias string `json:"alias"`
}

const (
	AndroidDeviceStatus_Background int16 = iota
	AndroidDeviceStatus_Foreground
)

type AndroidDeviceStatusPkt struct {
	Status int16 `json:"s,omitempty"`
}

const createAndroidDevicesTableSql = `
	CREATE TABLE IF NOT EXISTS androiddevices
		(
		  alias character varying(100) NOT NULL unique,
		  uid bigint NOT NULL default 0,
		  status smallint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
		`

func GetAndroidDevicesOfUid(uid int64, status int16) (devs []AndroidDevice) {
	command := `
	SELECT alias from androiddevices where uid = @uid AND status = @status;
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
			devs = make([]AndroidDevice, 0, 3)
			for {
				hasRow, _ := res.FetchNext()
				if hasRow {
					var dev AndroidDevice
					err = res.Scan(&dev.Alias)
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

func IsAndroidTokenExist(alias string) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) AS num FROM androiddevices WHERE alias = @alias;
	`
	aliasParam := pgsql.NewParameter("@alias", pgsql.Text)
	err = aliasParam.SetValue(alias)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, aliasParam)
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

func insertAndroidDevice(uid int64, dev AndroidDevice) {
	command := `
	INSERT INTO androiddevices(uid,alias) 
			VALUES(@uid, @alias);
	`
	aliasParam := pgsql.NewParameter("@alias", pgsql.Text)
	err := aliasParam.SetValue(dev.Alias)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, aliasParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func updateAndroidDevice(uid int64, dev AndroidDevice) {
	command := `
	update androiddevices set uid=@uid where alias=@alias;
	`
	aliasParam := pgsql.NewParameter("@alias", pgsql.Text)
	err := aliasParam.SetValue(dev.Alias)
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

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, uidParam, aliasParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func SetAndroidDeviceAlias(uid int64, dev AndroidDevice) {
	exist, err := IsAndroidTokenExist(dev.Alias)
	if err != nil {
		return
	}
	if exist {
		updateAndroidDevice(uid, dev)
	} else {
		insertAndroidDevice(uid, dev)
	}
}

func SetAndroidDeviceStatus(alias string, status int16) {
	command := `
	update androiddevices set status=@status  where alias=@alias;
	`
	aliasParam := pgsql.NewParameter("@alias", pgsql.Text)
	err := aliasParam.SetValue(alias)
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
		_, err := conn.Execute(command, statusParam, aliasParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
}

func (dev AndroidDevice) IsValid() (valid bool) {
	if len(dev.Alias) > 0 {
		return true
	} else {
		return false
	}

}
