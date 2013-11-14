package groups

import (
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/config"
	"hug/logs"
	"os"
	"time"
)

var pool *pgsql.Pool
var GroupChangedNotificationChan chan GroupChangedNotification

func ConnDB(cfg *config.Config) (err error) {
	dbName, err := cfg.GetString("db_groups_name")
	if err != nil {
		logs.Logger.Critical("load db name from config error: ", err)
		os.Exit(300)
		return
	}
	user, err := cfg.GetString("db_groups_user")
	if err != nil {
		logs.Logger.Critical("load user from config error: ", err)
		os.Exit(300)
		return
	}
	password, err := cfg.GetString("db_groups_password")
	if err != nil {
		logs.Logger.Critical("load password from config error: ", err)
		os.Exit(300)
		return
	}
	minConns, err := cfg.GetInt("db_groups_min_conns")
	if err != nil {
		logs.Logger.Critical("load min conns from config error: ", err)
		os.Exit(300)
		return
	}
	maxConns, err := cfg.GetInt("db_groups_max_conns")
	if err != nil {
		logs.Logger.Critical("load max conns from config error: ", err)
		os.Exit(300)
		return
	}
	idleTimeout, err := cfg.GetInt("db_groups_idle_timeout")
	if err != nil {
		logs.Logger.Critical("load idle timeout from config error: ", err)
		os.Exit(300)
		return
	}

	params := fmt.Sprintf("dbname=%s user=%s password=%s sslmode=disable", dbName, user, password)
	pool, err = pgsql.NewPool(params, minConns, maxConns, time.Duration(idleTimeout)*time.Second)
	if err != nil {
		logs.Logger.Critical("Error opening connection pool: %s\n", err)
	}
	//pool.Debug = true
	GroupChangedNotificationChan = make(chan GroupChangedNotification, 1024)

	return
}

func CloseDB() {
	pool.Close()
}
