package rosters

import (
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/config"
	"hug/logs"
	"os"
	"time"
)

var pool *pgsql.Pool
var RosterChangedNotificationChan chan RosterChangedNotification
var HandleRosterRequestNotificationChan chan HandleRosterRequestNotification

func ConnDB(cfg *config.Config) (err error) {
	dbName, err := cfg.GetString("db_rosters_name")
	if err != nil {
		logs.Logger.Critical("load db name from config error: ", err)
		os.Exit(300)
		return
	}
	user, err := cfg.GetString("db_rosters_user")
	if err != nil {
		logs.Logger.Critical("load user from config error: ", err)
		os.Exit(300)
		return
	}
	password, err := cfg.GetString("db_rosters_password")
	if err != nil {
		logs.Logger.Critical("load password from config error: ", err)
		os.Exit(300)
		return
	}
	minConns, err := cfg.GetInt("db_rosters_min_conns")
	if err != nil {
		logs.Logger.Critical("load min conns from config error: ", err)
		os.Exit(300)
		return
	}
	maxConns, err := cfg.GetInt("db_rosters_max_conns")
	if err != nil {
		logs.Logger.Critical("load max conns from config error: ", err)
		os.Exit(300)
		return
	}
	idleTimeout, err := cfg.GetInt("db_rosters_idle_timeout")
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
	RosterChangedNotificationChan = make(chan RosterChangedNotification, 1024)
	HandleRosterRequestNotificationChan = make(chan HandleRosterRequestNotification, 1024)
	return
}

func CloseDB() {
	pool.Close()
}
