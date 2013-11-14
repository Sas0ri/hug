package core

import (
	"hug/config"
	"hug/core/corps"
	"hug/core/devices"
	"hug/core/groups"
	"hug/core/messages"
	"hug/core/rosters"
	"hug/core/users"
	"hug/logs"
	"hug/utils"
	"log"
	"os"
)

func Start() {
	logs.Logger.Info("Starting core...")
	log.Println("Starting core...")

	cfg, err := config.LoadConfigFile(utils.ApplicationPath() + "/config_db.json")
	if err != nil {
		logs.Logger.Critical("Load config failed: ", err)
		os.Exit(300)
		return
	}

	if users.ConnDB(cfg) != nil {
		os.Exit(2)
	}

	if corps.ConnDB(cfg) != nil {
		os.Exit(3)
	}

	if groups.ConnDB(cfg) != nil {
		os.Exit(4)
	}

	if rosters.ConnDB(cfg) != nil {
		os.Exit(5)
	}

	if messages.ConnDB(cfg) != nil {
		os.Exit(6)
	}

	if devices.ConnDB(cfg) != nil {
		os.Exit(7)
	}
	log.Println("Starting core successful.")
	logs.Logger.Info("Starting core successful.")

}

func Stop() {
	users.CloseDB()
	corps.CloseDB()
	messages.CloseDB()
	groups.CloseDB()
	devices.CloseDB()
}
