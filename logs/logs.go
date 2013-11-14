package logs

import (
	"bytes"
	"fmt"
	seelog "github.com/cihub/seelog"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

var Logger seelog.LoggerInterface

func loadAppConfig() {
	app, _ := exec.LookPath(os.Args[0])
	logConfigFileName, _ := filepath.Abs(app)
	fmt.Println(logConfigFileName)
	logConfigFileName, _ = filepath.Split(logConfigFileName)
	fmt.Println(logConfigFileName)

	var logCfgFile string
	if runtime.GOOS == "windows" {
		logCfgFile += "config_log_win.xml"
	} else if runtime.GOOS == "darwin" {
		logCfgFile += "config_log_darwin.xml"
	} else {
		logCfgFile += "config_log_linux.xml"
	}

	logConfigFileName = logConfigFileName + "/" + logCfgFile
	file, err := os.Open(logConfigFileName)
	b := new(bytes.Buffer)
	_, err = b.ReadFrom(file)
	if err != nil {
		log.Println("load log config file error:", err)
		os.Exit(200)
		return
	}
	logger, err := seelog.LoggerFromConfigAsBytes(b.Bytes())
	if err != nil {
		log.Println(err)
		return
	}
	UseLogger(logger)
	//log.Println("start logger")
}

func InitLogger() {
	DisableLog()
	loadAppConfig()
}

// DisableLog disables all library log output
func DisableLog() {
	Logger = seelog.Disabled
}

// UseLogger uses a specified seelog.LoggerInterface to output library log.
// Use this func if you are using Seelog logging system in your app.
func UseLogger(newLogger seelog.LoggerInterface) {
	Logger = newLogger
}
