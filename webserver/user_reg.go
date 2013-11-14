package webserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hug/core/users"
	"hug/logs"
	"io/ioutil"
	"net/http"
)

const (
	UserRegPath    = "/user/reg"
	UserVerifyPath = "/user/verify"
	UserPort       = ":9090"
)

func initUserReg() {
	http.HandleFunc(UserRegPath, handleUserReg)
	http.HandleFunc(UserVerifyPath, handleUserVerify)
	logs.Logger.Info("init user registration web server successful.")
}

func handleUserVerify(w http.ResponseWriter, r *http.Request) {
	logs.Logger.Info("Handle user verify")
	var resPkt users.VerifyResPkt
	resPkt.Code = users.VerifyRegUserCode_InvalidReq
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("handleUserVerify json marshal respacket error:", err)
		}
		fmt.Fprint(w, string(resData))
	}()
	if r.Method == "POST" {
		r.ParseForm()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			logs.Logger.Critical("handleUserVerify get post data error:", err)
			return
		}
		logs.Logger.Info("handleUserVerify body:", string(body))
		var reqPkt users.VerifyReqPkt
		err = json.Unmarshal(body, &reqPkt)
		if err != nil {
			logs.Logger.Warn("handleUserVerify data json unmarshal error:", err, "body:", string(body))
			return
		}
		resPkt.Code = users.VerifyAccount(reqPkt)
	}
}

func handleUserReg(w http.ResponseWriter, r *http.Request) {
	//解析参数，默认是不会解析的
	logs.Logger.Info("handle user registration")
	var resPacket users.RegUserResPkt
	resPacket.Code = users.RegUserCode_RequestDataErr
	defer func() {
		resData, err := json.Marshal(resPacket)
		if err != nil {
			logs.Logger.Critical("handleUserReg json marshal respacket error:", err)
		}
		fmt.Fprint(w, string(resData))
	}()
	if r.Method == "POST" {
		r.ParseForm()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			logs.Logger.Critical("handleUserReg get post data error:", err)
			return
		}
		logs.Logger.Info("handleUserReg body:", string(body))
		var regUser users.RegUser
		err = json.Unmarshal(body, &regUser)
		if err != nil {
			logs.Logger.Warn("handleUserReg data json unmarshal error:", err, "body:", string(body))
			return
		}
		passData, err := base64.StdEncoding.DecodeString(regUser.Password)
		if err != nil {
			logs.Logger.Warn("handleUserReg base64 decode password error:", err, "password base64:", regUser.Password)
			return
		}
		regUser.Password = string(passData)
		emailData, err := base64.StdEncoding.DecodeString(regUser.Email)
		if err != nil {
			logs.Logger.Warn("handleUserReg base64 decode email error:", err, "email base64:", regUser.Email)
			return
		}
		regUser.Email = string(emailData)
		resPacket.Code, err = users.CreateRegUser(regUser)
		if err != nil {
			logs.Logger.Critical("handleUserReg create registration user error:", err)
			return
		}
		logs.Logger.Info("handleUserReg code =", resPacket.Code)
	}
}
