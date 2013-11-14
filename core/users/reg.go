package users

import (
	"encoding/base64"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/logs"
	"hug/utils"
	"math/rand"
	"time"
)

type RegUser struct {
	Email       string `json:"email"`
	Password    string `json:"pass"`
	Mobile      string `json:"mobile,omitempty"`
	RealName    string `json:"realname,omitempty"`
	NickName    string `json:"nickname,omitempty"`
	Gender      int16  `json:"gender,omitempty"`
	Signature   string `json:"signature,omitempty"`
	Location    string `json:"location,omitempty"`
	Birthday    string `json:"birthday,omitempty"`
	VerifyCode  string `json:"verifycode,omitempty"`
	CreateStamp int64  `json:"createstamp,omitempty"`
}

type RegUserResPkt struct {
	Code int8 `json:"code"`
}

type VerifyReqPkt struct {
	Account    string `json:"account"`
	VerifyCode string `json:"code"`
}

type VerifyResPkt struct {
	Code int8 `json:"code"`
}

const (
	RegUserCode_None int8 = iota
	RegUserCode_UserExist
	RegUserCode_UserWaitForVerify
	RegUserCode_InvalidEmail
	RegUserCode_InvalidPassword
	RegUserCode_InvalidMobile
	RegUserCode_DatabaseErr
	RegUserCode_RequestDataErr
)

const (
	VerifyRegUserCode_None int8 = iota
	VerifyRegUserCode_InvalidReq
	VerifyRegUserCode_InvalidVerifyText
	VerifyRegUserCode_AlreadyVerified
	VerifyRegUserCode_UnRegUser
	VerifyRegUserCode_InvalidVerifyCode
	VerifyRegUserCode_DatabaseErr
	VerifyRegUserCode_CreateUserErr
	VerifyRegUserCode_CreateUserInfoErr
)

const regVerifyUrl = "http://localhost:9090/user/verify/?code="

const regVerifyMailSender = "reg.wing.suzhou@gmail.com"
const regVerifyMailSenderPass = "RegWingSuzhou"
const regVerifyMailSenderAuthHost = "smtp.gmail.com"
const regVerifyMailSenderSmtpServer = "smtp.gmail.com:587"
const regVerifyMailSenderServerTLSEnable = true

const createRegTableSql = `
CREATE TABLE IF NOT EXISTS regusers
		(
		  Email character varying(200) NOT NULL unique,
		  Password character varying(200) NOT NULL,
		  Mobile character varying(100) default '',
		  RealName character varying(100) default '',
		  NickName character varying(100) default '',
		  Gender smallint NOT NULL default 1,
		  Signature character varying(200) default '',
		  Location character(8) default '11|01|01',
		  Birthday character(10) NOT NULL default '1980-1-1',
		  VerifyCode character varying(200) NOT NULL,
		  CreateStamp bigint NOT NULL default 0,
		  CONSTRAINT notverifiedusers_pkey PRIMARY KEY (Email)
		)
		WITH (OIDS=FALSE);
		`

// const regVerifyMailSender = "jc888@outlook.com"
// const regVerifyMailSenderPass = "JasonChong910"
// const regVerifyMailSenderAuthHost = "smtp.live.com"
// const regVerifyMailSenderSmtpServer = "smtp.live.com:587"

func CreateRegUser(reqPacket RegUser) (code int8, err error) {
	if !utils.IsValidEmail(reqPacket.Email) {
		code = RegUserCode_InvalidEmail
		return
	}
	if len(reqPacket.Password) == 0 {
		code = RegUserCode_InvalidPassword
		return
	}
	if len(reqPacket.Mobile) > 0 && !utils.IsValidMobile(reqPacket.Mobile) {
		code = RegUserCode_InvalidMobile
		return
	}
	exist, err := IsUserExist(reqPacket.Email)
	if err != nil {
		code = RegUserCode_DatabaseErr
		return
	}
	if exist {
		code = RegUserCode_UserExist
		return
	}

	exist, err = IsUserRegisted(reqPacket.Email)
	if err != nil {
		code = RegUserCode_DatabaseErr
		return
	}
	if exist {
		code = RegUserCode_UserWaitForVerify
		return
	}
	code = RegUserCode_DatabaseErr
	reqPacket.VerifyCode = generateSimpleVerifyCode()
	encryptPassword := EncryptPassword(reqPacket.Email, reqPacket.Password)
	command := `
		INSERT INTO regusers(Email,Password,Mobile,RealName,NickName,Gender,Signature,Location,Birthday,VerifyCode,CreateStamp) 
		VALUES(@Email,@Password,@Mobile,@RealName,@NickName,@Gender,@Signature,@Location,@Birthday,@VerifyCode,@CreateStamp);
		`
	//logs.Logger.Infof("%v", reqPacket)
	//logs.Logger.Info(command)
	emailParam := pgsql.NewParameter("@Email", pgsql.Text)
	err = emailParam.SetValue(reqPacket.Email)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	passwordParam := pgsql.NewParameter("@Password", pgsql.Text)
	err = passwordParam.SetValue(encryptPassword)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	mobileParam := pgsql.NewParameter("@Mobile", pgsql.Text)
	err = mobileParam.SetValue(reqPacket.Mobile)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	realnameParam := pgsql.NewParameter("@RealName", pgsql.Text)
	err = realnameParam.SetValue(reqPacket.RealName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	nicknameParam := pgsql.NewParameter("@NickName", pgsql.Text)
	err = nicknameParam.SetValue(reqPacket.NickName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	genderParam := pgsql.NewParameter("@Gender", pgsql.Smallint)
	err = genderParam.SetValue(reqPacket.Gender)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	signatureParam := pgsql.NewParameter("@Signature", pgsql.Text)
	err = signatureParam.SetValue(reqPacket.Signature)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	locationParam := pgsql.NewParameter("@Location", pgsql.Text)
	err = locationParam.SetValue(reqPacket.Location)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	birthdayParam := pgsql.NewParameter("@Birthday", pgsql.Text)
	err = birthdayParam.SetValue(reqPacket.Birthday)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	verifyCodeParam := pgsql.NewParameter("@VerifyCode", pgsql.Text)
	err = verifyCodeParam.SetValue(reqPacket.VerifyCode)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	createStampParam := pgsql.NewParameter("@CreateStamp", pgsql.Bigint)
	err = createStampParam.SetValue(time.Now().UnixNano())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	code = RegUserCode_DatabaseErr
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, emailParam, passwordParam, mobileParam, realnameParam, nicknameParam, genderParam, signatureParam, locationParam, birthdayParam, verifyCodeParam, createStampParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	} else {
		code = RegUserCode_None
		SendVerifyMail(reqPacket.Email, reqPacket.VerifyCode)
	}
	pool.Release(conn)
	return
}

func removeRegUser(email string) {
	command := `
	 DELETE FROM regusers where email = @email;
	 `
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err := emailParam.SetValue(email)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err = conn.Execute(command, emailParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func GetRegUser(email string) (regUser RegUser, err error) {
	command := `
	SELECT * FROM regusers where email = @email;
	`
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err = emailParam.SetValue(email)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, emailParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&regUser.Email, &regUser.Password, &regUser.Mobile, &regUser.RealName, &regUser.NickName, &regUser.Gender, &regUser.Signature, &regUser.Location, &regUser.Birthday, &regUser.VerifyCode, &regUser.CreateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsUserRegisted(email string) (exist bool, err error) {
	command := `
	SELECT COUNT(*) AS numusers FROM regusers where email = @email;
	`
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err = emailParam.SetValue(email)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	n := 0
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, emailParam)
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
	if n >= 1 {
		exist = true
	} else {
		exist = false
	}
	return
}

func generateSimpleVerifyCode() (code string) {
	key := rand.New(rand.NewSource(time.Now().UnixNano())).Int() % 1000000
	code = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%6d", key)))
	return
}

func VerifyAccount(reqPkt VerifyReqPkt) (code int8) {
	if len(reqPkt.Account) == 0 || len(reqPkt.VerifyCode) == 0 || !utils.IsValidEmail(reqPkt.Account) {
		return VerifyRegUserCode_InvalidVerifyText
	}
	exist, err := IsUserExist(reqPkt.Account)
	if err != nil {
		logs.Logger.Critical(err)
		return VerifyRegUserCode_DatabaseErr
	}
	if exist {
		logs.Logger.Critical("aready verified")
		return VerifyRegUserCode_AlreadyVerified
	}
	exist, err = IsUserRegisted(reqPkt.Account)
	if err != nil {
		logs.Logger.Critical(err)
		return VerifyRegUserCode_DatabaseErr
	}
	if !exist {
		return VerifyRegUserCode_UnRegUser
	}
	dbRegUser, err := GetRegUser(reqPkt.Account)
	if err != nil {
		logs.Logger.Critical(err)
		return VerifyRegUserCode_DatabaseErr
	}
	if reqPkt.VerifyCode != dbRegUser.VerifyCode {
		return VerifyRegUserCode_InvalidVerifyCode
	}
	uid, err := createUser(reqPkt.Account, dbRegUser.Password)
	if err != nil {
		logs.Logger.Critical(err)
		return VerifyRegUserCode_CreateUserErr
	}
	err = createUserInfo(uid, dbRegUser)
	if err != nil {
		logs.Logger.Critical(err)
		return VerifyRegUserCode_CreateUserInfoErr
	}
	removeRegUser(reqPkt.Account)
	return VerifyRegUserCode_None
}

func SendMail(e *utils.Email) {
	err := e.Send()
	if err != nil {
		logs.Logger.Critical(err)
	} else {
		logs.Logger.Info("SendMail successful")
	}
}

func SendVerifyMail(account string, verifyCode string) {
	subject := "Registration Verify Code"
	body := fmt.Sprintf(`
	<html>
    <body>
    %s
    </body>
    </html>`, fmt.Sprintf("Your verify code is: %s", verifyCode))
	go utils.SendMail(subject, regVerifyMailSender, account, regVerifyMailSenderAuthHost, regVerifyMailSenderSmtpServer, regVerifyMailSenderPass, body, regVerifyMailSenderServerTLSEnable)
	return
}
