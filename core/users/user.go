package users

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/core/devices"
	"hug/logs"
	"io"
)

const (
	UserStatus_None int16 = iota
	UserStatus_Active
	UserStatus_Frozen
)

type User struct {
	Uid                 int64  `json:"uid"`
	Email               string `json:"e"`
	Mobile              string `json:"m,omitempty"`
	Passwrod            string `json:"ep,omitempty"`
	Status              int16  `json:"st"`
	MaxCorpAllow        int16  `json:"mca,omitempty"`
	MaxNormalGroupAllow int16  `json:"mnga,omitempty"`
	MaxSuperGroupAllow  int16  `json:"msga,omitempty"`
}

const createUsersTableSql = `
CREATE TABLE IF NOT EXISTS users
		(
		  Uid serial NOT NULL unique,
		  Email character varying(200) NOT NULL unique,
		  mobile character varying(50) default '',
		  Password character varying(200) NOT NULL,
		  Status smallint NOT NULL default 1,
		  MaxCorpAllow smallint NOT NULL default 1,
		  MaxNormalGroupAllow smallint NOT NULL default 10,
		  MaxSuperGroupAllow smallint NOT NULL default 2,
		  CONSTRAINT users_pkey PRIMARY KEY (uid)
		)
		WITH (OIDS=FALSE);
		`

const userPasswordEncryptSalt1 = "#$@&*dd=="
const userPasswordEncryptSalt2 = "#$@^&*()=="

func EncryptPassword(account, password string) (encryptPass string) {
	h := md5.New()
	io.WriteString(h, password)

	pwmd5 := fmt.Sprintf("%x", h.Sum(nil))

	io.WriteString(h, userPasswordEncryptSalt1)
	io.WriteString(h, account)
	io.WriteString(h, userPasswordEncryptSalt2)
	io.WriteString(h, pwmd5)

	encryptPass = fmt.Sprintf("%x", h.Sum(nil))
	return
}

const (
	AuthCode_None int8 = iota
	AuthCode_InvalidReq
	AuthCode_UserNotExist
	AuthCode_PasswordIncorrect
	AuthCode_UserNotVerified
	AuthCode_DatabaseErr
	AuthCode_WaitAuth
)

const (
	TerminalType_None int16 = iota
	TerminalType_PC
	TerminalType_Mobile_Iphone
	TerminalType_Mobile_Android
	TerminalType_Pad_Ipad
	TerminalType_Pad_Android
	TerminalType_Web
)

type AuthInfo struct {
	Account         string
	Uid             int64
	TerminalType    int16
	TerminalSystem  string
	TerminalVersion string
	AuthCode        int8
	IosDevice       devices.IosDevice
	AndroidDevice   devices.AndroidDevice
}

func (a *AuthInfo) String() (str string) {
	str = "Auth info ["
	str += ("Account:" + a.Account)
	str += (fmt.Sprintf(", Uid: %d", a.Uid))
	str += ", Terminal type:"
	switch a.TerminalType {
	case TerminalType_PC:
		str += "PC"
	case TerminalType_Mobile_Iphone:
		str += "Mobile Iphone"
	case TerminalType_Mobile_Android:
		str += "Mobile Android"
	case TerminalType_Pad_Ipad:
		str += "Pad IPad"
	case TerminalType_Pad_Android:
		str += "Pad Android"
	case TerminalType_Web:
		str += "Web"
	default:
		str += "Unknown"
	}
	str += (", Terminal system:" + a.TerminalSystem)
	str += (", Terminal version:" + a.TerminalVersion)
	str += "]"
	return
}

func AuthUser(account, password string) (code int8, uid int64) {
	uid = 0
	exist, err := IsUserRegisted(account)
	if err != nil {
		code = AuthCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	}
	if exist {
		code = AuthCode_UserNotVerified
		return
	}
	exist, err = IsUserExist(account)
	if err != nil {
		code = AuthCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	}
	if !exist {
		code = AuthCode_UserNotExist
		return
	}
	user, err := GetUser(account)
	if err != nil {
		code = AuthCode_DatabaseErr
		logs.Logger.Critical(err)
		return
	}
	encryptPass := EncryptPassword(account, password)
	if user.Passwrod != encryptPass {
		code = AuthCode_PasswordIncorrect
		return
	}
	code = AuthCode_None
	uid = user.Uid
	return

}

func GetUserAccount(uid int64) (account string, err error) {
	command := `
	SELECT Email FROM users where uid = @uid;
	`
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
		res, err := conn.Query(command, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&account)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetUser(account string) (user User, err error) {
	command := `
	SELECT * FROM users where email = @email;
	`
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err = emailParam.SetValue(account)
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
			err = res.Scan(&user.Uid, &user.Email, &user.Mobile, &user.Passwrod, &user.Status, &user.MaxCorpAllow, &user.MaxNormalGroupAllow, &user.MaxSuperGroupAllow)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsUserExist(account string) (exist bool, err error) {
	exist = false
	n := 0
	command := `
	SELECT COUNT(*) AS numusers FROM users where email = @email;
	`
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err = emailParam.SetValue(account)
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

func IsUidExist(uid int64) (exist bool, err error) {
	exist = false
	if !IsUidValid(uid) {
		return
	}
	n := 0
	command := `
	SELECT COUNT(*) AS numusers FROM users where uid = @uid;
	`
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
		res, err := conn.Query(command, uidParam)
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

func MaxCorpAllow(uid int64) (n int16, err error) {
	n = 0
	command := `
	SELECT MaxCorpAllow FROM users where uid = @uid;
	`
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
		res, err := conn.Query(command, uidParam)
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

	return
}

func createUser(email, password string) (uid int64, err error) {
	uid = 0
	exist, err := IsUserExist(email)
	if err != nil {
		err = errors.New(fmt.Sprintln("check if corp is exist error=", err))
		logs.Logger.Critical(err)
		return
	}
	if exist {
		err = errors.New("createUser error: user already exist")
		logs.Logger.Critical(err)
		return
	}
	command := `
	INSERT INTO users(email,Password) 
		VALUES(@email,@password) RETURNING uid;
	`
	emailParam := pgsql.NewParameter("@email", pgsql.Text)
	err = emailParam.SetValue(email)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	passwordParam := pgsql.NewParameter("@password", pgsql.Text)
	err = passwordParam.SetValue(password)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, emailParam, passwordParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			err = res.Scan(&uid)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func IsUidValid(uid int64) (valid bool) {
	if uid <= 0 {
		return false
	}
	return true
}
