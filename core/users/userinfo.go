package users

import (
	"errors"
	"fmt"
	"github.com/lxn/go-pgsql"
	"hug/logs"
	"time"
)

const (
	Gender_None int16 = iota
	Gender_Male
	Gender_Female
	Gender_Other
)

const createUserInfoTableSql = `
CREATE TABLE IF NOT EXISTS userinfos
		(
		  Uid bigint NOT NULL unique,
		  RealName character varying(200) default '',
		  NickName character varying(200) default '',
		  AvatarFile character varying(200) default '',
		  Signature character varying(200) default '',
		  Mobile character varying(20) default '',
		  Gender smallint NOT NULL default 1,
		  Location character varying(12) default '11|01|01',
		  Birthday character(10) NOT NULL default '1990-01-01',
		  UpdateStamp bigint NOT NULL default 0,
		  CreateStamp bigint NOT NULL default 0,
		  VerifyStamp bigint NOT NULL default 0,
		  LastLoginStamp bigint NOT NULL default 0,
		  LastLogoutStamp bigint NOT NULL default 0
		)
		WITH (OIDS=FALSE);
		`

type UserInfo struct {
	Uid             int64  `json:"uid"`
	Account         string `json:"ac,omitempty"`
	RealName        string `json:"rn,omitempty"`
	NickName        string `json:"nn,omitempty"`
	AvatarFile      string `json:"af,omitempty"`
	Signature       string `json:"si,omitempty"`
	Mobile          string `json:"mb,omitempty"`
	Gender          int16  `json:"gd,omitempty"`
	Location        string `json:"l,omitempty"`
	Birthday        string `json:"bd,omitempty"`
	UpdateStamp     int64  `json:"us,omitempty"`
	CreateStamp     int64  `json:"cs,omitempty"`
	VerifyStamp     int64  `json:"vs,omitempty"`
	LastLogInStamp  int64  `json:"lis,omitempty"`
	LastLogoutStamp int64  `json:"los,omitempty"`
}

type GetUserInfosReqPkt struct {
	Uids []int64 `json:"uids,omitempty"`
}

type GetUserInfosResPkt struct {
	Infos []UserInfo `json:"infos,omitempty"`
}

type GetUserInfoChangedReqPkt struct {
	Stamp int64 `json:"st,omitempty"`
	Uid   int64 `json:"uid,omitempty"`
}

type GetUserInfoChangedResPkt struct {
	Infos []UserInfo `json:"infos,omitempty"`
}

type UserInfoChangedNotificationPkt struct {
	Uid int64 `json:"uid,omitempty"`
}

const (
	SetUserInfoCode_None int8 = iota
	SetUserInfoCode_InvalidFormat
	SetUserInfoCode_UidNotExist
	SetUserInfoCode_DatabaseErr
	SetUserInfoCOde_NoPermission
)

type SetUserInfoReqPkt struct {
	Changes []string `json:"changes,omitempty"`
	Info    UserInfo `json:"infos,omitempty"`
}

type SetUserInfoResPkt struct {
	Code  int8  `json:"code"`
	Stamp int64 `json:"stamp, omitempty"`
}

func createUserInfo(uid int64, regUser RegUser) (err error) {
	exist, err := IsUserInfoExist(uid)
	if err != nil {
		return
	}
	if exist {
		err = errors.New("userinfo already exist")
		logs.Logger.Critical(err)
		return
	}
	command := `
		INSERT INTO userinfos(uid,Mobile,RealName,NickName,Gender,Signature,Location,Birthday,updatestamp,CreateStamp,VerifyStamp) 
		VALUES(@uid,@Mobile,@RealName,@NickName,@Gender,@Signature,@Location,@Birthday,@updatestamp,@CreateStamp,@VerifyStamp);
		`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	mobileParam := pgsql.NewParameter("@Mobile", pgsql.Text)
	err = mobileParam.SetValue(regUser.Mobile)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	realnameParam := pgsql.NewParameter("@RealName", pgsql.Text)
	err = realnameParam.SetValue(regUser.RealName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	nicknameParam := pgsql.NewParameter("@NickName", pgsql.Text)
	err = nicknameParam.SetValue(regUser.NickName)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	genderParam := pgsql.NewParameter("@Gender", pgsql.Smallint)
	err = genderParam.SetValue(regUser.Gender)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	signatureParam := pgsql.NewParameter("@Signature", pgsql.Text)
	err = signatureParam.SetValue(regUser.Signature)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if len(regUser.Location) < 6 {
		regUser.Location = "11|01|01"
	}
	locationParam := pgsql.NewParameter("@Location", pgsql.Text)
	err = locationParam.SetValue(regUser.Location)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	if len(regUser.Birthday) < 6 {
		regUser.Birthday = "1970-01-01"
	}
	birthdayParam := pgsql.NewParameter("@Birthday", pgsql.Text)
	err = birthdayParam.SetValue(regUser.Birthday)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(time.Now().UnixNano())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	createStampParam := pgsql.NewParameter("@CreateStamp", pgsql.Bigint)
	err = createStampParam.SetValue(regUser.CreateStamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	verifyStampParam := pgsql.NewParameter("@VerifyStamp", pgsql.Bigint)
	err = verifyStampParam.SetValue(time.Now().UnixNano())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
		return
	}
	_, err = conn.Execute(command, uidParam, mobileParam, realnameParam, nicknameParam, genderParam, signatureParam, locationParam, birthdayParam, updatestampParam, createStampParam, verifyStampParam)
	if err != nil {
		logs.Logger.Critical("Error executing query: ", err)
	}
	pool.Release(conn)
	return
}

func UpdateUserInfo(reqPkt SetUserInfoReqPkt) (code int8, stamp int64) {
	exist, err := IsUserInfoExist(reqPkt.Info.Uid)
	if err != nil {
		logs.Logger.Critical(err)
		return SetUserInfoCode_DatabaseErr, 0
	}
	if !exist {
		logs.Logger.Critical(err)
		return SetUserInfoCode_UidNotExist, 0
	}

	params := make([]*pgsql.Parameter, 0, 8)
	commandParams := ""
	for _, node := range reqPkt.Changes {
		if node == "realname" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "realname=@realname"
			realnameParam := pgsql.NewParameter("@realname", pgsql.Text)
			err = realnameParam.SetValue(reqPkt.Info.RealName)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, realnameParam)
		} else if node == "nickname" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "nickname=@nickname"
			nicknameParam := pgsql.NewParameter("@nickname", pgsql.Text)
			err = nicknameParam.SetValue(reqPkt.Info.NickName)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, nicknameParam)
		} else if node == "mobile" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "mobile=@mobile"
			mobileParam := pgsql.NewParameter("@mobile", pgsql.Text)
			err = mobileParam.SetValue(reqPkt.Info.Mobile)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, mobileParam)
		} else if node == "gender" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "gender=@gender"
			genderParam := pgsql.NewParameter("@gender", pgsql.Smallint)
			err = genderParam.SetValue(reqPkt.Info.Gender)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, genderParam)
		} else if node == "signature" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "signature=@signature"
			signatureParam := pgsql.NewParameter("@signature", pgsql.Text)
			err = signatureParam.SetValue(reqPkt.Info.Signature)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, signatureParam)
		} else if node == "location" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "location=@location"
			locationParam := pgsql.NewParameter("@location", pgsql.Text)
			err = locationParam.SetValue(reqPkt.Info.Location)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, locationParam)
		} else if node == "birthday" {
			if len(commandParams) > 0 {
				commandParams += ","
			}
			commandParams += "birthday=@birthday"
			birthdayParam := pgsql.NewParameter("@birthday", pgsql.Text)
			err = birthdayParam.SetValue(reqPkt.Info.Birthday)
			if err != nil {
				logs.Logger.Critical(err)
				return
			}
			params = append(params, birthdayParam)
		}
	}
	if len(params) > 0 && len(commandParams) > 0 {
		stamp = time.Now().UnixNano()
		commandParams += ","
		commandParams += "updatestamp=@updatestamp"
		updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
		err = updatestampParam.SetValue(stamp)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}
		params = append(params, updatestampParam)

		command := fmt.Sprintf("UPDATE userinfos set %s where uid=@uid;", commandParams)

		uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
		err = uidParam.SetValue(reqPkt.Info.Uid)
		if err != nil {
			logs.Logger.Critical(err)
			return
		}
		params = append(params, uidParam)

		code = SetUserInfoCode_DatabaseErr
		conn, err := pool.Acquire()
		if err != nil {
			logs.Logger.Critical("Error acquiring connection: ", err)
			return
		}
		_, err = conn.Execute(command, params...)
		if err != nil {
			logs.Logger.Critical("Error executing query: ", err, " command = ", command)
		} else {
			code = SetUserInfoCode_None
		}
		pool.Release(conn)
		if code == SetUserInfoCode_None {
			UserInfoChangedNotificationChan <- reqPkt.Info.Uid
		} else {
			stamp = 0
		}
	}
	return
}

func GetUserInfo(uid int64) (info UserInfo, err error) {
	command := `
	SELECT realname, nickname, avatarfile, signature, mobile, gender, location, birthday, updatestamp FROM userinfos where uid = @uid;
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
			info.Uid = uid
			err = res.Scan(&info.RealName, &info.NickName, &info.AvatarFile, &info.Signature, &info.Mobile, &info.Gender, &info.Location, &info.Birthday, &info.UpdateStamp)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			}
		}
		res.Close()
	}
	pool.Release(conn)
	info.Account, err = GetUserAccount(uid)
	return
}

func IsUserInfoChanged(uid, stamp int64) (changed bool) {
	changed = false
	command := `
	SELECT COUNT(*) as nums FROM userinfos where uid = @uid AND updatestamp > @updatestamp;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err := uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	updatestampParam := pgsql.NewParameter("@updatestamp", pgsql.Bigint)
	err = updatestampParam.SetValue(stamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		res, err := conn.Query(command, uidParam, updatestampParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		} else if hasRow, _ := res.FetchNext(); hasRow {
			n := 0
			err = res.Scan(&n)
			if err != nil {
				logs.Logger.Critical("Error scan: ", err)
			} else {
				if n > 0 {
					changed = true
				}
			}
		}
		res.Close()
	}
	pool.Release(conn)
	return
}

func GetUserInfos(uids []int64) (infos []UserInfo, err error) {
	infos = make([]UserInfo, 0, len(uids))
	for _, uid := range uids {
		info, err := GetUserInfo(uid)
		if err != nil {
			logs.Logger.Critical(err)
			continue
		} else {
			infos = append(infos, info)
		}
	}
	return
}

func IsUserInfoExist(uid int64) (exist bool, err error) {
	exist = false
	command := `
	SELECT COUNT(*) as nums FROM userinfos where uid = @uid;
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

func UpdateUserLastLoginStamp(uid int64) (err error) {
	lastLoginStamp := time.Now().UnixNano()
	command := `
	update userinfos set LastLoginStamp=@LastLoginStamp where uid=@uid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	lastLoginStampParam := pgsql.NewParameter("@LastLoginStamp", pgsql.Bigint)
	err = lastLoginStampParam.SetValue(lastLoginStamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}

	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err = conn.Execute(command, lastLoginStampParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func UpdateUserLastLogoutStamp(uid int64) (err error) {
	lastLogoutStamp := time.Now().UnixNano()
	command := `
	update userinfos set LastLogoutStamp=@LastLogoutStamp where uid=@uid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	lastLogoutStampParam := pgsql.NewParameter("@LastLogoutStamp", pgsql.Bigint)
	err = lastLogoutStampParam.SetValue(lastLogoutStamp)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, lastLogoutStampParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	return
}

func UpdateUserAvatar(uid int64, filename string) (err error) {
	command := `
	update userinfos set AvatarFile=@AvatarFile, UpdateStamp=@UpdateStamp where uid=@uid;
	`
	uidParam := pgsql.NewParameter("@uid", pgsql.Bigint)
	err = uidParam.SetValue(uid)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	avatarFileParam := pgsql.NewParameter("@AvatarFile", pgsql.Text)
	err = avatarFileParam.SetValue(filename)
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	updateStampParam := pgsql.NewParameter("@UpdateStamp", pgsql.Bigint)
	err = updateStampParam.SetValue(time.Now().UnixNano())
	if err != nil {
		logs.Logger.Critical(err)
		return
	}
	conn, err := pool.Acquire()
	if err != nil {
		logs.Logger.Critical("Error acquiring connection: ", err)
	} else {
		_, err := conn.Execute(command, avatarFileParam, updateStampParam, uidParam)
		if err != nil {
			logs.Logger.Critical("Error execute query: ", err)
		}
	}
	pool.Release(conn)
	UserInfoChangedNotificationChan <- uid
	return
}
