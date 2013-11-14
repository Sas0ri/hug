package utils

import (
	"hug/logs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
)

//const EmailRegExpString = "^[a-z0-9]([a-z0-9]*[-_]?[a-z0-9]+)*@([a-z0-9]*[-_]?[a-z0-9]+)+[\\.][a-z]{2,3}([\\.][a-z]{2})?"
const EmailRegExpString = "^([\\w\\.\\_]{2,20})@(\\w{1,}).([a-z]{2,4})$"

func IsValidEmail(email string) (valid bool) {
	valid, _ = regexp.MatchString(EmailRegExpString, email)
	return
}

const MobileRegExpString = "^((13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$"

func IsValidMobile(mobile string) (valid bool) {
	valid, _ = regexp.MatchString(MobileRegExpString, mobile)
	return
}

func SendMail(subject, from, to, authHost, smtpServer, pass, htmlBody string, tlsRequire bool) {
	email := ComposeMail(subject, authHost, smtpServer)
	email.From = from
	email.Password = pass
	email.EnableTLS = tlsRequire
	email.AddRecipient(to)
	email.AddHtmlBody(htmlBody)
	err := email.Send()
	if err != nil {
		logs.Logger.Critical("SendMail error:", err, "to:", to, "body:", htmlBody)
	} else {
		logs.Logger.Info("SendMail successful.", "to:", to, "body:", htmlBody)
	}
}

func ApplicationPath() (path string) {
	file, _ := exec.LookPath(os.Args[0])
	path, _ = filepath.Abs(file)
	path, _ = filepath.Split(path)
	return
}

func RemoveIntSliceDuplicate(original []int64) (ret []int64) {
	ret = make([]int64, 0, len(original))
	duplicate := false
	for _, v := range original {
		for _, v2 := range ret {
			if v == v2 {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ret = append(ret, v)
		}
	}
	return
}
