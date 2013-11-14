package jpush

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

const (
	receiver_type = 3
	msg_type      = 2
	platform      = "android"
	server        = "http://api.jpush.cn:8800/sendmsg/v2/sendmsg"
	time_to_live  = 864000
)

type sendArg struct {
	alias   string
	payload Payload
	err     chan<- error
}

// An Apn contain a ErrorChan channle when connected to apple server. When a notification sent wrong, you can get the error infomation from this channel.
type JPush struct {
	app_key      string
	masterSecret string
	sendno       int

	sendChan chan *sendArg
	quit     chan bool
}

// New JPush with app_key
func New(app_key string, masterSecret string) (jpush *JPush) {
	jpush = &JPush{
		app_key:      app_key,
		masterSecret: masterSecret,
		sendno:       1,
		sendChan:     make(chan *sendArg, 1024),
		quit:         make(chan bool),
	}
	go jpush.sendLoop()
	return
}

// Send a notification to iOS
func (j *JPush) Send(alias string, payload Payload) error {
	err := make(chan error)
	arg := &sendArg{
		alias:   alias,
		payload: payload,
		err:     err,
	}
	j.append(arg)
	return <-err
}

func (j *JPush) append(arg *sendArg) {
	j.sendChan <- arg
}

func (j *JPush) Close() {
	j.quit <- true

}

func (j *JPush) send(alias string, payload Payload) error {
	j.sendno++
	v := url.Values{}
	v.Set("sendno", strconv.Itoa(j.sendno))
	v.Set("app_key", j.app_key)
	v.Set("platform", platform)
	v.Set("receiver_type", strconv.Itoa(receiver_type))
	v.Set("msg_type", strconv.Itoa(msg_type))
	v.Set("time_to_live", strconv.Itoa(time_to_live))

	var verifycode string
	verifycode = strconv.Itoa(j.sendno)
	verifycode += strconv.Itoa(receiver_type)
	verifycode += alias
	verifycode += j.masterSecret
	h := md5.New()
	io.WriteString(h, verifycode)
	verifycode = fmt.Sprintf("%x", h.Sum(nil))
	v.Set("verification_code", verifycode)

	v.Set("receiver_value", alias)

	cotBytes, err := json.Marshal(payload)
	if err != nil {
		log.Println("JPush send notification error: json marshal error", err)
	}
	v.Set("msg_content", string(cotBytes))

	_, err = http.PostForm(server, v)
	if err != nil {
		log.Println("JPush send notification error: http post error", err)
		return err
	}
	return nil
}

func (j *JPush) sendLoop() {
	for {
		select {
		case <-j.quit:
			return
		case arg := <-j.sendChan:
			arg.err <- j.send(arg.alias, arg.payload)
		}
	}
}
