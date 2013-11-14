package connections

import (
	"hug/logs"
	"sync"
)

type Presence struct {
	Terminals map[int16](*ClientConnection)
}

var presences map[int64](*Presence)
var NewPresenceChan chan *ClientConnection
var KilledPresenceChan chan *ClientConnection
var ConflictConnChan chan *ClientConnection
var presencesLock sync.RWMutex

func StartManagePresences() {
	presences = make(map[int64](*Presence))
	NewPresenceChan = make(chan *ClientConnection, 128)
	KilledPresenceChan = make(chan *ClientConnection, 128)
	ConflictConnChan = make(chan *ClientConnection, 16)
	go manageLoop()
}

func newPresence(uid int64) (p *Presence) {
	logs.Logger.Info("new presence uid = ", uid)
	p = &Presence{
		Terminals: make(map[int16](*ClientConnection)),
	}
	presencesLock.RLock()
	presences[uid] = p
	presencesLock.RUnlock()
	return
}

func getPresence(uid int64) (p *Presence) {
	p, ok := presences[uid]
	if ok == false {
		p = newPresence(uid)
	}
	return
}

func manageLoop() {
	for {
		select {
		case c := <-NewPresenceChan:
			insertNewConnection(c)
		case c := <-KilledPresenceChan:
			removeKilledConnection(c)
		}
	}
}

func removeKilledConnection(conn *ClientConnection) {
	p := FindPresences(conn.AuthInfo.Uid)
	if p != nil {
		c, exist := p.Terminals[conn.AuthInfo.TerminalType]
		if exist && c.Identifier == conn.Identifier {
			logs.Logger.Info("remove killed conn", " addr:", conn.RemoteAddr(), " info:", conn.AuthInfo.String())
			presencesLock.RLock()
			delete(p.Terminals, conn.AuthInfo.TerminalType)
			presencesLock.RUnlock()
		}
		if len(p.Terminals) == 0 {
			presencesLock.RLock()
			delete(presences, conn.AuthInfo.Uid)
			presencesLock.RUnlock()
		}
	}
}

func insertNewConnection(conn *ClientConnection) {
	logs.Logger.Info("insert new conn.", " addr:", conn.RemoteAddr(), " info:", conn.AuthInfo.String())
	presence := getPresence(conn.AuthInfo.Uid)
	oldConn, ok := presence.Terminals[conn.AuthInfo.TerminalType]
	presence.Terminals[conn.AuthInfo.TerminalType] = conn
	if ok {
		logs.Logger.Info("Connection Conflict.", " user:", conn.AuthInfo.Account, " addr:", conn.RemoteAddr())
		ConflictConnChan <- oldConn
	}

}

func FindPresences(uid int64) (p *Presence) {
	p, ok := presences[uid]
	if ok == false {
		p = nil
	}
	return
}
