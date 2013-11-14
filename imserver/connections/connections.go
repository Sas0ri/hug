package connections

import (
	"fmt"
	"hug/core/devices"
	"hug/core/users"
	"hug/logs"
	"io"
	"net"
	"time"
)

const (
	AuthDuration      = 5 * time.Second
	KeepAliveDuration = 300 * time.Second
	AuthPacketMaxSize = 1024
)

type ClientConnection struct {
	conn              net.Conn
	AuthInfo          users.AuthInfo
	Shutdown          chan bool
	kill              chan bool
	packetChan        chan Packet
	WriteChan         chan []byte
	receivedBufChache []byte
	socketBuf         []byte
	Identifier        int64
	authed            chan bool
}

func New(conn net.Conn, packetChan chan Packet) {
	c := &ClientConnection{
		conn:              conn,
		Shutdown:          make(chan bool),
		kill:              make(chan bool),
		packetChan:        packetChan,
		WriteChan:         make(chan []byte, 8),
		receivedBufChache: make([]byte, 0, 256),
		socketBuf:         make([]byte, 256, 256),
		Identifier:        time.Now().UnixNano(),
		authed:            make(chan bool),
	}
	c.AuthInfo.Account = ""
	c.AuthInfo.AuthCode = users.AuthCode_WaitAuth
	go c.Listen()
	return

}

func (c *ClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *ClientConnection) Listen() {
	defer func() {
		if err := recover(); err != nil {
			logs.Logger.Info("Connection terminated: ", err, " user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
			if c.AuthInfo.AuthCode == users.AuthCode_None {
				KilledPresenceChan <- c
				if c.AuthInfo.IosDevice.IsValid() {
					devices.SetIosDeviceStatus(c.AuthInfo.IosDevice.Token, devices.IosDeviceStatus_Background)
				} else if c.AuthInfo.AndroidDevice.IsValid() {
					devices.SetIosDeviceStatus(c.AuthInfo.AndroidDevice.Alias, devices.AndroidDeviceStatus_Background)
				}
			}
			c.quitLoops()
			logs.Logger.Info("Delete connection: ", c.conn.RemoteAddr())
		}
	}()

	go c.listenBeforeAuth(c.kill)
	go c.writeSocketLoop(c.kill)

	select {
	case <-c.Shutdown:
		panic("Shutting down connection.")
	}
}

func (c *ClientConnection) quitLoops() {
	logs.Logger.Info("quiteloop connection user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	select {
	case c.kill <- true:
		logs.Logger.Info("kill connection user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	case <-time.After(time.Second):
		logs.Logger.Info("kill connection timeout user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	}

	select {
	case c.kill <- true:
		logs.Logger.Info("kill connection user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	case <-time.After(time.Second):
		logs.Logger.Info("kill connection timeout user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	}
	c.conn.Close()

}

func (c *ClientConnection) SetAuthResult(info users.AuthInfo) {
	// defer func() {
	// 	logs.Logger.Info("quit SetAuthResult user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	// }()
	if info.AuthCode == users.AuthCode_None {
		c.AuthInfo = info
		NewPresenceChan <- c
		c.authed <- true
		logs.Logger.Info("New connection authed. user:", info.Account, " addr:", c.RemoteAddr())
	} else {
		logs.Logger.Info("New connection authed failed. user:", info.Account, " addr:", c.RemoteAddr(), " code:", info.AuthCode)
		go c.Close()
	}
}

func (c *ClientConnection) handlePacketIn(packet Packet) {
	//log.Println("handlePacketIn:", c.conn.RemoteAddr())
	// defer func() {
	// 	logs.Logger.Info("quit handlePacketIn user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	// }()
	select {
	case c.packetChan <- packet:
		logs.Logger.Info("Received packet cmd = ", fmt.Sprintf("0x%02x", packet.Cmd), " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr(), " data: ", string(packet.Data))
	}

}

func (c *ClientConnection) Write(data []byte) {
	select {
	case c.WriteChan <- data:
	case <-time.After(time.Second):
	}

}

func (c *ClientConnection) WritePacket(cmd uint8, pktType uint8, code int8, sid uint16, data []byte) (err error) {
	var wtBuf []byte
	logs.Logger.Info("Conn write packet: cmd: ", fmt.Sprintf("0x%02x", cmd), " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr(), " data = ", string(data))
	wtBuf, err = StreamPacket(cmd, pktType, code, sid, data)
	if err != nil {
		return
	}
	wtBuf, err = PrepareSendData(wtBuf)
	if err != nil {
		return
	}
	c.Write(wtBuf)
	return
}

func (c *ClientConnection) Close() {
	logs.Logger.Info("Start close connection: user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
	select {
	case c.Shutdown <- true:

	case <-time.After(1 * time.Second):

	}
}

func (c *ClientConnection) readSocket() (n int, err error) {
	//logs.Logger.Info("readSocket")
	c.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err = c.conn.Read(c.socketBuf)
	if err != nil {
		if err == io.EOF {
			go c.Close()
		}
		return
	}
	if n == 0 {
		return
	}
	//logs.Logger.Info("read socket: ", n, " bytes ：", c.socketBuf[:n])
	c.receivedBufChache = append(c.receivedBufChache, c.socketBuf[:n]...)

	if c.AuthInfo.AuthCode != users.AuthCode_None && len(c.receivedBufChache) > AuthPacketMaxSize {
		go c.Close()
	} else if len(c.receivedBufChache) >= Pkt_Data_Index {
		for {
			packet, found := ParseReceivedData(&(c.receivedBufChache))
			if found {
				packet.Conn = c
				go c.handlePacketIn(packet)
			} else {
				break
			}
		}
	}

	return
}

func (c *ClientConnection) listenBeforeAuth(quit chan bool) {
	defer func() {
		logs.Logger.Info("quit listenBeforeAuth user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
	}()
	timer := time.NewTimer(AuthDuration)
	for {
		select {
		case <-quit:
			logs.Logger.Info("kill listenBeforeAuth user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			return
		case <-c.authed:
			go c.listenAfterAuth(quit)
			logs.Logger.Info("quit listenBeforeAuth when authed user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			return
		case <-timer.C:
			logs.Logger.Info("Auth timeout addr: ", c.conn.RemoteAddr())
			go c.Close()
			return
		default:
			_, err := c.readSocket()
			if err == io.EOF {
				logs.Logger.Info("quit listenBeforeAuth when read error: ", err, " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
				return
			}
		}
	}
}

func (c *ClientConnection) listenAfterAuth(quit chan bool) {
	defer func() {
		logs.Logger.Info("quit listenAfterAuth user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
	}()
	timer := time.NewTimer(KeepAliveDuration)
	for {
		select {
		case <-quit:
			logs.Logger.Info("kill listenAfterAuth user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			return
		case <-timer.C:
			logs.Logger.Info("keepalive timeout user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			go c.Close()
			return
		default:
			n, err := c.readSocket()
			if err == nil && n > 0 {
				timer.Reset(KeepAliveDuration)
			} else if err == io.EOF {
				logs.Logger.Info("quit listenAfterAuth when read error: ", err, " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
				return
			}
		}
	}
}

func (c *ClientConnection) writeSocketLoop(quit chan bool) {
	defer func() {
		logs.Logger.Info("quit writeSocketLoop user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
	}()
	for {
		select {
		case <-quit:
			logs.Logger.Info("kill writeSocketLoop user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			return
		case data := <-c.WriteChan:
			c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			n, err := c.conn.Write(data)
			if err != nil {
				logs.Logger.Warn("write socket error: ", err, " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			} else if n != len(data) {
				logs.Logger.Warn("wrote socket error: wrote n = ", n, " but datalen = ", len(data), " user:", c.AuthInfo.Account, " addr:", c.conn.RemoteAddr())
			} else {
				logs.Logger.Info("Wrote socket n = ", n, " user: ", c.AuthInfo.Account, " addr: ", c.conn.RemoteAddr())
			}
		}
	}
}
