package connections

import (
	"errors"
	"math/rand"
)

const (
	Pkt_STX uint8 = 0x55
	Pkt_ETX uint8 = 0x04
	Pkt_DLE uint8 = 0x05
)

const (
	Pkt_Type_None uint8 = iota
	Pkt_Type_Request
	Pkt_Type_Response
	Pkt_Type_Error
)

const (
	Pkt_Encrypt_Seed uint8  = 0xA5
	Pkt_Max_Data_Len uint32 = 0xFFFFFF
	Pkt_Max_Len      int    = int(Pkt_Max_Data_Len) + 4
)

const (
	Pkt_Key_Index = iota
	Pkt_Cmd_INdex
	Pkt_Type_Index
	Pkt_Code_Index
	Pkt_Sid_Index1
	Pkt_Sid_Index2
	Pkt_Data_Index
)

type Packet struct {
	Key     uint8
	Cmd     uint8
	PktType uint8
	Code    int8
	Sid     uint16
	Data    []byte
	Conn    *ClientConnection
}

func UnStreamPacket(buf []byte) (packet Packet, err error) {
	packet.Cmd = 0
	if len(buf) < Pkt_Data_Index {
		err = errors.New("Received data too small")
		return
	}
	packet.Key = buf[Pkt_Key_Index]
	decryptKey := buf[Pkt_Key_Index] ^ Pkt_Encrypt_Seed
	for i := int(Pkt_Key_Index + 1); i < len(buf); i++ {
		buf[i] = buf[i] ^ decryptKey
	}
	packet.Cmd = buf[Pkt_Cmd_INdex]
	packet.PktType = buf[Pkt_Type_Index]
	packet.Code = int8(buf[Pkt_Code_Index])
	packet.Sid = uint16(buf[Pkt_Sid_Index1])
	packet.Sid = packet.Sid << 8
	packet.Sid += uint16(buf[Pkt_Sid_Index2])
	if len(buf) > Pkt_Data_Index {
		packet.Data = buf[Pkt_Data_Index:]
	}
	return
}

func StreamPacket(cmd uint8, pktType uint8, code int8, sid uint16, data []byte) (buf []byte, err error) {
	buf = make([]byte, 0, Pkt_Data_Index+len(data))
	buf = append(buf, byte(rand.Intn(256)))
	encryptKey := buf[Pkt_Key_Index] ^ Pkt_Encrypt_Seed
	buf = append(buf, cmd)
	buf = append(buf, pktType)
	buf = append(buf, byte(code))
	sid1 := byte((sid & 0xFF00) >> 8)
	sid2 := byte(sid & 0xFF)
	buf = append(buf, sid1)
	buf = append(buf, sid2)
	buf = append(buf, data...)
	for i := int(Pkt_Key_Index + 1); i < len(buf); i++ {
		buf[i] = buf[i] ^ encryptKey
	}
	return
}

func PrepareSendPacket(cmd uint8, pktType uint8, code int8, sid uint16, data []byte) (buf []byte, err error) {
	buf, err = StreamPacket(cmd, pktType, code, sid, data)
	if err != nil {
		return
	}
	buf, err = PrepareSendData(buf)
	return
}

// func randKey() (key uint8) {
// 	key = uint8(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(256))
// 	return
// }

func PrepareSendData(buf []byte) (sendBuf []byte, err error) {
	if len(buf) < Pkt_Data_Index {
		err = errors.New("Prepare send data error: buf is too small")
		return
	}
	sendBuf = make([]byte, 0, len(buf))
	sendBuf = append(sendBuf, Pkt_STX)
	sendBuf = append(sendBuf, Pkt_STX)
	for i := 0; i < len(buf); i++ {
		if buf[i] == Pkt_STX || buf[i] == Pkt_DLE || buf[i] == Pkt_ETX {
			sendBuf = append(sendBuf, Pkt_DLE)
		}
		sendBuf = append(sendBuf, buf[i])
	}
	sendBuf = append(sendBuf, Pkt_ETX)
	return
}

func ParseReceivedData(buf *[]byte) (packet Packet, found bool) {
	found = false
	startIndex := -1
	for i, v := range *buf {
		if v == Pkt_STX {
			startIndex = i
			break
		}
	}
	if startIndex > 0 {
		*buf = (*buf)[startIndex:]
		if len(*buf) < Pkt_Data_Index {
			return
		}
	} else if startIndex < 0 {
		*buf = []byte{}
		return
	}
	var stxCount uint8
STARTFIND:
	packetBuf := make([]byte, 0, 256)
	stxCount = 0
	for i := 0; i < len(*buf); i++ {
		dataByte := (*buf)[i]
		if stxCount < 2 {
			if dataByte == Pkt_STX {
				stxCount++
			} else {
				*buf = (*buf)[i+1:]
				goto STARTFIND
			}
		} else {
			if dataByte == Pkt_STX {
				*buf = (*buf)[i:]
				goto STARTFIND
			} else if dataByte == Pkt_DLE {
				i++
				if i < len(*buf) {
					dataByte = (*buf)[i]
					packetBuf = append(packetBuf, dataByte)
				} else {
					return
				}
			} else if dataByte == Pkt_ETX {
				if i < (len(*buf) - 1) {
					*buf = (*buf)[i+1:]
				} else {
					*buf = []byte{}
				}
				if len(packetBuf) >= Pkt_Data_Index {
					packet, err := UnStreamPacket(packetBuf)
					if err == nil {
						//log.Println("ParseReceivedData: packet received data =", string(packet.Data), "buf cache len =", len(*buf), "buf cache data:", *buf)
						return packet, true
					}
				}
				goto STARTFIND
			} else {
				packetBuf = append(packetBuf, dataByte)
			}
		}
	}
	return

}
