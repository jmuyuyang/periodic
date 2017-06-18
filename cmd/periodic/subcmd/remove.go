package subcmd

import (
	"bytes"
	"fmt"
	"github.com/jmuyuyang/periodic/driver"
	"github.com/jmuyuyang/periodic/protocol"
	"log"
	"net"
	"strings"
)

// RemoveJob cli remove
func RemoveJob(entryPoint string, job driver.Job) {
	parts := strings.SplitN(entryPoint, "://", 2)
	c, err := net.Dial(parts[0], parts[1])
	if err != nil {
		log.Fatal(err)
	}
	conn := protocol.NewClientConn(c)
	defer conn.Close()
	err = conn.Send(protocol.TYPECLIENT.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	var msgID = []byte("100")
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(protocol.NullChar)
	buf.WriteByte(byte(protocol.REMOVEJOB))
	buf.Write(protocol.NullChar)
	buf.Write(job.Bytes())
	err = conn.Send(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	payload, err := conn.Receive()
	if err != nil {
		log.Fatal(err)
	}
	_, cmd, _ := protocol.ParseCommand(payload)
	fmt.Printf("%s\n", cmd.String())
}
