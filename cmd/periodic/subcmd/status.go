package subcmd

import (
	"bytes"
	"fmt"
	"github.com/jmuyuyang/periodic/protocol"
	"github.com/gosuri/uitable"
	"log"
	"net"
	"sort"
	"strings"
)

// ShowStatus cli status
func ShowStatus(entryPoint string) {
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
	buf.Write(protocol.STATUS.Bytes())
	err = conn.Send(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	payload, err := conn.Receive()
	if err != nil {
		log.Fatal(err)
	}
	_parts := bytes.SplitN(payload, protocol.NullChar, 2)
	if len(_parts) != 2 {
		err := fmt.Sprint("ParseCommand InvalID %v\n", payload)
		panic(err)
	}
	stats := strings.Split(string(_parts[1]), "\n")
	sort.Strings(stats)
	table := uitable.New()
	table.MaxColWidth = 50

	table.AddRow("FUNCTION", "WORKERS", "JOBS", "PROCESSING")
	for _, stat := range stats {
		if stat == "" {
			continue
		}
		line := strings.Split(stat, ",")
		table.AddRow(line[0], line[1], line[2], line[3])
	}
	fmt.Println(table)
}
