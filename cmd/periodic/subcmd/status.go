package subcmd

import (
	"fmt"
	"log"

	"github.com/gosuri/uitable"
	"github.com/jmuyuyang/go-periodic"
)

// ShowStatus cli status
func ShowStatus(entryPoint string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	stats, _ := c.Status()
	table := uitable.New()
	table.MaxColWidth = 50

	table.AddRow("FUNCTION", "WORKERS", "JOBS", "PROCESSING")
	for _, stat := range stats {
		table.AddRow(stat[0], stat[1], stat[2], stat[3])
	}
	fmt.Println(table)
}
