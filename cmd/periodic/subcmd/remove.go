package subcmd

import (
	"log"

	"github.com/jmuyuyang/go-periodic"
)

// RemoveJob cli remove
func RemoveJob(entryPoint, funcName, name string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	if err := c.RemoveJob(funcName, name); err != nil {
		log.Fatal(err)
	}
	log.Printf("Remove Job[%s] success.\n", name)
}
