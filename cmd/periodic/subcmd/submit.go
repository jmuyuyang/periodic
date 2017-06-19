package subcmd

import (
	"log"

	"github.com/jmuyuyang/go-periodic"
)

// SubmitJob cli submit
func SubmitJob(entryPoint, funcName, name string, opts map[string]string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	if err := c.SubmitJob(funcName, name, opts); err != nil {
		log.Fatal(err)
	}
	log.Printf("Submit Job[%s] success.\n", name)
}
