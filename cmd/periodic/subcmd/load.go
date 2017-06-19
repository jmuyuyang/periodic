package subcmd

import (
	"log"
	"os"

	"github.com/jmuyuyang/go-periodic"
)

// Load cli load
func Load(entryPoint, input string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	var fp *os.File
	var err error
	if fp, err = os.Open(input); err != nil {
		log.Fatal(err)
	}

	defer fp.Close()
	if err = c.Load(fp); err != nil {
		log.Fatal(err)
	}
}
