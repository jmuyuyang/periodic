package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/jmuyuyang/periodic"
	"github.com/jmuyuyang/periodic/cmd/periodic/subcmd"
	"github.com/jmuyuyang/periodic/driver"
	"github.com/jmuyuyang/periodic/driver/leveldb"
	"github.com/jmuyuyang/periodic/driver/redis"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "periodic"
	app.Usage = "Periodic task system"
	app.Version = periodic.Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "H",
			Value:  "unix:///tmp/periodic.sock",
			Usage:  "the server address eg: tcp://127.0.0.1:5000",
			EnvVar: "PERIODIC_PORT",
		},
		cli.StringFlag{
			Name:  "redis",
			Value: "tcp://127.0.0.1:6379",
			Usage: "The redis server address, required for driver redis",
		},
		cli.StringFlag{
			Name:  "driver",
			Value: "memstore",
			Usage: "The driver [memstore, leveldb, redis]",
		},
		cli.StringFlag{
			Name:  "dbpath",
			Value: "leveldb",
			Usage: "The db path, required for driver leveldb",
		},
		cli.BoolFlag{
			Name:  "d",
			Usage: "Enable daemon mode",
		},
		cli.IntFlag{
			Name:  "timeout",
			Value: 0,
			Usage: "The socket timeout",
		},
		cli.IntFlag{
			Name:   "cpus",
			Value:  runtime.NumCPU(),
			Usage:  "The runtime.GOMAXPROCS",
			EnvVar: "GOMAXPROCS",
		},
		cli.StringFlag{
			Name:  "cpuprofile",
			Value: "",
			Usage: "write cpu profile to file",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "status",
			Usage: "Show status",
			Action: func(c *cli.Context) error {
				subcmd.ShowStatus(c.GlobalString("H"))
				return nil
			},
		},
		{
			Name:  "submit",
			Usage: "Submit job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "function name",
				},
				cli.StringFlag{
					Name:  "n",
					Value: "",
					Usage: "job name",
				},
				cli.StringFlag{
					Name:  "args",
					Value: "",
					Usage: "job workload",
				},
				cli.StringFlag{
					Name:  "t",
					Value: "0",
					Usage: "job running timeout",
				},
				cli.IntFlag{
					Name:  "sched_later",
					Value: 0,
					Usage: "job sched_later",
				},
				cli.StringFlag{
					Name:  "period",
					Value: "",
					Usage: "job running period,example: every_5s",
				},
			},
			Action: func(c *cli.Context) error {
				var name = c.String("n")
				var funcName = c.String("f")
				var opts = map[string]string{
					"args":    c.String("args"),
					"timeout": c.String("t"),
					"period":  c.String("period"),
				}
				if len(name) == 0 || len(funcName) == 0 {
					cli.ShowCommandHelp(c, "submit")
					log.Fatal("Job name and func is require")
				}
				delay := c.Int("sched_later")
				var now = time.Now()
				var schedAt = int64(now.Unix()) + int64(delay)
				opts["schedat"] = strconv.FormatInt(schedAt, 10)
				subcmd.SubmitJob(c.GlobalString("H"), funcName, name, opts)
				return nil
			},
		},
		{
			Name:  "remove",
			Usage: "Remove job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "function name",
				},
				cli.StringFlag{
					Name:  "n",
					Value: "",
					Usage: "job name",
				},
			},
			Action: func(c *cli.Context) error {
				var name = c.String("n")
				var funcName = c.String("f")
				if len(name) == 0 || len(funcName) == 0 {
					cli.ShowCommandHelp(c, "remove")
					log.Fatal("Job name and func is require")
				}
				subcmd.RemoveJob(c.GlobalString("H"), funcName, name)
				return nil
			},
		},
		{
			Name:  "drop",
			Usage: "Drop func",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "function name",
				},
			},
			Action: func(c *cli.Context) error {
				Func := c.String("f")
				if len(Func) == 0 {
					cli.ShowCommandHelp(c, "drop")
					log.Fatal("function name is required")
				}
				subcmd.DropFunc(c.GlobalString("H"), Func)
				return nil
			},
		},
		{
			Name:  "run",
			Usage: "Run func",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "function name required",
				},
				cli.StringFlag{
					Name:  "exec",
					Value: "",
					Usage: "command required",
				},
				cli.IntFlag{
					Name:  "n",
					Value: runtime.NumCPU() * 2,
					Usage: "the size of goroutines. (optional)",
				},
			},
			Action: func(c *cli.Context) error {
				Func := c.String("f")
				exec := c.String("exec")
				n := c.Int("n")
				if len(Func) == 0 {
					cli.ShowCommandHelp(c, "run")
					log.Fatal("function name is required")
				}
				if len(exec) == 0 {
					cli.ShowCommandHelp(c, "run")
					log.Fatal("command is required")
				}
				subcmd.Run(c.GlobalString("H"), Func, exec, n)
				return nil
			},
		},
		{
			Name:  "dump",
			Usage: "Dump database to file.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "o",
					Value: "dump.db",
					Usage: "output file name required",
				},
			},
			Action: func(c *cli.Context) error {
				subcmd.Dump(c.GlobalString("H"), c.String("o"))
				return nil
			},
		},
		{
			Name:  "load",
			Usage: "Load file to database.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "i",
					Value: "dump.db",
					Usage: "input file name required",
				},
			},
			Action: func(c *cli.Context) error {
				subcmd.Load(c.GlobalString("H"), c.String("i"))
				return nil
			},
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.Bool("d") {
			if c.String("cpuprofile") != "" {
				f, err := os.Create(c.String("cpuprofile"))
				if err != nil {
					log.Fatal(err)
				}
				pprof.StartCPUProfile(f)
				defer pprof.StopCPUProfile()
			}
			var store driver.StoreDriver
			switch c.String("driver") {
			case "memstore":
				store = driver.NewMemStroeDriver()
				break
			case "redis":
				store = redis.NewDriver(c.String("redis"))
				break
			case "leveldb":
				store = leveldb.NewDriver(c.String("dbpath"))
				break
			default:
				store = driver.NewMemStroeDriver()
				break
			}

			runtime.GOMAXPROCS(c.Int("cpus"))
			timeout := time.Duration(c.Int("timeout"))
			periodicd := periodic.NewSched(c.String("H"), store, timeout)
			go periodicd.Serve()
			s := make(chan os.Signal, 1)
			signal.Notify(s, os.Interrupt, os.Kill)
			<-s
			periodicd.Close()
		} else {
			cli.ShowAppHelp(c)
		}
		return nil
	}

	app.Run(os.Args)
}
