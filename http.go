package periodic

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jmuyuyang/periodic/driver"
	"github.com/jmuyuyang/periodic/protocol"
)

var (
	bufSize = 1024
)

type httpClient struct {
	sched *Sched
	conn  net.Conn
}

func newHTTPClient(sched *Sched, conn net.Conn) (c *httpClient) {
	c = new(httpClient)
	c.conn = conn
	c.sched = sched
	return
}

func (c *httpClient) handle(header []byte) {
	defer c.conn.Close()
	writer := bytes.NewBuffer(header)
	for {
		buf := make([]byte, bufSize)
		n, err := c.conn.Read(buf)
		if err != nil {
			c.sendErrResponse(err)
			return
		}
		writer.Write(buf)
		if n < bufSize {
			break
		}
	}
	req, _ := http.ReadRequest(bufio.NewReader(writer))

	url := req.URL.String()
	funcName := url[1:]

	switch req.Method {
	case "GET":
		c.handleStatus(funcName)
		break
	case "POST":
		act := req.FormValue("act")
		if strings.ToLower(act) == "remove" {
			c.handleRemoveJob(req)
		} else {
			c.handleSubmitJob(req)
		}
		break
	case "DELETE":
		c.handleDropFunc(funcName)
		break
	default:
		c.sendResponse("400 Bad Request", nil)
		break
	}
}

func (c *httpClient) sendResponse(status string, body []byte) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("HTTP/1.1 " + status + "\r\n")
	buf.WriteString("Content-Type: application/json; charset=utf-8\r\n")
	buf.WriteString("Server: periodic/" + Version + "\r\n")
	length := len(body)
	if length > 0 {
		buf.WriteString("Content-Length: " + strconv.Itoa(length) + "\r\n")
		buf.WriteString("\r\n")
		buf.Write(body)
	}
	c.conn.Write(buf.Bytes())
}

func (c *httpClient) sendErrResponse(e error) {
	c.sendResponse("400 Bad Request",
		[]byte("{\"err\": \""+e.Error()+"\"}"))
}

func (c *httpClient) handleSubmitJob(req *http.Request) {
	var job driver.Job
	var e error
	var sched = c.sched
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()
	url := req.URL.String()
	funcName := url[1:]
	if funcName == "" {
		funcName = req.FormValue("func")
	}
	job.Name = req.FormValue("name")
	job.Func = funcName
	job.Args = req.FormValue("args")
	job.Timeout, _ = strconv.ParseInt(req.FormValue("timeout"), 10, 64)
	job.SchedAt, _ = strconv.ParseInt(req.FormValue("sched_at"), 10, 64)
	job.Period = req.FormValue("period")
	job.FailRetry, _ = strconv.Atoi(req.FormValue("fail_retry"))
	if job.Name == "" || job.Func == "" {
		c.sendErrResponse(errors.New("job name or func is required"))
		return
	}

	isNew := true
	changed := false
	job.SetReady()
	oldJob, e := sched.driver.GetOne(job.Func, job.Name)
	if e == nil && oldJob.ID > 0 {
		job.ID = oldJob.ID
		if job.IsProc() {
			sched.decrStatProc(oldJob)
			sched.removeRevertPQ(job)
			changed = true
		}
		isNew = false
	}
	e = sched.driver.Save(&job)
	if e != nil {
		c.sendErrResponse(e)
		return
	}

	if isNew {
		sched.incrStatJob(job)
	}
	if isNew || changed {
		sched.pushJobPQ(job)
	}
	sched.notifyJobTimer()
	c.sendResponse("200 OK", []byte("{\"msg\": \""+protocol.SUCCESS.String()+"\"}"))
	return
}

type sstat struct {
	FuncName    string `json:"func_name"`
	TotalWorker int    `json:"total_worker"`
	TotalJob    int    `json:"total_job"`
	Processing  int    `json:"processing"`
}

func (c *httpClient) handleStatus(funcName string) {
	defer c.sched.funcLocker.Unlock()
	c.sched.funcLocker.Lock()
	var stats = make(map[string]sstat)
	for _, st := range c.sched.stats {
		stats[st.Name] = sstat{
			FuncName:    st.Name,
			TotalWorker: st.Worker.Int(),
			TotalJob:    st.Job.Int(),
			Processing:  st.Processing.Int(),
		}
	}
	var data = []byte("{}")
	if funcName == "" {
		data, _ = json.Marshal(stats)
	} else {
		if _, ok := stats[funcName]; ok {
			data, _ = json.Marshal(stats[funcName])
		}
	}
	c.sendResponse("200 OK", data)
	return
}

func (c *httpClient) handleDropFunc(funcName string) {
	if funcName == "" {
		c.sendErrResponse(errors.New("func is required"))
		return
	}
	sched := c.sched
	defer sched.notifyJobTimer()
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()
	defer sched.funcLocker.Unlock()
	sched.funcLocker.Lock()
	stat, ok := sched.stats[funcName]
	if ok && stat.Worker.Int() == 0 {
		iter := sched.driver.NewIterator([]byte(funcName))
		var deleteJob = make([]int64, 0)
		for {
			if !iter.Next() {
				break
			}
			job := iter.Value()
			deleteJob = append(deleteJob, job.ID)
		}
		iter.Close()
		for _, jobID := range deleteJob {
			sched.driver.Delete(jobID)
		}
		delete(sched.stats, funcName)
		delete(sched.jobPQ, funcName)
	}
	c.sendResponse("200 OK", []byte("{\"msg\": \""+protocol.SUCCESS.String()+"\"}"))
	return
}

func (c *httpClient) handleRemoveJob(req *http.Request) {
	var job driver.Job
	var e error
	var sched = c.sched
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()
	url := req.URL.String()
	funcName := url[1:]
	if funcName == "" {
		funcName = req.FormValue("func")
	}
	name := req.FormValue("name")
	job, e = sched.driver.GetOne(funcName, name)
	if e == nil && job.ID > 0 {
		if _, ok := sched.procQueue[job.ID]; ok {
			delete(sched.procQueue, job.ID)
		}
		sched.driver.Delete(job.ID)
		sched.decrStatJob(job)
		if job.IsProc() {
			sched.decrStatProc(job)
			sched.removeRevertPQ(job)
		}
		sched.notifyJobTimer()
	}

	if e != nil {
		c.sendErrResponse(e)
	} else {
		c.sendResponse("200 OK", []byte("{\"msg\": \""+protocol.SUCCESS.String()+"\"}"))
	}
}
