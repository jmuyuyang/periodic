package driver

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/jmuyuyang/periodic/util"
)

// Job workload.
type Job struct {
	ID        int64         `json:"job_id"`
	Name      string        `json:"name"`       // The job name, this is unique.
	Func      string        `json:"func"`       // The job function reffer on worker function
	Args      string        `json:"workload"`   // Job args
	Timeout   int64         `json:"timeout"`    // Job processing timeout
	SchedAt   int64         `json:"sched_at"`   // When to sched the job.
	FailRetry int           `json:"fail_retry"` //num to retry When job fail done
	Period    string        `json:"period"`
	RunAt     int64         `json:"run_at"`  // The job is start at
	Counter   int64         `json:"counter"` // The job run counter
	Status    string        `json:"status"`
	timeCon   timeCondition `json:"_"`
}

type timeCondition struct {
	Cron  *cronexpr.Expression
	Every time.Duration
}

func (job *Job) Init() error {
	if job.Period != "" {
		if strings.Index(job.Period, "every_") == 0 {
			every, err := util.ParseDuration(strings.Trim(job.Period[6:], " "))
			if err != nil {
				return err
			}
			job.timeCon = timeCondition{
				Every: every,
			}
			job.SchedAt = time.Now().Add(every).Unix()
		} else {
			cron, err := cronexpr.Parse(job.Period)
			if err != nil {
				return err
			}
			job.timeCon = timeCondition{
				Cron: cron,
			}
			job.SchedAt = cron.Next(time.Now()).Unix()
		}
	}
	return nil
}

func (job Job) IsPeriod() bool {
	if job.Period != "" {
		return true
	}
	return false
}

// IsReady check job status ready
func (job Job) IsReady() bool {
	return job.Status == "ready"
}

// IsProc check job status processing
func (job Job) IsProc() bool {
	return job.Status == "processing"
}

func (job *Job) ResetPeriod() {
	if job.Period != "" {
		schedTime := time.Unix(job.SchedAt, 0)
		if job.timeCon.Cron == nil {
			job.SchedAt = schedTime.Add(job.timeCon.Every).Unix()
		} else {
			job.SchedAt = job.timeCon.Cron.Next(schedTime).Unix()
		}
	}
}

// SetReady set job status ready
func (job *Job) SetReady() {
	job.Status = "ready"
}

// SetProc set job status processing
func (job *Job) SetProc() {
	job.Status = "processing"
}

// NewJob create a job from json bytes
func NewJob(payload []byte) (job Job, err error) {
	err = json.Unmarshal(payload, &job)
	if err == nil {
		err = job.Init()
	}
	return
}

// Bytes encode job to json bytes
func (job Job) Bytes() (data []byte) {
	data, _ = json.Marshal(job)
	return
}
