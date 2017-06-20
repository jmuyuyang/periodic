package stat

import (
	"strconv"
	"sync/atomic"
)

// Counter defined a counter
type Counter struct {
	c int64
}

// NewCounter create a counter
func NewCounter(c int) *Counter {
	var counter = new(Counter)
	return counter
}

// Incr the counter
func (c *Counter) Incr() {
	atomic.AddInt64(&c.c, 1)
}

// Decr the counter
func (c *Counter) Decr() {
	for {
		i := c.Int()
		if i-1 > 0 {
			if atomic.CompareAndSwapInt64(&c.c, i, i-1) {
				return
			}
		} else {
			if atomic.CompareAndSwapInt64(&c.c, i, 0) {
				return
			}
		}
	}
}

func (c *Counter) String() string {
	return strconv.FormatInt(c.Int(), 10)
}

// Int return the counter value
func (c *Counter) Int() int64 {
	return atomic.LoadInt64(&c.c)
}
