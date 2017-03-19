package replication

import (
	"fmt"
	"strings"
	"time"
)

var (
	fracTimeFormat []string
	zeroTimeString []string
)

// Time is a help structure wrapping Golang Time.
// In practice, MySQL use "0000-00-00 00:00:00" for zero time, but Golang Time can't
// handle it, so we supply a Time instead.
// Notice: we don't check time and frac validation here, maybe later.
type Time struct {
	time.Time

	// Frac must in [0, 6]
	Frac int
}

func (t Time) String() string {
	if t.IsZero() {
		return zeroTimeString[t.Frac]
	}

	return t.Format(fracTimeFormat[t.Frac])
}

func init() {
	fracTimeFormat = make([]string, 7)
	zeroTimeString = make([]string, 7)
	fracTimeFormat[0] = "2006-01-02 15:04:05"
	zeroTimeString[0] = "0000-00-00 00:00:00"

	for i := 1; i <= 6; i++ {
		fracTimeFormat[i] = fmt.Sprintf("2006-01-02 15:04:05.%s", strings.Repeat("0", i))
		zeroTimeString[i] = fmt.Sprintf("0000-00-00 00:00:00.%s", strings.Repeat("0", i))
	}
}
