package sqlanywhere

import "time"

//DateTime The Go time format .9 fractional seconds accepts any number of fractional second digits (up to 9)
const DateTime = "2006-01-02 15:04:05.9"

//Date is a standard date format understood by sqlanywhere
const Date = "2006-01-02"

//Time is a standard time format understood by sqlanywhere
const Time = "15:04:05.9"

func timeToString(t time.Time) string {
	y, m, d := t.Date()
	if y == 0 && m == 1 && d == 1 {
		return t.Format(Time)
	}
	return t.Format(DateTime)
}
