package sqlanywhere

import "fmt"

//DriverErrorCodeEOF is the error code representing end of results
const DriverErrorCodeEOF = 100

//DriverError is an error returned by calls to a connection
type DriverError struct {
	prefix  string
	message string
	code    int
}

func (err *DriverError) Error() string {
	return fmt.Sprintf("%s: %s %d", err.prefix, err.message, err.code)
}
