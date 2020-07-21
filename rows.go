package sqlanywhere

//#include <driver.c>
import "C"
import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"
)

type rows struct {
	stmt    *statement
	columns []*C.a_sqlany_column_info
	names   []string
	ctx     context.Context
}

func (r *rows) Columns() []string {

	return r.names
}

func (r *rows) Close() error {
	if r.stmt == nil {
		return nil
	}

	r.stmt.reset()

	if r.stmt.closeStatementOnRowsClose {
		r.stmt.Close()
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {

	if r.ctx == nil {
		return errors.New("can't run next, because we don't have a context")
	}

	err := r.stmt.con.awaitFunc(r.ctx, r.fetch)
	if err != nil {
		return err
	}

	for i := 0; i < len(dest); i++ {
		if err := r.column(i, &dest[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *rows) fetch() error {

	if C.sqlany_fetch_next(r.stmt.ptr) == 1 {
		return nil
	}

	if r.ctx.Err() != nil {
		return io.EOF //no more rows
	}

	if err := r.stmt.con.lasterr("did not fetch next"); err != nil {
		de, ok := err.(*DriverError)
		if ok && de.code == 100 {
			return io.EOF

		}
		return err
	}

	return io.EOF
}

func (r *rows) column(i int, v *driver.Value) error {

	value := new(C.a_sqlany_data_value)

	result, errno := C.sqlany_get_column(r.stmt.ptr, C.sacapi_u32(i), value)
	if result == 0 {
		return r.stmt.con.lasterr("no data remains to be copied")
	}

	if result == -1 {
		return r.stmt.con.lasterr("failed to get column value")
	}

	if errno != nil {
		return fmt.Errorf("errorno was %v", errno)
	}

	if *value.is_null == 1 {
		return nil
	}

	if value.buffer == nil {
		return fmt.Errorf("value buffer is nil at index %v", i)
	}

	var err error

	switch r.columns[i].native_type {
	case C.DT_DOUBLE:
		*v = *(*float64)(unsafe.Pointer(value.buffer))
	case C.DT_FLOAT:
		*v = float32(*(*float64)(unsafe.Pointer(value.buffer))) //read as float64, cast to float32
	case C.DT_DECIMAL:
		*v = C.GoStringN(value.buffer, C.int(*value.length))
	case C.DT_SMALLINT:
		*v = *(*int16)(unsafe.Pointer(value.buffer))
	case C.DT_INT:
		*v = *(*int32)(unsafe.Pointer(value.buffer))
	case C.DT_BIGINT:
		*v = *(*int64)(unsafe.Pointer(value.buffer))
	case C.DT_TINYINT:
		*v = *(*uint8)(unsafe.Pointer(value.buffer))
	case C.DT_UNSSMALLINT:
		*v = *(*uint16)(unsafe.Pointer(value.buffer))
	case C.DT_UNSINT:
		*v = *(*uint32)(unsafe.Pointer(value.buffer))
	case C.DT_UNSBIGINT:
		*v = *(*uint64)(unsafe.Pointer(value.buffer))
	case C.DT_BIT:
		*v = *(*bool)(unsafe.Pointer(value.buffer))
	case C.DT_VARCHAR, C.DT_FIXCHAR, C.DT_LONGVARCHAR, C.DT_STRING, C.DT_LONGNVARCHAR:
		*v = C.GoStringN(value.buffer, C.int(*value.length))
	case C.DT_BINARY, C.DT_LONGBINARY:
		//const MaxBlobSize = 1<<31 - 1 // 2,147,483,647 bytes
		//*v = (*[MaxBlobSize]byte)(unsafe.Pointer(value.buffer))[:*value.length:*value.length]
		*v = C.GoBytes(unsafe.Pointer(value.buffer), C.int(*value.length))
	case C.DT_DATE:
		str := C.GoStringN(value.buffer, C.int(*value.length))
		*v, err = time.Parse(Date, str)
	case C.DT_TIME:
		str := C.GoStringN(value.buffer, C.int(*value.length))
		*v, err = time.Parse(Time, str)
	case C.DT_TIMESTAMP:
		str := C.GoStringN(value.buffer, C.int(*value.length))
		*v, err = time.Parse(DateTime, str)
	case C.DT_NOTYPE:
		*v = nil
	default:
		*v = nil
		err = fmt.Errorf("unexpected type %v: %s", r.columns[i].native_type, C.GoStringN(value.buffer, C.int(*value.length)))
	}

	return err
}

// HasNextResultSet is called at the end of the current result set and
// reports whether there is another result set after the current one.
func (r *rows) HasNextResultSet() bool {
	return true //it appears sqlanywhere doesn't provide a way to tell, so assume yes.
}

// NextResultSet advances the driver to the next result set even
// if there are remaining rows in the current result set.
//
// NextResultSet should return io.EOF when there are no more result sets.
func (r *rows) NextResultSet() error {

	if C.sqlany_get_next_result(r.stmt.ptr) == 1 {
		return nil
	}

	err := r.stmt.con.lasterr("did not advance to next result set:")
	if driverError, ok := err.(*DriverError); ok && driverError.code == 105 {
		return io.EOF
	}
	return err
}
