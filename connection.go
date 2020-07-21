package sqlanywhere

//#include <driver.c>
import "C"
import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

//connection implements driver.Conn
type connection struct {
	ptr    *C.a_sqlany_connection
	errbuf [C.SACAPI_ERROR_SIZE]byte
	valid  bool
}

func (con *connection) IsValid() bool {
	return con.valid && con.ptr != nil
}

func (con *connection) connect(ctx context.Context, name string) error {
	err := con.awaitFunc(ctx, func() error {
		str := C.CString(name)
		defer C.free(unsafe.Pointer(str))

		if C.sqlany_connect(con.ptr, str) == 0 {
			err := con.lasterr("did not connect")
			C.sqlany_free_connection(con.ptr)
			con.valid = false
			return err
		}

		con.valid = true
		return nil
	})

	return err
}

func (con *connection) Ping(ctx context.Context) error {
	if !con.IsValid() {
		return driver.ErrBadConn
	}
	return nil
}

func (con *connection) lasterr(prefix string) error {
	defer C.sqlany_clear_error(con.ptr)
	buf := con.errbuf[:]
	code := C.sqlany_error(con.ptr, (*C.char)(unsafe.Pointer(&buf[0])), C.SACAPI_ERROR_SIZE)
	if code == 0 {
		return nil
	}
	if code == DriverErrorCodeEOF {
		return io.EOF
	}

	if pos := bytes.IndexByte(buf, 0); pos >= 0 {
		buf = buf[:pos]
	}
	return &DriverError{prefix, string(buf), int(code)}
}

func (con *connection) Close() error {
	var err error
	if C.sqlany_disconnect(con.ptr) != 0 { //any uncommitted txns rolled back.
		err = con.lasterr("disconnect")
	}
	C.sqlany_free_connection(con.ptr)

	con.valid = false

	sacapi.connectionClosed()

	return err
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before. If the driver returns ErrBadConn
// the connection is discarded.
func (con *connection) ResetSession(ctx context.Context) error {
	if con == nil {
		return driver.ErrBadConn
	}

	if !con.valid {
		return driver.ErrBadConn
	}

	return nil
}

//cancel cancels an outstanding request on the connection
func (con *connection) cancel() {
	C.sqlany_cancel(con.ptr)
}

//awaitFunc runs a function with opportunity to cancel via the given context
func (con *connection) awaitFunc(ctx context.Context, run func() error) error {
	// avoid spawning a goroutine if the context cannot be cancelled.
	if ctx.Done() == nil {
		return run()
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	done := make(chan error)

	go func() {
		done <- run()
	}()

	select {
	case err := <-done:
		return err

	case <-ctx.Done():
		//context finished first; send cancellation to interrupt 'run' function
		con.cancel()

		//wait for interrupted run to finish, but discard it's error
		discardErr := <-done
		_ = discardErr
		return ctx.Err()
	}
}

//execImmediate executes a SQL statement with no arguments and no results.
func (con *connection) execImmediate(sql string) error {
	str := C.CString(sql)
	defer C.free(unsafe.Pointer(str))
	if C.sqlany_execute_immediate(con.ptr, str) == 0 {
		return con.lasterr("did not execute")
	}

	return nil
}

func (con *connection) execImmediateContext(ctx context.Context, sql string) error {
	return con.awaitFunc(ctx, func() error {
		return con.execImmediate(sql)
	})
}

func (con *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		stmt, err := con.PrepareContext(ctx, query)
		if err != nil {
			stmt.Close()
			return nil, err
		}
		result, err := stmt.(*statement).ExecContext(ctx, query, args)
		stmt.Close() //ensure statement is freed
		return result, err
	}

	err := con.execImmediateContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &result{con: con}, nil
}

//execDirect executes a SQL statement with no arguments, returning a statement to access results, if any.
func (con *connection) execDirect(query string) (*statement, error) {
	str := C.CString(query)
	defer C.free(unsafe.Pointer(str))

	ptr := C.sqlany_execute_direct(con.ptr, str)
	if ptr == nil {
		return nil, con.lasterr("did not execute direct")
	}
	return &statement{con: con, ptr: ptr, closeStatementOnRowsClose: true}, nil
}

func (con *connection) execDirectContext(ctx context.Context, query string) (*statement, error) {
	var stmt *statement

	err := con.awaitFunc(ctx, func() error {
		var err error
		stmt, err = con.execDirect(query)
		return err
	})

	return stmt, err
}

func (con *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		stmt, err := con.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		stmt.(*statement).closeStatementOnRowsClose = true
		return stmt.(*statement).QueryContext(ctx, args)
	}
	stmt, err := con.execDirectContext(ctx, query)
	if err != nil {
		return nil, err
	}

	stmt.closeStatementOnRowsClose = true

	return stmt.newRows(ctx)
}

func (con *connection) prepare(query string) (*statement, error) {
	if con.ptr == nil {
		return nil, fmt.Errorf("con.ptr is nil")
	}

	queryWithoutNamedParameters, args := splitNamed(query)

	sql := C.CString(queryWithoutNamedParameters)
	defer C.free(unsafe.Pointer(sql))

	ptr := C.sqlany_prepare(con.ptr, sql)
	if ptr == nil {
		return nil, con.lasterr("did not prepare statement")
	}
	s := &statement{con: con, ptr: ptr, closeStatementOnRowsClose: false, args: args}

	return s, nil
}

func (con *connection) Prepare(query string) (driver.Stmt, error) {
	return con.prepare(query)
}

func (con *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var stmt *statement

	err := con.awaitFunc(ctx, func() error {
		var err error
		stmt, err = con.prepare(query)
		return err
	})

	return stmt, err
}

//option queries the property of the current connection and returns the value as a string.
//Connection specific properties can only be relied on during exclusive use of a connection,
//such as during a transaction.
func (con *connection) option(property string) (string, error) {
	sql := fmt.Sprintf("SELECT CONNECTION_PROPERTY('%s')", property)
	str := C.CString(sql)
	defer C.free(unsafe.Pointer(str))
	stmt := C.sqlany_execute_direct(con.ptr, str)
	if stmt == nil {
		return "", con.lasterr(sql)
	}
	defer C.sqlany_free_stmt(stmt)

	ncols := int(C.sqlany_num_cols(stmt))
	if ncols != 1 {
		return "", fmt.Errorf("did not read connection option, ncols != 1: %v", con.lasterr(""))
	}

	if C.sqlany_fetch_next(stmt) == 0 {
		return "", con.lasterr(sql)
	}
	var val C.a_sqlany_data_value
	if C.sqlany_get_column(stmt, 0, &val) == 0 {
		return "", con.lasterr(sql)
	}

	result := C.GoStringN(val.buffer, C.int(*val.length))
	if result == "" {
		return "", fmt.Errorf("no result for connection property '%s': check the property name", property)
	}
	return result, nil
}

//Begin begins a transaction with default options
func (con *connection) Begin() (driver.Tx, error) {
	return con.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (con *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, fmt.Errorf("unsupported transaction option: read only")
	}

	//BEGIN TRANSACTION is only required if auto commit was turned on.
	//Otherwise, transactions are started implicitly.
	if err := con.execImmediate("BEGIN TRANSACTION"); err != nil {
		return nil, err
	}

	lastIsolation, err := con.setIsolation(opts.Isolation)
	if err != nil {
		return nil, err
	}

	return &tx{con: con, opts: opts, lastIsolation: *lastIsolation}, nil
}

//setIsolation sets the current connection isolation level
func (con *connection) setIsolation(level driver.IsolationLevel) (*driverIsolationLevel, error) {

	isolation, supported := isolationLevels[sql.IsolationLevel(level)]
	if !supported {
		return nil, fmt.Errorf("unsupported transaction option: isolation level: %v", level)
	}

	previous, err := con.option("isolation_level")
	if err != nil {
		return nil, err
	}

	if err := con.execImmediate("SET TEMPORARY OPTION isolation_level = " + string(isolation)); err != nil {
		return nil, err
	}

	was := driverIsolationLevel(previous)
	return &was, nil
}

//queryInt fetches an int64 directly
func (con *connection) queryInt(query string) (int64, error) {
	if con == nil || con.ptr == nil {
		return -1, errors.New("connection is nil")
	}

	q := C.CString(query)
	defer C.free(unsafe.Pointer(q))

	s := C.sqlany_execute_direct(con.ptr, q)
	if s == nil {
		return -1, con.lasterr("did not exec")
	}
	defer C.sqlany_free_stmt(s)

	if C.sqlany_fetch_absolute(s, 1) == 0 {
		return -1, con.lasterr("did not fetch")
	}

	value := new(C.a_sqlany_data_value)
	if C.sqlany_get_column(s, 0, value) == 0 {
		return -1, con.lasterr("did not get value")
	}

	buf := C.GoBytes(unsafe.Pointer(value.buffer), C.int(*value.length))
	id := int64(binary.LittleEndian.Uint32(buf))

	return id, nil
}
