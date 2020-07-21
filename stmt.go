package sqlanywhere

//#include <driver.c>
//sacapi_bool YES = 1;
//sacapi_bool NO = 0;
import "C"
import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"
)

type statement struct {
	con                       *connection
	ptr                       *C.a_sqlany_stmt
	args                      []string
	params                    []*C.a_sqlany_bind_param
	closeStatementOnRowsClose bool
	freed                     bool
}

func (stmt *statement) Close() error {
	if stmt == nil {
		return nil
	}

	if stmt.freed {
		//second call to statement.Close(), statement already closed, return to avoid double free & signal abort
		return nil
	}

	if stmt.ptr == nil {
		return errors.New("stmt.ptr is nil")
	}

	C.sqlany_free_stmt(stmt.ptr)
	stmt.freed = true
	return nil
}

func (stmt *statement) NumInput() int {
	return int(C.sqlany_num_params(stmt.ptr))
}

func (stmt *statement) indexOfArgName(name string) int {
	for i, arg := range stmt.args {
		if arg == name {
			return i + 1
		}
	}
	return -1
}

func (stmt *statement) reset() error {
	if C.sqlany_reset(stmt.ptr) == 0 {
		return stmt.con.lasterr("did not reset statement")
	}

	return nil
}

func (stmt *statement) freeParams() {
	for _, param := range stmt.params {
		if param.value.length != nil {
			C.free(unsafe.Pointer(param.value.length))
		}
		if param.value.buffer != nil {
			C.free(unsafe.Pointer(param.value.buffer))
		}
	}

	stmt.params = stmt.params[:0]
}

func (stmt *statement) exec(args []driver.NamedValue) error {

	if len(stmt.args) > 0 {
		for i := 0; i < len(args); i++ {
			v := args[i] //watch out! variable shadowing
			if v.Name != "" {

				ordinal := stmt.indexOfArgName(v.Name)
				if ordinal > 0 {
					args[i].Ordinal = ordinal //watch out!
				}
			}
		}
	}

	defer stmt.freeParams()

	for _, namedValue := range args {
		index := namedValue.Ordinal - 1
		value := namedValue.Value

		cindex := C.sacapi_u32(index)

		param := new(C.a_sqlany_bind_param)

		if C.sqlany_describe_bind_param(stmt.ptr, cindex, param) == 0 {
			return stmt.con.lasterr("did not describe bind param: ")
		}

		if value == nil {
			param.value.is_null = &C.YES
			if C.sqlany_bind_param(stmt.ptr, cindex, param) == 0 {
				return stmt.con.lasterr(fmt.Sprintf("did not bind parameter at index %d", cindex))
			}

			return nil
		}
		param.value.is_null = &C.NO

		stmt.params = append(stmt.params, param)

		param.value.length = (*C.size_t)(unsafe.Pointer(C.calloc(1, C.sizeof_size_t))) //must free later

		switch v := value.(type) {
		case bool:
			param.value._type = C.A_VAL8
			*param.value.length = 1
			param.value.buffer = (*C.char)(unsafe.Pointer(C.calloc(1, 1))) //zeroed = false by default

			if v {
				*param.value.buffer = 1
			}

		case int64:
			param.value._type = C.A_VAL64
			*param.value.length = 8
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(v))
			param.value.buffer = (*C.char)(C.CBytes(buf[:]))

		case float64:
			param.value._type = C.A_DOUBLE
			*param.value.length = 8
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
			param.value.buffer = (*C.char)(C.CBytes(buf[:]))

		case string:
			param.value._type = C.A_STRING
			*param.value.length = C.size_t(len(v))
			param.value.buffer = C.CString(v)

		case []byte:
			param.value._type = C.A_BINARY
			*param.value.length = C.size_t(len(v))
			param.value.buffer = (*C.char)(C.CBytes(v))

		case time.Time:
			param.value._type = C.A_STRING
			s := timeToString(v)
			*param.value.length = C.size_t(len(s))
			param.value.buffer = (*C.char)(C.CString(s))

		default:
			return fmt.Errorf("did not create param at index %d: no binding for value type: %T", index, value)
		}

		if C.sqlany_bind_param(stmt.ptr, cindex, param) == 0 {
			return stmt.con.lasterr(fmt.Sprintf("did not bind parameter at index %d", index))
		}
	}

	if C.sqlany_execute(stmt.ptr) == 0 {
		return stmt.con.lasterr("did not exec")
	}

	return nil
}

func asNamedArgs(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return named
}

func (stmt *statement) Exec(args []driver.Value) (driver.Result, error) {

	var r *result
	named := asNamedArgs(args)
	if err := stmt.exec(named); err != nil {
		return r, err
	}
	r = &result{stmt: stmt}
	return r, nil
}

func (stmt *statement) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {

	err := stmt.con.awaitFunc(ctx, func() error {
		return stmt.exec(args)
	})
	return &result{stmt: stmt}, err
}

func (stmt *statement) Query(args []driver.Value) (driver.Rows, error) {
	named := asNamedArgs(args)
	if err := stmt.exec(named); err != nil {
		return nil, err
	}
	return stmt.newRows(context.Background())
}

func (stmt *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	err := stmt.con.awaitFunc(ctx, func() error {
		return stmt.exec(args)
	})

	if err != nil {
		return nil, err
	}

	return stmt.newRows(ctx)
}

func (stmt *statement) newRows(ctx context.Context) (driver.Rows, error) {
	ncols := int(C.sqlany_num_cols(stmt.ptr))
	r := &rows{
		stmt:    stmt,
		columns: make([]*C.a_sqlany_column_info, ncols),
		names:   make([]string, ncols),
		ctx:     ctx,
	}
	for i := 0; i < ncols; i++ {
		r.columns[i] = new(C.a_sqlany_column_info)
		if C.sqlany_get_column_info(r.stmt.ptr, C.sacapi_u32(i), r.columns[i]) == 0 {
			return nil, r.stmt.con.lasterr("did not get column info")
		}
		r.names[i] = C.GoString(r.columns[i].name)
	}
	return r, nil
}
