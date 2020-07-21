package sqlanywhere

//#cgo CFLAGS: -Wall -Wno-unused -Werror
//#cgo LDFLAGS: -l:libdbtasks17_r.so -l:libdblib17_r.so -l:libdbcapi_r.so
//#include <driver.c>
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"unsafe"
)

//DriverName is the registered name of this driver
const DriverName = "sqlanywhere"

//sacapi is the driver instance
var sacapi *Driver

func init() {
	sacapi = &Driver{}
	sql.Register(DriverName, sacapi)
}

//Driver is the interface to the database c api defined in sacapi.h
type Driver struct {
	mu          sync.Mutex
	connections uint
	ptr         *C.a_sqlany_interface_context
}

//Open returns a new connection to the database, implementing driver.Driver.Open
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

//OpenConnector returns a new connector, initialising the driver api interface if necessary
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ptr == nil {

		lang := C.CString("PHP") //one of "PHP", "PERL" or "RUBY" (see sacapi.h)
		defer C.free(unsafe.Pointer(lang))

		var maxVersion C.sacapi_u32

		d.ptr = C.sqlany_init_ex(lang, C._SACAPI_VERSION, &maxVersion)
		if d.ptr == nil {
			return nil, fmt.Errorf("did not initialise api, requested version %d, max version %d", C._SACAPI_VERSION, maxVersion)
		}
	}

	return &connector{name: name}, nil
}

func (d *Driver) connectionClosed() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.connections--

	if d.connections == 0 {
		C.sqlany_fini_ex(d.ptr)
		d.ptr = nil
	}
}

type connector struct {
	name string
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	sacapi.mu.Lock()
	defer sacapi.mu.Unlock()

	ptr := C.sqlany_new_connection_ex(sacapi.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("did not create a new connection")
	}
	con := &connection{ptr: ptr}

	err := con.connect(ctx, c.name)

	if err == nil {
		sacapi.connections++
	}

	return con, err
}

func (c *connector) Driver() driver.Driver {
	return sacapi
}
