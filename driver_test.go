package sqlanywhere

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestTemporaryDatabase(t *testing.T) {

	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	t.Run("bad connection", func(t *testing.T) {
		testBadConnectionString(testdb)
	})

	pool, close := testdb.Open()
	defer close()

	if err := pool.Ping(); err != nil {
		testdb.t.Fatalf("did not ping test db: %v", err)
	}

	t.Run("datatypes", func(t *testing.T) {
		dbTestDataTypes(pool, t)
	})
	t.Run("charset", func(t *testing.T) {
		testCharSet(pool, t)
	})
	t.Run("blob", func(t *testing.T) {
		testBlob(pool, t)
	})
	t.Run("lastInsertId", func(t *testing.T) {
		testLastInsertID(pool, t)
	})
}

func testBadConnectionString(testdb *TestDatabase) {
	badpool, err := sql.Open(DriverName, testdb.BadConnectionString())
	if err != nil {
		testdb.t.Fatalf("did not open db")
	}

	if err := badpool.Ping(); err == nil {
		testdb.t.Fatalf("able to ping bad db! %v", err)
	}
}

type TestDatabase struct {
	utility  *sql.DB
	name     string
	filename string
	t        *testing.T
}

func (test *TestDatabase) ConnectionString() string {
	return fmt.Sprintf("uid=dba;pwd=sqlsql;dbn=%s;servername=sqlanywhere-db-server;charset=utf-8;", test.name)
}

func (test *TestDatabase) BadConnectionString() string {
	return fmt.Sprintf("uid=xxx;pwd=xxx;dbn=%s;servername=sqlanywhere-db-server;charset=utf-8;", test.name)
}

func (test *TestDatabase) Stop() {
	_, err := test.utility.Exec(fmt.Sprintf("stop database %s UNCONDITIONALLY", test.name))
	if err != nil {
		test.t.Fatalf("stop %s: %v", test.name, err)
	}
}

func (test *TestDatabase) Drop() {
	_, err := test.utility.Exec(fmt.Sprintf("drop database '%s'", test.filename))
	if err != nil {
		test.t.Fatalf("drop %s: %v", test.name, err)
	}
}

func (test *TestDatabase) OpenUtility() {
	//An existing connection is required to create a new database.
	//If no databases exist yet, use the built in utility_db database.
	//For personal server, any password will work. For network server,
	//password is minimum 6 characters,
	//set as a command line parameter when the server is started,
	//eg: dbsrv12 -n TestEng -su dba,sqlsql
	utility, err := sql.Open(DriverName, "uid=dba;pwd=sqlsql;dbn=utility_db;servername=sqlanywhere-db-server;charset=utf-8;")
	if err != nil {
		test.t.Fatalf("did not open utility db: %v", err)
	}

	if err := utility.Ping(); err != nil {
		test.t.Fatalf("did not ping utility db: %v", err)
	}

	test.utility = utility
}

func (test *TestDatabase) CloseUtility() {
	if err := test.utility.Close(); err != nil {
		test.t.Fatalf("err during close of utility db: %v", err)
	}
}

func (test *TestDatabase) Create() {
	_, err := test.utility.Exec(fmt.Sprintf(`CREATE DATABASE '%s' DBA USER 'dba' DBA PASSWORD 'sqlsql' ENCODING 'UTF-8' COLLATION 'UCA' NCHAR COLLATION 'UCA'`, test.filename))
	if err != nil {
		test.t.Fatalf("did not create test db: %v", err)
	}
}

func (test *TestDatabase) Start() {
	_, err := test.utility.Exec(fmt.Sprintf(`START DATABASE '%s' AS %s AUTOSTOP OFF`, test.filename, test.name))
	if err != nil {
		test.t.Fatalf("did not start test db: %v", err)
	}
}

func (test *TestDatabase) Open() (*sql.DB, func()) {
	pool, err := sql.Open(DriverName, test.ConnectionString())
	if err != nil {
		test.t.Fatalf("did not open testdb: %v", err)
	}

	cleanup := func() {
		if err := pool.Close(); err != nil {
			test.t.Errorf("error while closing testdb: %v", err)
		}
	}

	return pool, cleanup
}

func (test *TestDatabase) Cleanup() {

	test.Stop()

	test.Drop()

	test.CloseUtility()
}

//NewTestDB creates a new random test database. It should be cleaned up with Cleanup() on exit.
//Creating a database is relatively slow, about 2.5 seconds, so reuse them for faster tests.
func NewTestDB(t *testing.T) *TestDatabase {

	name := fmt.Sprintf("sqlany_test_%d", randInt(math.MaxInt64))

	filename := filepath.Join(os.TempDir(), name+".db")

	//a db file may be left over from prior failed test; remove it
	if _, err := os.Stat(filename); os.IsExist(err) {
		if err := os.Remove(filename); err != nil {
			t.Fatalf("remove test file %q: %v", filename, err)
		}
	}

	testdb := &TestDatabase{
		name:     name,
		filename: filename,
		t:        t,
	}

	testdb.OpenUtility()

	testdb.Create()

	testdb.Start()

	return testdb
}
