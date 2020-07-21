package sqlanywhere

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
)

type Tester interface {
	RunTest(*testing.T, func(params))
}

type saDB struct {
	once    sync.Once
	running bool
}

var sa Tester = &saDB{}

const TablePrefix = "gosqltest_"

func (s *saDB) RunTest(t *testing.T, fn func(params)) {
	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	db, close := testdb.Open()
	defer close()

	params := params{sa, t, db}

	fn(params)
}

// type oracleDB struct {
// 	once    sync.Once // guards init of running
// 	running bool      // whether port 1521 is listening
// }

// func (o *oracleDB) Running() bool {
// 	o.once.Do(func() {
// 		c, err := net.Dial("tcp", "localhost:1521")
// 		if err == nil {
// 			o.running = true
// 			c.Close()
// 		}
// 	})
// 	return o.running
// }

// func (o *oracleDB) RunTest(t *testing.T, fn func(params)) {
// 	if !o.Running() {
// 		t.Logf("skipping test; no Oracle running on localhost:1521")
// 		return
// 	}
// 	db, err := sql.Open("goracle", os.Getenv("GOSQLTEST_ORACLE"))
// 	if err != nil {
// 		t.Fatalf("error connecting: %v", err)
// 	}
// 	defer db.Close()

// 	params := params{oracle, t, db}

// 	// Drop all tables in the test database.
// 	rows, err := db.Query(`SELECT table_name FROM user_tables
// 		WHERE UPPER(table_name) LIKE UPPER('` + TablePrefix + `%')`)
// 	if err != nil {
// 		t.Fatalf("failed to enumerate tables: %v", err)
// 	}
// 	var table sql.NullString
// 	for rows.Next() {
// 		err = rows.Scan(&table)
// 		// t.Logf("Next => table=%+v err=%s", table, err)
// 		if err != nil {
// 			t.Fatalf("error reading table name: %s", err)
// 		} else if !table.Valid {
// 			t.Fatalf("error reading table name: null value!")
// 		} else {
// 			params.mustExec("DROP TABLE " + table.String)
// 		}
// 	}

// 	fn(params)
// }

type params struct {
	dbType Tester
	*testing.T
	*sql.DB
}

func (t params) mustExec(sql string, args ...interface{}) sql.Result {
	res, err := t.DB.Exec(sql, args...)
	if err != nil {
		t.Fatalf("Error running %q: %v", sql, err)
	}
	return res
}

var qrx = regexp.MustCompile(`\?`)

// q converts "?" characters to $1, $2, $n on postgres, :1, :2, :n on Oracle
func (t params) q(sql string) string {
	var pref string
	switch t.dbType {
	default:
		return sql
	}
	n := 0
	return qrx.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return pref + strconv.Itoa(n)
	})
}

func sqlBlobParam(t params, size int) string {
	return fmt.Sprintf("VARBINARY (%d)", size)
}

func TestBlobs_SQLAnywhere(t *testing.T) { sa.RunTest(t, testBlobs) }

func testBlobs(t params) {
	var blob = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	t.mustExec("create table " + TablePrefix + "foo (id integer primary key, bar " + sqlBlobParam(t, 16) + ")")
	t.mustExec(t.q("insert into "+TablePrefix+"foo (id, bar) values(?,?)"), 0, blob)

	want := fmt.Sprintf("%x", blob)

	b := make([]byte, 16)
	err := t.QueryRow(t.q("select bar from "+TablePrefix+"foo where id = ?"), 0).Scan(&b)
	got := fmt.Sprintf("%x", b)
	if err != nil {
		t.Errorf("[]byte scan: %v", err)
	} else if got != want {
		t.Errorf("for []byte, got %q; want %q", got, want)
	}

	err = t.QueryRow(t.q("select bar from "+TablePrefix+"foo where id = ?"), 0).Scan(&got)
	want = string(blob)
	if err != nil {
		t.Errorf("string scan: %v", err)
	} else if got != want {
		t.Errorf("for string, got %q; want %q", got, want)
	}
}

func TestManyPreparedQueryRow_SQLAnywhere(t *testing.T) { sa.RunTest(t, testManyPreparedQueryRow) }

func testManyPreparedQueryRow(t params) {
	if testing.Short() {
		t.Logf("skipping in short mode")
		return
	}
	t.mustExec("create table " + TablePrefix + "foo (id integer primary key, name varchar(50))")
	t.mustExec(t.q("insert into "+TablePrefix+"foo (id, name) values(?,?)"), 1, "bob")

	statement, err := t.Prepare(t.q("select name from " + TablePrefix + "foo where id = ?"))
	if err != nil {
		t.T.Fatal("did not prepare select query: ", err)
	}

	var name string
	for i := 0; i < 10000; i++ {
		err := statement.QueryRow(1).Scan(&name)
		if err != nil || name != "bob" {
			t.Fatalf("on query %d: err=%v, name=%q", i, err, name)
		}
	}

	if err := statement.Close(); err != nil {
		t.T.Fatal("did not close statement", err)
	}
}

func TestManyQueryRow_SQLAnywhere(t *testing.T) { sa.RunTest(t, testManyQueryRow) }
func testManyQueryRow(t params) {

	const MAX = 500

	t.Log(MAX, "MAX, if > 50 prepared statements will cause error: -685: Resource governor for 'prepared statements' exceeded")

	if testing.Short() {
		t.Logf("skipping in short mode")
		return
	}
	t.mustExec("create table " + TablePrefix + "foo (id integer primary key, name varchar(50))")
	t.mustExec(t.q("insert into "+TablePrefix+"foo (id, name) values(?,?)"), 1, "bob")

	var name string
	for i := 0; i < MAX; i++ {

		// query fails on sqlanywhere, because queryrow opens many prepared statements and doesn't close them.
		// the driver can't be sure the statement won't be used later so can't close it yet.
		//    --- FAIL: TestManyQueryRow_SQLAnywhere (3.60s)
		//    sql_test.go:169: on query 50: err=did not prepare statement: -685: Resource governor for 'prepared statements' exceeded, name="bob"

		// sqlanywhere has resources per prepared statement, so they're limited to 50 (by default)
		// so we need to close them. But QueryRow might prepare one, and although rows are closed, no way to tell if the prepared statement should
		// be closed too, because go connection pool maintains a reference to the prepared statements (via the connection).
		err := t.QueryRow(t.q("select name from "+TablePrefix+"foo where id = ?"), 1).Scan(&name)
		if err != nil || name != "bob" {
			t.Fatalf("on query %d: err=%v, name=%q", i, err, name)
		}
	}
}

func TestTxQuery_SQLAnywhere(t *testing.T) { sa.RunTest(t, testTxQuery) }

func testTxQuery(t params) {
	tx, err := t.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = t.DB.Exec("create table " + TablePrefix + "foo (id integer primary key, name varchar(50))")
	if err != nil {
		t.Logf("cannot drop table "+TablePrefix+"foo: %s", err)
	}

	_, err = tx.Exec(t.q("insert into "+TablePrefix+"foo (id, name) values(?,?)"), 1, "bob")
	if err != nil {
		t.Fatal(err)
	}

	r, err := tx.Query(t.q("select name from "+TablePrefix+"foo where id = ?"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
		t.Fatal("expected one rows")
	}

	var name string
	err = r.Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedStmt_SQLAnywhere(t *testing.T) { sa.RunTest(t, testPreparedStmt) }

func testPreparedStmt(t params) {
	t.mustExec("CREATE TABLE " + TablePrefix + "t (count INT)")
	sel, err := t.Prepare("SELECT count FROM " + TablePrefix + "t ORDER BY count DESC")
	if err != nil {
		t.Fatalf("prepare 1: %v", err)
	}
	defer sel.Close()

	ins, err := t.Prepare(t.q("INSERT INTO " + TablePrefix + "t (count) VALUES (?)"))
	if err != nil {
		t.Fatalf("prepare 2: %v", err)
	}
	defer ins.Close()

	for n := 1; n <= 3; n++ {
		if _, err := ins.Exec(n); err != nil {
			t.Fatalf("insert(%d) = %v", n, err)
		}
	}

	const nRuns = 10
	ch := make(chan bool)
	for i := 0; i < nRuns; i++ {
		go func() {
			defer func() {
				ch <- true
			}()
			for j := 0; j < 10; j++ {
				count := 0
				if err := sel.QueryRow().Scan(&count); err != nil && err != sql.ErrNoRows {
					t.Errorf("Query: %v", err)
					return
				}
				if _, err := ins.Exec(rand.Intn(100)); err != nil {
					t.Errorf("Insert: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < nRuns; i++ {
		<-ch
	}
}

func getenvOk(k string) (v string, ok bool) {
	v = os.Getenv(k)
	if v != "" {
		return v, true
	}
	keq := k + "="
	for _, kv := range os.Environ() {
		if kv == keq {
			return "", true
		}
	}
	return "", false
}
