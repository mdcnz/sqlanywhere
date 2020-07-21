package sqlanywhere

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestTransactions(t *testing.T) {
	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	db, close := testdb.Open()
	defer close()

	tp := &TestPool{pool: db, t: t}

	testUnsupported(tp)
	testReadUncommitted(tp)
	testSerializableRead(tp)
	testSerializableWriteRead(tp)
}

func testUnsupported(tp *TestPool) {
	tx, err := tp.pool.BeginTx(context.TODO(), &sql.TxOptions{
		Isolation: sql.LevelWriteCommitted,
	})
	if err == nil || tx != nil {
		tp.t.Fatal("created tx with unsupported isolation level")
	}
}

func testSerializableRead(tp *TestPool) {
	tp.resetAccountTable()

	ctx, cancel := context.WithTimeout(context.Background(), 4000*time.Millisecond)
	defer cancel()

	opts := &sql.TxOptions{Isolation: sql.LevelSerializable}

	a, err := tp.pool.BeginTx(ctx, opts)
	tp.check(err)

	b, err := tp.pool.BeginTx(ctx, opts)
	tp.check(err)

	c, err := tp.pool.BeginTx(ctx, opts)
	tp.check(err)

	var first int
	err = a.QueryRowContext(ctx, "select balance from account where id=1").Scan(&first) //works
	tp.check(err)

	var second int
	err = b.QueryRowContext(ctx, "select balance from account where id=1").Scan(&second) //causes block?
	tp.check(err)

	var third int
	err = c.QueryRowContext(ctx, "select balance from account where id=1").Scan(&third) //works
	tp.check(err)

	want := 100
	if first != second || first != want {
		tp.t.Fatalf("Balances should both be %d: first) %d second) %d", want, first, second)
	}

	tp.check(a.Commit())
	tp.check(b.Commit())
	tp.check(c.Commit())

	tp.resetAccountTable() //blocked by a prev txn
}

func testSerializableWriteRead(tp *TestPool) {
	tp.resetAccountTable()

	// serializable write
	writer, err := tp.pool.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		tp.t.Fatal(err)
	}

	_, err = writer.Exec("update account set balance=50 where id=1")
	if err != nil {
		tp.t.Fatal(err)
	}

	// serializable read: blocked because writer hasn't released lock
	reader, err := tp.pool.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		tp.t.Fatal(err)
	}

	// use context to timeout the reader
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer func() {
		tp.t.Log("Cancelling....")
		cancel()
	}()

	var balance int
	err = reader.QueryRowContext(ctx, "select balance from account n where id = 1").Scan(&balance)
	if err == nil {
		panic("should never reach here")
	}

	if !strings.Contains(err.Error(), "context deadline exceeded") {
		tp.t.Fatalf("want timeout error, got %T: %q", err, err)
	}

	tp.t.Log("		connection was cancelled. Assume reader transaction is no longer usable")

	var answer int
	tp.read(writer, "select balance from account a where id=1", &answer)
	tp.t.Log("		balance is ", answer)

	writer.Commit()

	tp.resetAccountTable()
}

func testReadUncommitted(tp *TestPool) {
	tp.resetAccountTable()

	writer := tp.newTx(sql.LevelReadUncommitted)

	reader := tp.newTx(sql.LevelReadUncommitted)

	tp.write(writer, "update account set balance=75 where id=1")

	balance, want := 0, 75
	tp.read(reader, "select balance from account where id = 1", &balance)

	if balance != want {
		tp.t.Fatalf("want %v, got %v", want, balance)
	}
	writer.Commit()
	reader.Commit()
}

type TestPool struct {
	pool *sql.DB
	t    *testing.T
}

func (tp *TestPool) check(err error) {
	if err != nil {
		tp.t.Fatal("error: " + err.Error())
	}
}

func (tp *TestPool) write(tx *sql.Tx, update string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Second)
	defer cancel()
	_, err := tx.ExecContext(ctx, update)
	tp.check(err)
}

func (tp *TestPool) read(tx *sql.Tx, query string, dest ...interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	err := tx.QueryRowContext(ctx, query).Scan(dest...)
	tp.check(err)
}

func (tp *TestPool) resetAccountTable() {

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	tx, err := tp.pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	tp.check(err)

	_, err = tx.ExecContext(ctx, "create table if not exists account (id int primary key, balance int)")
	if err != nil {
		tp.t.Fatalf("create table error: %v", err)
	}

	_, err = tx.ExecContext(ctx, "delete from account")
	if err != nil {
		tp.t.Fatalf("delete error: %v", err)
	}

	_, err = tx.ExecContext(ctx, "insert into account values ((select isnull(max(id)+1,1) from account), 100)")
	if err != nil {
		tp.t.Fatalf("insert error: %v", err)
	}

	err = tx.Commit()
	tp.check(err)
}

func (tp *TestPool) newTx(level sql.IsolationLevel) *sql.Tx {
	opts := &sql.TxOptions{Isolation: level}
	tx, err := tp.pool.BeginTx(context.TODO(), opts)
	tp.check(err)
	return tx
}
