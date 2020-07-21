package sqlanywhere

//#include <driver.c>
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
)

type tx struct {
	con           *connection
	opts          driver.TxOptions
	lastIsolation driverIsolationLevel
}

type driverIsolationLevel string

const (
	levelReadUncommitted           driverIsolationLevel = "0"
	levelReadCommitted                                  = "1"
	levelRepeatableRead                                 = "2"
	levelSerializable                                   = "3"
	levelSnapshot                                       = "snapshot"
	levelStatementSnapshot                              = "statement-snapshot"
	levelReadOnlyStatementSnapshot                      = "readonly-statement-snapshot"
)

//TODO set snapshot isolation level, see: http://dcx.sap.com/1200/en/dbusage/transact-s-3847634.html
var isolationLevels = map[sql.IsolationLevel]driverIsolationLevel{
	sql.LevelDefault:         levelReadUncommitted,
	sql.LevelReadUncommitted: levelReadUncommitted,
	sql.LevelReadCommitted:   levelReadCommitted,
	// sql.LevelWriteCommitted
	sql.LevelRepeatableRead: levelRepeatableRead,
	sql.LevelSnapshot:       levelSnapshot,
	sql.LevelSerializable:   levelSerializable,
	// sql.LevelLinearizable
}

func (t *tx) Commit() error {
	defer t.restoreIsolationLevel()

	if C.sqlany_commit(t.con.ptr) == 0 {
		return t.con.lasterr("did not commit")
	}

	return nil
}

func (t *tx) Rollback() error {
	defer func() {
		if err := t.restoreIsolationLevel(); err != nil {
			log.Println("error during restore in rollback", err)
		}
	}()

	var err error

	if err := t.con.lasterr("last err"); err != nil {
		panic(err)
	}

	if C.sqlany_rollback(t.con.ptr) == 0 {
		err = t.con.lasterr("did not rollback")
		if err != nil {
			log.Println("error during rollback", err)
		}
		return err
	}

	return err
}

func (t *tx) restoreIsolationLevel() error {
	err := t.con.execImmediate("SET TEMPORARY OPTION isolation_level = " + string(t.lastIsolation))
	if err != nil {
		return fmt.Errorf("did not set isolation level: %v", err)
	}
	return nil
}
