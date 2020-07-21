package sqlanywhere

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestNamedSQL(t *testing.T) {
	cases := []struct {
		sql      string
		wantSQL  string
		wantArgs []string
	}{
		{
			"",
			"",
			[]string{},
		},
		{
			"hello",
			"hello",
			[]string{},
		},
		{
			"select * from a where name = :name",
			"select * from a where name = ?",
			[]string{"name"},
		},
		{
			"select * from b where name = ':name'",
			"select * from b where name = ':name'",
			[]string{},
		},
		{
			"select * from c where name = '\n:name'",
			"select * from c where name = '\n:name'",
			[]string{},
		},
		{
			"select * from d where name = ':name' and other = :name",
			"select * from d where name = ':name' and other = ?",
			[]string{"name"},
		},
		{
			"select * from e where name = '\n:name' and age = :age",
			"select * from e where name = '\n:name' and age = ?",
			[]string{"age"},
		},
		{
			"select * from f where name = :name and age = :age",
			"select * from f where name = ? and age = ?",
			[]string{"name", "age"},
		},
		{
			"select * from g where name = :name\nand secondname = :name",
			"select * from g where name = ?\nand secondname = ?",
			[]string{"name", "name"},
		},
		{
			`select * from h where name = :name and content = 'o\'rielly:xxx' and something='hello' and age = :age`,
			`select * from h where name = ? and content = 'o\'rielly:xxx' and something='hello' and age = ?`,
			[]string{"name", "age"},
		},
		{
			`select * from i where name = :name:age`,
			`select * from i where name = ??`,
			[]string{"name", "age"},
		},
		{
			`select * from k where name = : x`,
			`select * from k where name = : x`,
			[]string{},
		},
		{
			`:`,
			`:`,
			[]string{},
		},
		{
			`:one`,
			`?`,
			[]string{"one"},
		},
		{
			`select 1 from m where age = :age	and tab=true`,
			`select 1 from m where age = ?	and tab=true`,
			[]string{"age"},
		},
		{
			`select 1 from n where age = :age5 or numbers = false`,
			`select 1 from n where age = ? or numbers = false`,
			[]string{"age5"},
		},
		{
			`select 1 from o where age = :age_5 or numbers = false`,
			`select 1 from o where age = ? or numbers = false`,
			[]string{"age_5"},
		},
		{
			`select 1 from p where age = :\nstuff`,
			`select 1 from p where age = :\nstuff`,
			[]string{},
		},
		{
			`select 1 from p where age = :stuff\n`,
			`select 1 from p where age = ?\n`,
			[]string{"stuff"},
		},
		{
			`select 1 from p where age = :st''uff\n`,
			`select 1 from p where age = ?''uff\n`,
			[]string{"st"},
		},
		{
			`'select this is :entirely within quotes\n'`,
			`'select this is :entirely within quotes\n'`,
			[]string{},
		},
	}

	for i, c := range cases {
		gotSQL, gotArgs := splitNamed(c.sql)
		if gotSQL != c.wantSQL {
			t.Fatalf("\ncase %d\nwant: %q %v\n got: %q %v", i+1, c.wantSQL, c.wantArgs, gotSQL, gotArgs)
		}

		if !reflect.DeepEqual(gotArgs, c.wantArgs) {
			t.Fatalf("want %#v, but got %#v", c.wantArgs, gotArgs)
		}
	}
}

func TestQueryNamedParam(t *testing.T) {
	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	db, close := testdb.Open()
	defer close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("did not begin transaction %v", err)
	}

	_, err = tx.Exec(`create table person(
		id uniqueidentifier primary key default newid(), 
		name varchar(100),
		age unsigned smallint
	)`)
	if err != nil {
		t.Fatalf("did not create person table: %v", err)
	}
	defer func() {
		_, err := tx.Exec("drop table person")
		if err != nil {
			t.Fatalf("did not drop person table: %v", err)
		}
	}()

	_, err = tx.Exec(`insert into person (name, age) values('Edmund Hillary', 33)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(`commit`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.Query(
		"select name, age from person where name = :name and age = :age",
		sql.Named("age", 33),
		sql.Named("name", "Edmund Hillary"),
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	defer rows.Close()

	var gotName string
	var gotAge int
	for rows.Next() {
		err = rows.Scan(&gotName, &gotAge)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("Err: %v", err)
	}

	wantName := "Edmund Hillary"
	wantAge := 33

	if gotAge != wantAge {
		t.Errorf("AGE mismatch.\n got: %#v\nwant: %#v", gotAge, wantAge)
	}

	if gotName != wantName {
		t.Errorf("Name mismatch.\n got: %#v\nwant: %#v", gotName, wantName)
	}
}
