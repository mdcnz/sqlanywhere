package sqlanywhere

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"hash"
	"hash/fnv"
	"math/big"
	"testing"
)

func randInt(n int64) int64 {
	max := big.NewInt(n)
	i, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	return i.Int64()
}

func testBlob(pool *sql.DB, t *testing.T) {
	hasher := fnv.New64()

	dropBlobTable := createBlobTable(t, pool, hasher)
	defer dropBlobTable()

	insertBlobs(t, pool, hasher)

	testSelectBlobs(t, pool, hasher)
}

func createBlobTable(t *testing.T, pool *sql.DB, hasher hash.Hash) func() {
	var err error

	_, err = pool.Exec(`create table blob_table(
		id unsigned int default autoincrement primary key, 
		hash long binary,
		blob long binary
	)`)
	if err != nil {
		t.Fatalf("did not insert blob table: %v", err)
	}

	dropper := func() {
		_, err := pool.Exec("drop table blob_table")
		if err != nil {
			t.Fatalf("did not drop blob table: %v", err)
		}
	}

	return dropper
}

func insertBlobs(t *testing.T, pool *sql.DB, hasher hash.Hash) {
	const MaxBytes = 1 << 25
	const CountBlobs = 10
	const MB = 1 << 20

	var count int64 = 1
	for ; count <= CountBlobs; count++ {
		n := randInt(MaxBytes / CountBlobs)
		buf := make([]byte, n)

		rand.Read(buf)

		hash := hasher.Sum(buf)

		_, err := pool.Exec("insert into blob_table (hash, blob) values(?, ?)", hash, buf)
		if err != nil {
			t.Fatalf("did not insert blob: %v", err)
		}

		if _, err := pool.Exec("commit"); err != nil {
			t.Fatalf(" did not commit: %v", err)
		}
	}
}

func testSelectBlobs(t *testing.T, db *sql.DB, hasher hash.Hash) {
	var id int
	var hash []byte
	var blob []byte

	rows, err := db.Query("select id, hash, blob from blob_table")
	if err != nil {
		t.Errorf("did not query blob: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &hash, &blob)
		if err != nil {
			t.Errorf("did not scan blob row: %v", err)
			return
		}
		want := hash
		got := hasher.Sum(blob)
		if !bytes.Equal(want, got) {
			t.Errorf("blob hash mismatch, want %v, got %v", want, got)
			return
		}
	}

	if err := rows.Err(); err != nil {
		t.Errorf("error after rows.Next(): %v", err)
		return
	}
}
