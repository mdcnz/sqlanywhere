package sqlanywhere

import (
	"crypto/sha256"
	"sync"
	"testing"
)

func TestSimultaneousConnections(t *testing.T) {

	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	pool, close := testdb.Open()
	defer close()

	hasher := sha256.New()

	drop := createBlobTable(t, pool, hasher)
	defer drop()

	insertBlobs(t, pool, hasher)

	var wg sync.WaitGroup

	//TODO i > 10 exceeds max db connection limit
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			gopool, goclose := testdb.Open()
			defer goclose()

			testSelectBlobs(t, gopool, hasher)
		}()
	}

	wg.Wait()
}
