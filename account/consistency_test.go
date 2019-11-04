package account

import (
	"sync"
	"testing"
)

type consistencyTestFixture struct {
	store       eventStore
	repo        Repository
	aggregateId AggregateId
}

func openAccount(t *testing.T) *consistencyTestFixture {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	ownerId := OwnerId{2}
	err := repo.Open(id, ownerId)
	expectNoError(t, err)

	return &consistencyTestFixture{&store, *repo, id}
}

func TestConcurrentDeposits(t *testing.T) {
	fixture := openAccount(t)

	operationCount := 50
	concurrentUsers := 8

	depositWithRetryOnConcurrentModification := func(wg *sync.WaitGroup) {
		for {
			err := fixture.repo.Deposit(fixture.aggregateId, 1)
			if err == nil {
				break
			}
			if err.Error() != "Concurrent modification error" {
				t.Error("Expecting only concurrent modification errors")
			}
		}
		wg.Done()
	}

	for i := 0; i < operationCount; i++ {
		wg := sync.WaitGroup{}
		for j := 0; j < concurrentUsers; j++ {
			wg.Add(1)
			go depositWithRetryOnConcurrentModification(&wg)
		}
		wg.Wait()
	}

	snapshot, err := fixture.repo.Query(fixture.aggregateId)
	expectNoError(t, err)

	assertEqual(t, snapshot.balance, int64(operationCount*concurrentUsers))
}
