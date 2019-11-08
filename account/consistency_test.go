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

	id := NewAccountId()
	ownerId := NewOwnerId()
	err := repo.Open(id, ownerId)
	expectNoError(t, err)

	return &consistencyTestFixture{&store, *repo, id}
}

func withRetryOnConcurrentModification(t *testing.T, wg *sync.WaitGroup, operation func() error) {
	for {
		err := operation()
		if err == nil {
			break
		}
		if err.Error() != "Concurrent modification error" {
			t.Error("Expecting only concurrent modification errors")
		}
	}
	wg.Done()
}

func TestConcurrentDeposits(t *testing.T) {
	fixture := openAccount(t)

	operationCount := 50
	concurrentUsers := 8

	for i := 0; i < operationCount; i++ {
		wg := sync.WaitGroup{}
		for j := 0; j < concurrentUsers; j++ {
			wg.Add(1)
			go withRetryOnConcurrentModification(t, &wg, func() error {
				return fixture.repo.Transact(fixture.aggregateId, func(a *account) error {
					return a.Deposit(1)
				})
			})
		}
		wg.Wait()
	}

	snapshot, err := fixture.repo.Query(fixture.aggregateId)
	expectNoError(t, err)

	assertEqual(t, snapshot.balance, int64(operationCount*concurrentUsers))
}
