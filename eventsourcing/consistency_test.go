package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/test"
	"sync"
	"testing"
)

type consistencyTestFixture struct {
	store       eventStore
	repo        Repository
	aggregateId account.AggregateId
}

func openAccount(t *testing.T, snapshottingFrequency int) *consistencyTestFixture {
	store := newInMemoryStore()
	repo := NewAccountRepository(store, snapshottingFrequency)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := repo.Open(id, ownerId)
	test.ExpectNoError(t, err)

	return &consistencyTestFixture{store, *repo, id}
}

func withRetryOnConcurrentModification(t *testing.T, wg *sync.WaitGroup, operation func() error) {
	for {
		err := operation()
		if err == nil {
			break
		}
		if err.Error() != "concurrent modification error" {
			t.Error("Expecting only concurrent modification errors")
		}
	}
	wg.Done()
}

func TestConcurrentDeposits(t *testing.T) {
	fixture := openAccount(t, 0)

	operationCount := 50
	concurrentUsers := 8

	for i := 0; i < operationCount; i++ {
		wg := sync.WaitGroup{}
		for j := 0; j < concurrentUsers; j++ {
			wg.Add(1)
			go withRetryOnConcurrentModification(t, &wg, func() error {
				return fixture.repo.Transact(fixture.aggregateId, func(a *account.Account) error {
					return a.Deposit(1)
				})
			})
		}
		wg.Wait()
	}

	snapshot, err := fixture.repo.Query(fixture.aggregateId)
	test.ExpectNoError(t, err)

	assertEqual(t, snapshot.Balance, int64(operationCount*concurrentUsers))
}

func TestConcurrentDepositsWithSnapshotting(t *testing.T) {
	fixture := openAccount(t, 5)

	operationCount := 50
	concurrentUsers := 8

	for i := 0; i < operationCount; i++ {
		wg := sync.WaitGroup{}
		for j := 0; j < concurrentUsers; j++ {
			wg.Add(1)
			go withRetryOnConcurrentModification(t, &wg, func() error {
				return fixture.repo.Transact(fixture.aggregateId, func(a *account.Account) error {
					return a.Deposit(1)
				})
			})
		}
		wg.Wait()
	}

	snapshot, err := fixture.repo.Query(fixture.aggregateId)
	test.ExpectNoError(t, err)

	assertEqual(t, snapshot.Balance, int64(operationCount*concurrentUsers))
}
