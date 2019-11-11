package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type consistencyTestFixture struct {
	accountService  accountService
	aggregateId     account.Id
	operationCount  int
	concurrentUsers int
}

func (f consistencyTestFixture) doConcurrently(t *testing.T, action func(s accountService) error) {
	for i := 0; i < f.operationCount; i++ {
		wg := sync.WaitGroup{}
		for j := 0; j < f.concurrentUsers; j++ {
			wg.Add(1)
			go withRetryOnConcurrentModification(t, &wg, j, func() error {
				return action(f.accountService)
			})
		}
		wg.Wait()
	}
}

func openAccount(t *testing.T, snapshottingFrequency int) *consistencyTestFixture {
	accountService := accountService{*NewAccountRepository(newInMemoryStore(), snapshottingFrequency)}

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := accountService.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	return &consistencyTestFixture{
		accountService:  accountService,
		aggregateId:     id,
		operationCount:  100,
		concurrentUsers: 8,
	}
}

func withRetryOnConcurrentModification(t *testing.T, wg *sync.WaitGroup, threadNo int, operation func() error) {
	//fmt.Printf("thread %v\n", threadNo)
	for {
		err := operation()
		if err == nil {
			break
		}
		//fmt.Printf("thread %v retrying...\n", threadNo)
		if err.Error() != "concurrent modification error" {
			t.Error("Expecting only concurrent modification errors")
		}
	}
	wg.Done()
}

func testConcurrentDeposits(t *testing.T, snapshottingFrequency int) {
	fixture := openAccount(t, snapshottingFrequency)

	fixture.doConcurrently(t, func(s accountService) error {
		return s.Deposit(fixture.aggregateId, 1)
	})

	snapshot, err := fixture.accountService.QueryAccount(fixture.aggregateId)
	assert.NoError(t, err)
	assert.Equal(t, int64(fixture.operationCount*fixture.concurrentUsers), snapshot.Balance)
}

func TestConcurrentDeposits(t *testing.T) {
	testConcurrentDeposits(t, 0)
}

func TestConcurrentDepositsWithSnapshotting(t *testing.T) {
	testConcurrentDeposits(t, 5)
}
