package eventsourcing

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type consistencyTestFixture struct {
	accountService  accountService
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

func newConsistencyTestFixture(t *testing.T, snapshottingFrequency int) *consistencyTestFixture {
	accountService := accountService{*NewAccountRepository(newInMemoryStore(), snapshottingFrequency)}

	return &consistencyTestFixture{
		accountService:  accountService,
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
			t.Errorf("Expecting only concurrent modification errors, got %v", err.Error())
			break
		}
	}
	wg.Done()
}

func testConcurrentDeposits(t *testing.T, snapshottingFrequency int) {
	fixture := newConsistencyTestFixture(t, snapshottingFrequency)

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := fixture.accountService.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	fixture.doConcurrently(t, func(s accountService) error {
		return s.Deposit(id, uuid.New(), 1)
	})

	snapshot, err := fixture.accountService.QueryAccount(id)
	assert.NoError(t, err)
	assert.Equal(t, int64(fixture.operationCount*fixture.concurrentUsers), snapshot.Balance)
}

func testConcurrentTransfers(t *testing.T, snapshottingFrequency int) {
	// given
	fixture := newConsistencyTestFixture(t, snapshottingFrequency)

	sourceAccountId, sourceOwnerId := account.NewAccountId(), account.NewOwnerId()
	err := fixture.accountService.OpenAccount(sourceAccountId, sourceOwnerId)
	assert.NoError(t, err)
	err = fixture.accountService.Deposit(sourceAccountId, uuid.New(), int64(fixture.operationCount*fixture.concurrentUsers))
	assert.NoError(t, err)

	targetAccountId, targetOwnerId := account.NewAccountId(), account.NewOwnerId()
	err = fixture.accountService.OpenAccount(targetAccountId, targetOwnerId)
	assert.NoError(t, err)
	err = fixture.accountService.Deposit(targetAccountId, uuid.New(), int64(fixture.operationCount))
	assert.NoError(t, err)

	// when
	fixture.doConcurrently(t, func(s accountService) error {
		return s.Transfer(sourceAccountId, targetAccountId, uuid.New(), 1)
	})

	// then
	sourceSnapshot, err := fixture.accountService.QueryAccount(sourceAccountId)
	assert.NoError(t, err)
	assert.Zero(t, sourceSnapshot.Balance)

	targetSnapshot, err := fixture.accountService.QueryAccount(targetAccountId)
	assert.NoError(t, err)
	assert.Equal(t, int64(fixture.operationCount*fixture.concurrentUsers+fixture.operationCount), targetSnapshot.Balance)
}

func TestConcurrentDeposits(t *testing.T) {
	testConcurrentDeposits(t, 0)
}

func TestConcurrentDepositsWithSnapshotting(t *testing.T) {
	testConcurrentDeposits(t, 5)
}

func TestConcurrentTransfers(t *testing.T) {
	testConcurrentTransfers(t, 0)
}

func TestConcurrentTransfersWithSnapshotting(t *testing.T) {
	testConcurrentTransfers(t, 5)
}
