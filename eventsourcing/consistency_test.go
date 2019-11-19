package eventsourcing_test

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
)

type ConsistencyTestSuite struct {
	suite.Suite
	accountService  *eventsourcing.AccountService
	operationCount  int
	concurrentUsers int
}

func TestConsistencyInMemory(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := ConsistencyTestSuite{
		Suite:           suite.Suite{},
		accountService:  eventsourcing.NewAccountService(store, 0),
		operationCount:  100,
		concurrentUsers: 8,
	}

	suite.Run(t, &testSuite)
}

func TestConsistencyInMemoryWithSnapshotting(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := ConsistencyTestSuite{
		Suite:           suite.Suite{},
		accountService:  eventsourcing.NewAccountService(store, 5),
		operationCount:  100,
		concurrentUsers: 8,
	}

	suite.Run(t, &testSuite)
}

/*func TestConsistencyInMemoryDb(t *testing.T) {
	testSuite := ConsistencyTestSuite{
		Suite:           suite.Suite{},
		service:         eventsourcing.NewAccountService(store, 0),
		operationCount:  100,
		concurrentUsers: 8,
	}

	suite.Run(t, &testSuite)
}*/

func (suite *ConsistencyTestSuite) doConcurrently(action func(s *eventsourcing.AccountService) error) {
	for i := 0; i < suite.operationCount; i++ {
		wg := sync.WaitGroup{}
		wg.Add(suite.concurrentUsers)
		for j := 0; j < suite.concurrentUsers; j++ {
			go suite.withRetryOnConcurrentModification(&wg, i, j, func() error {
				return action(suite.accountService)
			})
		}
		wg.Wait()
	}
}

func (suite *ConsistencyTestSuite) doConcurrentTransactions(action func(s *eventsourcing.AccountService, txId uuid.UUID) error) {
	for i := 0; i < suite.operationCount; i++ {
		var txId = uuid.New()
		wg := sync.WaitGroup{}
		wg.Add(suite.concurrentUsers)
		for j := 0; j < suite.concurrentUsers; j++ {
			go suite.withRetryOnConcurrentModification(&wg, i, j, func() error {
				return action(suite.accountService, txId)
			})
		}
		wg.Wait()
	}
}

func (suite *ConsistencyTestSuite) withRetryOnConcurrentModification(wg *sync.WaitGroup, iteration, threadNo int, operation func() error) {
	//fmt.Printf("thread %v\n", threadNo)
	for {
		err := operation()
		if err == nil {
			break
		}
		//fmt.Printf("thread %v retrying...\n", threadNo)
		if err.Error() != "concurrent modification error" {
			suite.T().Errorf(
				"Expecting only concurrent modification errors, got %v, threadNo %v, iteration %v",
				err,
				threadNo,
				iteration,
			)
			break
		}
	}
	wg.Done()
}

func (suite *ConsistencyTestSuite) TestConcurrentDeposits() {
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(id, ownerID)
	suite.NoError(err)

	suite.doConcurrently(func(s *eventsourcing.AccountService) error {
		return s.Deposit(id, uuid.New(), 1)
	})

	snapshot, err := suite.accountService.QueryAccount(id)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*suite.concurrentUsers), snapshot.Balance)
}

func (suite *ConsistencyTestSuite) TestConcurrentTransfers() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(sourceAccountId, sourceownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(sourceAccountId, uuid.New(), int64(suite.operationCount*suite.concurrentUsers))
	suite.NoError(err)

	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err = suite.accountService.OpenAccount(targetAccountId, targetownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(targetAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	// when
	suite.doConcurrently(func(s *eventsourcing.AccountService) error {
		return s.Transfer(sourceAccountId, targetAccountId, uuid.New(), 1)
	})

	// then
	sourceSnapshot, err := suite.accountService.QueryAccount(sourceAccountId)
	suite.NoError(err)
	suite.Zero(sourceSnapshot.Balance)

	targetSnapshot, err := suite.accountService.QueryAccount(targetAccountId)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*suite.concurrentUsers+suite.operationCount), targetSnapshot.Balance)
}

func (suite *ConsistencyTestSuite) TestConcurrentIdempotentTransfers() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(sourceAccountId, sourceownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(sourceAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err = suite.accountService.OpenAccount(targetAccountId, targetownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(targetAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	// when
	suite.doConcurrentTransactions(func(s *eventsourcing.AccountService, txId uuid.UUID) error {
		return s.Transfer(sourceAccountId, targetAccountId, txId, 1)
	})

	// then
	sourceSnapshot, err := suite.accountService.QueryAccount(sourceAccountId)
	suite.NoError(err)
	suite.Zero(sourceSnapshot.Balance)

	targetSnapshot, err := suite.accountService.QueryAccount(targetAccountId)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*2), targetSnapshot.Balance)
}
