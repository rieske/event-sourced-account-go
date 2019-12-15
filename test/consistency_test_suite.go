package test

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/stretchr/testify/suite"
)

type ConsistencyTestSuite struct {
	suite.Suite
	accountService  *eventsourcing.AccountService
	operationCount  int
	concurrentUsers int
}

func NewConsistencyTestSuite(opCount, concurrentUsers, snapshotFrequency int, store eventsourcing.EventStore) *ConsistencyTestSuite {
	return &ConsistencyTestSuite{
		Suite:           suite.Suite{},
		accountService:  eventsourcing.NewAccountService(store, snapshotFrequency),
		operationCount:  opCount,
		concurrentUsers: concurrentUsers,
	}
}

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
		if err != account.ConcurrentModification {
			suite.T().Errorf(
				"Expecting only concurrent modification errors, got %v, threadNo %v, iteration %v",
				err, threadNo, iteration,
			)
			break
		}
	}
	wg.Done()
}

func (suite *ConsistencyTestSuite) TestConcurrentDeposits() {
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(context.Background(), id, ownerID)
	suite.NoError(err)

	suite.doConcurrently(func(s *eventsourcing.AccountService) error {
		return s.Deposit(context.Background(), id, uuid.New(), 1)
	})

	snapshot, err := suite.accountService.QueryAccount(context.Background(), id)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*suite.concurrentUsers), snapshot.Balance)
}

func (suite *ConsistencyTestSuite) TestConcurrentTransfers() {
	// given
	sourceAccountId, sourceOwnerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(context.Background(), sourceAccountId, sourceOwnerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(context.Background(), sourceAccountId, uuid.New(), int64(suite.operationCount*suite.concurrentUsers))
	suite.NoError(err)

	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err = suite.accountService.OpenAccount(context.Background(), targetAccountId, targetownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(context.Background(), targetAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	// when
	suite.doConcurrently(func(s *eventsourcing.AccountService) error {
		return s.Transfer(context.Background(), sourceAccountId, targetAccountId, uuid.New(), 1)
	})

	// then
	sourceSnapshot, err := suite.accountService.QueryAccount(context.Background(), sourceAccountId)
	suite.NoError(err)
	suite.Zero(sourceSnapshot.Balance)

	targetSnapshot, err := suite.accountService.QueryAccount(context.Background(), targetAccountId)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*suite.concurrentUsers+suite.operationCount), targetSnapshot.Balance)
}

func (suite *ConsistencyTestSuite) TestConcurrentIdempotentTransfers() {
	// given
	sourceAccountId, sourceOwnerID := account.NewID(), account.NewOwnerID()
	err := suite.accountService.OpenAccount(context.Background(), sourceAccountId, sourceOwnerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(context.Background(), sourceAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err = suite.accountService.OpenAccount(context.Background(), targetAccountId, targetownerID)
	suite.NoError(err)
	err = suite.accountService.Deposit(context.Background(), targetAccountId, uuid.New(), int64(suite.operationCount))
	suite.NoError(err)

	// when
	suite.doConcurrentTransactions(func(s *eventsourcing.AccountService, txId uuid.UUID) error {
		return s.Transfer(context.Background(), sourceAccountId, targetAccountId, txId, 1)
	})

	// then
	sourceSnapshot, err := suite.accountService.QueryAccount(context.Background(), sourceAccountId)
	suite.NoError(err)
	suite.Zero(sourceSnapshot.Balance)

	targetSnapshot, err := suite.accountService.QueryAccount(context.Background(), targetAccountId)
	suite.NoError(err)
	suite.Equal(int64(suite.operationCount*2), targetSnapshot.Balance)
}
