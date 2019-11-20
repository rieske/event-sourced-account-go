// +build e2e

package test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

const (
	serviceUrl     = "http://localhost:8080"
	composeCommand = "docker-compose"
	composeFile    = "../docker-compose.yml"
)

func waitForPing() {
	for i := 0; i < 30; i++ {
		res, err := http.Get(serviceUrl + "/ping")
		if err == nil && res.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(time.Second * 1)
	}
	log.Println("Service is healthy")
}

func composeUp() {
	cmd := exec.Command(composeCommand, "-f", composeFile, "up", "--build", "-d")
	log.Println(cmd)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func composeDown() {
	cmd := exec.Command(composeCommand, "-f", composeFile, "down", "--volumes")
	log.Println(cmd)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	composeUp()
	waitForPing()

	code := m.Run()
	composeDown()

	os.Exit(code)
}

func TestPing(t *testing.T) {
	res, err := http.Get(serviceUrl + "/ping")

	assert.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, "pong", string(body))
}

func TestCreateAndQueryAccount(t *testing.T) {
	accountID, ownerID := uuid.New().String(), uuid.New().String()

	res, err := http.Post(serviceUrl+"/account/"+accountID+"?owner="+ownerID, "", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, res.StatusCode)

	accountLocation, err := res.Location()
	require.NoError(t, err)

	res, err = http.Get(accountLocation.String())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(
		t,
		fmt.Sprintf(`{"ID":"%s","OwnerID":"%s","Balance":0,"Open":true}`, accountID, ownerID),
		string(body),
	)
}

func TestConsistencyInDistributedEnvironmentUnderLoad(t *testing.T) {
	accountID, ownerID := uuid.New().String(), uuid.New().String()
	res, err := http.Post(serviceUrl+"/account/"+accountID+"?owner="+ownerID, "", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, res.StatusCode)
	accountLocation, err := res.Location()
	require.NoError(t, err)

	depositCount := 500
	depositConcurrently(t, depositCount, 8, accountID)

	res, err = http.Get(accountLocation.String())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(
		t,
		fmt.Sprintf(`{"ID":"%s","OwnerID":"%s","Balance":%d,"Open":true}`, accountID, ownerID, depositCount),
		string(body),
	)
}

func depositConcurrently(t *testing.T, operationCount, concurrentUsers int, accountID string) {
	for i := 0; i < operationCount; i++ {
		txId := uuid.New().String()
		wg := sync.WaitGroup{}
		wg.Add(concurrentUsers)
		for j := 0; j < concurrentUsers; j++ {
			go withRetryOnConcurrentModification(t, &wg, i, j, func() int {
				req, err := http.NewRequest(http.MethodPut, serviceUrl+"/account/"+accountID+"/deposit?amount=1&transactionId="+txId, nil)
				require.NoError(t, err)
				client := &http.Client{}
				res, err := client.Do(req)
				require.NoError(t, err)
				return res.StatusCode
			})
		}
		wg.Wait()
	}
}

func withRetryOnConcurrentModification(t *testing.T, wg *sync.WaitGroup, iteration, threadNo int, operation func() int) {
	//fmt.Printf("thread %v\n", threadNo)
	for {
		status := operation()
		if status == http.StatusNoContent {
			break
		}
		//fmt.Printf("thread %v retrying...\n", threadNo)
		if status != http.StatusConflict {
			t.Errorf(
				"Expecting only conflicts, got %v, threadNo %v, iteration %v",
				status, threadNo, iteration,
			)
			break
		}
	}
	wg.Done()
}
