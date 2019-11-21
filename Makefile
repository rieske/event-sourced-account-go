GOCMD=go
GOBUILD=CGO_ENABLED=0 $(GOCMD) build
GOCLEAN=$(GOCMD) clean -testcache
GOTEST=$(GOCMD) test ./...
GOGET=$(GOCMD) get

BINARY_NAME=bin/account-app
DOCKER_TAG=account:snapshot

all: build test
full: build test integration-test e2e-test
build:
	$(GOBUILD) -o $(BINARY_NAME) -v
test:
	$(GOTEST)
integration-test:
	$(GOTEST) -tags=integration
e2e-test: build
	$(GOTEST) -tags=e2e
docker: build
	docker build -t $(DOCKER_TAG) .
docker-run: docker
	docker run -p 8080:8080 $(DOCKER_TAG)
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
run: build
	./$(BINARY_NAME)

.PHONY: all test clean
