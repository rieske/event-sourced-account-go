GOCMD=go
GOBUILD=CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOCMD) build
GOCLEAN=$(GOCMD) clean -testcache
GOTEST=$(GOCMD) test ./...
GOGET=$(GOCMD) get

BINARY_NAME=bin/account-app
DOCKER_TAG=account-go:snapshot

all: build test
full: build test integration-test e2e-test
build:
	$(GOBUILD) -o $(BINARY_NAME) -v
test:
	$(GOTEST) -coverprofile=coverage.out
integration-test:
	$(GOTEST) -tags=integration
e2e-test: build
	$(GOTEST) -tags=e2e
docker: build
	docker build -t $(DOCKER_TAG) .
docker-run: docker
	docker run -p 8080:8080 $(DOCKER_TAG)
compose-run: build
	docker-compose up --build
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME) coverage.out coverage.json
run: build
	./$(BINARY_NAME)
coverage-report: test
	sed -i 's/^github.com\/rieske\/event-sourced-account-go\///g' coverage.out

.PHONY: all test clean
