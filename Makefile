ifndef VERSION
	VERSION := $(shell git describe --tags --always --dirty="-dev")
endif

PKG := ./cmd/topicctl
BIN := build/topicctl

LDFLAGS := -ldflags='-X "main.version=$(VERSION)"'

.PHONY: topicctl
topicctl:
	go build -o $(BIN) $(LDFLAGS) $(PKG)

.PHONY: install
install:
	go install $(LDFLAGS) $(PKG)

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: vet
	go test -count 1 -p 1 ./...

.PHONY: test-v2
test-v2: vet
	KAFKA_TOPICS_TEST_BROKER_ADMIN=1 go test -count 1 -p 1 ./...

.PHONY: clean
clean:
	rm -Rf build
