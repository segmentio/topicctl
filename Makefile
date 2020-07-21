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
	$Qgo vet ./...

.PHONY: test
test: vet
	$Qgo test -count 1 -p 1 ./...

.PHONY: clean
clean:
	rm -Rf build
