package main

import (
	"github.com/segmentio/topicctl/cmd/topicctl/subcmd"
)

var (
	Version = "dev"
)

func main() {
	subcmd.Execute(Version)
}
