package main

import (
	"github.com/segmentio/topicctl/cmd/topicctl/subcmd"
)

var (
	// Version is the version of this binary. Overridden as part of the build process.
	Version = "dev"
)

func main() {
	subcmd.Execute(Version)
}
