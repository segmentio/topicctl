package main

import (
	_ "github.com/segmentio/kafka-go/sasl/aws_msk_iam"
	"github.com/segmentio/topicctl/cmd/topicctl/subcmd"
)

var (
	// Version is the version of this binary. Overridden as part of the build process.
	Version = "dev"
)

func main() {
	subcmd.Execute(Version)
}
