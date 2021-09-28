package main

import (
	"flag"

	"github.com/sirupsen/logrus"
)

func main() {
	srcQueue := flag.String("source", "", "The source queue name to move messages from")
	dstQueue := flag.String("destination", "", "The destination queue name to move messages to")
	region := flag.String("region", "us-west-2", "The AWS region for source and destination queues")
	profile := flag.String("profile", "default", "Use a specific profile from AWS credentials file")
	limit := flag.Int("limit", 0, "Limits total number of messages moved. No limit is set by default")
	parallel := flag.Int("parallel", 10, "Max numers of messages to be moved in parallel")

	flag.Parse()

	if *srcQueue == "" || *dstQueue == "" {
		logrus.Fatalf("both -from and -to must be provided")
	}

	sc, err := NewSQSClient(*region, *profile)
	if err != nil {
		logrus.WithError(err).Fatal("failed to create sqs client")
	}

	srcQueURL, err := sc.ResolveQueueURL(*srcQueue)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to resolve URL for queue %s", *srcQueue)
	}

	dstQueURL, err := sc.ResolveQueueURL(*dstQueue)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to resolve URL for queue %s", *dstQueue)
	}

	logrus.Infof("moving messages from %s to %s", srcQueURL, dstQueURL)
	if err := sc.MoveMessages(srcQueURL, dstQueURL, *limit, *parallel); err != nil {
		logrus.WithError(err).Fatal("error moving all messages")
	}
	logrus.Info("completed!")
}
