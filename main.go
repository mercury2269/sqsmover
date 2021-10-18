package main

import (
	"errors"
	"flag"

	"github.com/sirupsen/logrus"

	"github.com/rtkwlf/sqsmover/pkg/rtksqs"
)

type Config struct {
	SrcQueue  string
	DestQueue string
	Region    string
	Limit     int
	Parallel  int
}

func main() {
	cfg, err := getConfig()
	if err != nil {
		logrus.WithError(err).Fatalf("bad configuration")
	}

	sc, err := rtksqs.NewSQSClient(cfg.Region)
	if err != nil {
		logrus.WithError(err).Fatal("failed to create sqs client")
	}

	srcQueURL, err := sc.ResolveQueueURL(cfg.SrcQueue)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to resolve URL for queue %s", cfg.SrcQueue)
	}

	dstQueURL, err := sc.ResolveQueueURL(cfg.DestQueue)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to resolve URL for queue %s", cfg.DestQueue)
	}

	logrus.Infof("moving messages from %s to %s", srcQueURL, dstQueURL)
	if err := sc.MoveMessages(srcQueURL, dstQueURL, cfg.Limit, cfg.Parallel); err != nil {
		logrus.WithError(err).Fatal("error moving all messages")
	}
	logrus.Info("completed!")
}

func getConfig() (*Config, error) {
	srcQueue := flag.String("source", "", "The source queue name to move messages from")
	destQueue := flag.String("destination", "", "The destination queue name to move messages to")

	// Optional
	region := flag.String("region", "us-west-2",
		"[Optional] The AWS region for source and destination queues. \"us-west-2\" by default")
	limit := flag.Int("limit", 0,
		"[Optional] Limits total number of messages moved. No limit is set by default")
	parallel := flag.Int("parallel", 10,
		"[Optional] Maximum number of messages to be moved in parallel. Default of 10")

	flag.Parse()

	if *srcQueue == "" {
		return nil, errors.New("source queue name missing")
	}

	if *destQueue == "" {
		return nil, errors.New("destination queue name missing")
	}

	return &Config{
		SrcQueue:  *srcQueue,
		DestQueue: *destQueue,
		Region:    *region,
		Limit:     *limit,
		Parallel:  *parallel,
	}, nil
}
