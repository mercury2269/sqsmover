package rtksqs

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	defaultVisibilityTimeout = 60
	sqsLongPollTimeout       = 10

	// AWS SQS sets this limit of 10
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
	maxMessagesPerRead = 10
)

// sqsAPI is internal interface that allows sqs to be mocked in unit tests
type sqsAPI interface {
	GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error)
	ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
	DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
}

// SQSClient wraps sqs.SQS and allows sqs to be mocked in unit tests
//goland:noinspection GoUnnecessarilyExportedIdentifiers
type SQSClient struct {
	sqsAPI
}

// NewSQSClient creates a new SQS instance
func NewSQSClient(region string) (*SQSClient, error) {
	sess, err := session.NewSessionWithOptions(
		session.Options{
			Config:            aws.Config{Region: aws.String(region)},
			SharedConfigState: session.SharedConfigEnable,
		},
	)

	if err != nil {
		err = errors.Wrapf(err, "creating AWS session for region %s", region)
		return nil, err
	}

	return &SQSClient{sqsAPI: sqs.New(sess)}, nil
}

// ResolveQueueURL gets the queue URL from a queue name
func (sc *SQSClient) ResolveQueueURL(queueName string) (string, error) {
	resp, err := sc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", errors.Wrapf(err, "resolving the url of queue %s", queueName)
	}

	return *resp.QueueUrl, nil
}

// MoveMessages moves messages from one queue to the other
// If limit is 0, move all messages; otherwise move up to the limit amount
// parallel is number of messages to move in parallel
func (sc *SQSClient) MoveMessages(srcURL, dstURL string, limit, parallel int) error {
	sqAttrs, err := sc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(srcURL),
		AttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		return errors.Wrapf(err, "getting all attributes from queue %s", srcURL)
	}

	pendingMsgs, _ := strconv.Atoi(*sqAttrs.Attributes["ApproximateNumberOfMessages"])
	logrus.Infof("ApproximateNumberOfMessages: %d", pendingMsgs)
	if pendingMsgs == 0 {
		logrus.Info("looks like nothing to move.")
		return nil
	}

	if limit > 0 && limit < pendingMsgs {
		pendingMsgs = limit
	}

	if maxParallel := (pendingMsgs + maxMessagesPerRead - 1) / maxMessagesPerRead; maxParallel < parallel {
		parallel = maxParallel
	}

	if parallel < 1 {
		parallel = 1
	}

	logrus.Infof("will move ~%d messages using %d goroutines", pendingMsgs, parallel)
	// buffered error channel with a capacity of `parallel`,
	// this channel will hold all errors until all goroutines are finished
	errCh := make(chan error, parallel)
	var wg sync.WaitGroup
	messagesToRead := int32(pendingMsgs)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for messagesToRead > 0 && len(errCh) == 0 {
				maxPerRead := maxMessagesPerRead
				if int(messagesToRead) < maxPerRead {
					maxPerRead = int(messagesToRead)
				}
				atomic.AddInt32(&messagesToRead, -int32(maxPerRead))

				moved, err := sc.moveMessageBatch(srcURL, dstURL, maxPerRead)
				atomic.AddInt32(&messagesToRead, int32(maxPerRead-moved)) // add back messages not processed
				if err != nil {
					errCh <- err
					break
				}
				if moved == 0 {
					logrus.Info("no more messages to move in current goroutine")
					break
				}
				logrus.Infof("moved %d messages", pendingMsgs-int(messagesToRead))
			}
		}()
	}
	wg.Wait()

	var movingError error
	select {
	case movingError = <-errCh:
	default:
	}
	return movingError
}

// moveMessageBatch reads up to maxPerRead messages and moves them to the destination
func (sc *SQSClient) moveMessageBatch(srcURL, dstURL string, maxPerRead int) (messagesMoved int, err error) {
	rcvParams := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(srcURL),
		VisibilityTimeout:     aws.Int64(defaultVisibilityTimeout),
		WaitTimeSeconds:       aws.Int64(sqsLongPollTimeout),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		MaxNumberOfMessages:   aws.Int64(int64(maxPerRead)),
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameMessageGroupId),
			aws.String(sqs.MessageSystemAttributeNameMessageDeduplicationId)},
	}
	rcvResp, err := sc.ReceiveMessage(rcvParams)
	if err != nil {
		return 0, errors.Wrap(err, "receiving message")
	}

	logrus.Infof("received %d messages", len(rcvResp.Messages))
	if len(rcvResp.Messages) == 0 {
		return 0, nil
	}

	return sc.sendMessageBatch(srcURL, dstURL, rcvResp.Messages)
}

// sendMessageBatch sends out a batch of messages. Each batch is within aws's size limit
func (sc *SQSClient) sendMessageBatch(srcURL, dstURL string, messages []*sqs.Message) (int, error) {
	messagesProcessed := 0
	for len(messages) > 0 {
		entries := packSendMessageBatchRequestEntries(messages)
		batch := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(dstURL),
			Entries:  entries,
		}

		sendResp, err := sc.SendMessageBatch(batch)
		if err != nil {
			return messagesProcessed, errors.Wrap(err, "sending message batch")
		}

		movedMessages := getSentMessages(messages, sendResp.Successful)
		if len(movedMessages) == 0 {
			break
		}

		if len(sendResp.Failed) > 0 {
			logrus.Warnf("%d/%d messages failed to send", len(sendResp.Failed), len(entries))
		}

		deleteResp, err := sc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
			Entries:  newDeleteMessageBatchRequestEntries(movedMessages),
			QueueUrl: aws.String(srcURL),
		})
		if err != nil {
			return messagesProcessed, errors.Wrap(err, "deleting messages from source queue")
		}

		if len(deleteResp.Failed) > 0 {
			err = errors.New("deleting all moved messages")
			logrus.WithError(err).Errorf("%+v messages not deleted", deleteResp.Failed)
			messagesProcessed += len(deleteResp.Successful)
			return messagesProcessed, err
		}

		messagesProcessed += len(movedMessages)
		messages = messages[len(entries):]
	}

	return messagesProcessed, nil
}

// packSendMessageBatchRequestEntries packs messages into SendMessageBatchRequestEntries
// without exceeding the 256KB aws size limit
func packSendMessageBatchRequestEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	// assume metadata occupies less than 10k
	rCap := (256 - 10) * 1024 // remaining capacity

	result := make([]*sqs.SendMessageBatchRequestEntry, 0)
	for _, message := range messages {
		rCap -= len(*message.Body)

		// stop if adding the next message will exceed size limit
		if rCap < 0 && len(result) > 0 {
			break
		}

		entry := &sqs.SendMessageBatchRequestEntry{
			MessageBody:       message.Body,
			Id:                message.MessageId,
			MessageAttributes: message.MessageAttributes,
		}

		if id, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]; ok {
			entry.MessageGroupId = id
		}

		if id, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]; ok {
			entry.MessageDeduplicationId = id
		}

		result = append(result, entry)
	}

	return result
}

func newDeleteMessageBatchRequestEntries(messages []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	result := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		result[i] = &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
	}

	return result
}

func getSentMessages(allMessages []*sqs.Message, sentMessages []*sqs.SendMessageBatchResultEntry) []*sqs.Message {
	result := make([]*sqs.Message, 0)
	for _, entry := range sentMessages {
		for _, msg := range allMessages {
			if *entry.Id == *msg.MessageId {
				result = append(result, msg)
				break
			}
		}
	}
	return result
}
