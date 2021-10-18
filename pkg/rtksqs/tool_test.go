package rtksqs

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewSQSClient(t *testing.T) {
	sc, err := NewSQSClient("test")

	require.NoError(t, err)
	require.NotNil(t, sc)
}

func TestSQSClient_ResolveQueueURL(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	// No Error
	urlToReturn := "https://sqs.queue.url"
	sqsMock.On("GetQueueUrl", mock.AnythingOfType("*sqs.GetQueueUrlInput")).
		Return(&sqs.GetQueueUrlOutput{QueueUrl: &urlToReturn}, nil).Once()

	url, err := sc.ResolveQueueURL("queue-name")

	require.NoError(t, err)
	require.NotEmpty(t, url)

	// Error
	errStr := "sqs error"
	sqsMock.On("GetQueueUrl", mock.AnythingOfType("*sqs.GetQueueUrlInput")).
		Return(&sqs.GetQueueUrlOutput{}, errors.New(errStr)).Once()

	url, err = sc.ResolveQueueURL("queue-name")

	require.Empty(t, url)
	require.Error(t, err)
	require.Contains(t, err.Error(), errStr)
}

const (
	srcURL  = "/srcQ"
	dstURL  = "/dstQ"
	noLimit = 0
	// number of messages to be moved in parallel
	parallel = 3
)

func TestSQSClient_MoveMessages_Success(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	const totalMsgs = 105 // choose a number not divisible by 10

	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{
			Attributes: map[string]*string{"ApproximateNumberOfMessages": aws.String(strconv.Itoa(totalMsgs))},
		}, nil)

	msgID := int32(0)
	// make buffer larger than pending messages, so it won't block when code incorrectly sends more messages
	movedMessageCh := make(chan string, totalMsgs+20)

	// mock ReceiveMessage to prepare source messages
	recvMsg := sqsMock.On("ReceiveMessage", mock.Anything)
	recvMsg.Run(func(args mock.Arguments) {
		in := args.Get(0).(*sqs.ReceiveMessageInput)
		n := *in.MaxNumberOfMessages
		msgs := make([]*sqs.Message, n)
		for i := range msgs {
			msgs[i] = &sqs.Message{
				MessageId: aws.String(strconv.Itoa(int(msgID))),
				Body:      aws.String("any content"),
			}
			atomic.AddInt32(&msgID, 1)
		}

		out := &sqs.ReceiveMessageOutput{Messages: msgs}
		recvMsg.Return(out, nil)
	})

	// mock SendMessageBatch to return moved message
	sendMsg := sqsMock.On("SendMessageBatch", mock.Anything)
	sendMsg.Run(func(args mock.Arguments) {
		entries := args.Get(0).(*sqs.SendMessageBatchInput).Entries
		result := make([]*sqs.SendMessageBatchResultEntry, len(entries))
		for i, entry := range entries {
			result[i] = &sqs.SendMessageBatchResultEntry{
				Id: entry.Id,
			}
			movedMessageCh <- *entry.Id
		}

		out := &sqs.SendMessageBatchOutput{Successful: result}
		sendMsg.Return(out, nil)
	})

	deletedMessageCh := make(chan string, totalMsgs+20)
	delMsg := sqsMock.On("DeleteMessageBatch", mock.Anything)
	delMsg.Run(func(args mock.Arguments) {
		entries := args.Get(0).(*sqs.DeleteMessageBatchInput).Entries
		for _, entry := range entries {
			deletedMessageCh <- *entry.Id
		}
		delMsg.Return(&sqs.DeleteMessageBatchOutput{}, nil)
	})

	err := sc.MoveMessages(srcURL, dstURL, noLimit, parallel)
	fmt.Println("Messages sent:", len(movedMessageCh))
	fmt.Println("Messages deleted:", len(deletedMessageCh))
	require.NoError(t, err)
	require.EqualValues(t, totalMsgs, msgID, "all messages are read")
	require.EqualValuesf(t, len(movedMessageCh), len(deletedMessageCh),
		"number of moved messages and deleted messages are the same")
	require.EqualValues(t, totalMsgs, len(movedMessageCh), "all messages must have moved now")
	close(movedMessageCh)

	// ensure each message was only sent once, no race between goroutines
	messageIds := map[int]bool{}
	for idStr := range movedMessageCh {
		idNum, err := strconv.Atoi(idStr)
		require.NoError(t, err, "should be number")
		require.False(t, messageIds[idNum], "duplicate found")
		messageIds[idNum] = true
	}

	// ensure all sent messages were deleted
	for idStr := range movedMessageCh {
		exists := existsInChan(idStr, deletedMessageCh)
		require.True(t, exists, "message was deleted")
	}
}

func TestSQSClient_MoveMessages_GetQueueAttributesError(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	errStr := "sqs error"
	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{}, errors.New(errStr)).Once()

	err := sc.MoveMessages(srcURL, dstURL, noLimit, parallel)
	require.Error(t, err)
	require.Contains(t, err.Error(), errStr)
}

func TestSQSClient_MoveMessages_NoMessages(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	const totalMsgs = 0

	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{
			Attributes: map[string]*string{"ApproximateNumberOfMessages": aws.String(strconv.Itoa(totalMsgs))},
		}, nil)

	err := sc.MoveMessages(srcURL, dstURL, noLimit, parallel)
	require.NoError(t, err)
}

func TestSQSClient_MoveMessages_Limited(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	const totalMsgs = 20
	const limit = 11

	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{
			Attributes: map[string]*string{"ApproximateNumberOfMessages": aws.String(strconv.Itoa(totalMsgs))},
		}, nil)

	msgID := int32(0)
	// make buffer larger than pending messages, so it won't block when code incorrectly sends more messages
	movedMessageCh := make(chan string, totalMsgs+20)

	// mock ReceiveMessage to prepare source messages
	recvMsg := sqsMock.On("ReceiveMessage", mock.Anything)
	recvMsg.Run(func(args mock.Arguments) {
		in := args.Get(0).(*sqs.ReceiveMessageInput)
		n := *in.MaxNumberOfMessages
		msgs := make([]*sqs.Message, n)
		for i := range msgs {
			id := int(atomic.AddInt32(&msgID, 1))
			msgs[i] = &sqs.Message{
				MessageId: aws.String(strconv.Itoa(id)),
				Body:      aws.String("any content"),
			}
		}

		out := &sqs.ReceiveMessageOutput{Messages: msgs}
		recvMsg.Return(out, nil)
	})

	// mock SendMessageBatch to return moved message
	sendMsg := sqsMock.On("SendMessageBatch", mock.Anything)
	sendMsg.Run(func(args mock.Arguments) {
		in := args.Get(0).(*sqs.SendMessageBatchInput)
		entries := in.Entries
		result := make([]*sqs.SendMessageBatchResultEntry, len(entries))
		for i, entry := range entries {
			result[i] = &sqs.SendMessageBatchResultEntry{
				Id: entry.Id,
			}
			movedMessageCh <- *entry.Id
		}

		out := &sqs.SendMessageBatchOutput{Successful: result}
		sendMsg.Return(out, nil)
	})

	sqsMock.On("DeleteMessageBatch", mock.Anything).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	err := sc.MoveMessages(srcURL, dstURL, limit, parallel)
	require.NoError(t, err)
	require.EqualValues(t, limit, msgID, "all messages up to limit are read")
	require.EqualValues(t, limit, len(movedMessageCh), "all messages up to limit must have moved now")
	close(movedMessageCh)

	// check to ensure each message should be sent once, no race between goroutines
	messageIds := map[int]bool{}
	for sid := range movedMessageCh {
		msgID, err := strconv.Atoi(sid)
		require.NoError(t, err, "should be number")
		require.False(t, messageIds[msgID], "duplicated")
		messageIds[msgID] = true
	}
}

func TestSQSClient_MoveMessages_ReceiveMessageError(t *testing.T) {
	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	const totalMsgs = 105 // choose a number not divisible by 10

	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{
			Attributes: map[string]*string{"ApproximateNumberOfMessages": aws.String(strconv.Itoa(totalMsgs))},
		}, nil)

	errStr := "sqs error"
	msgID := int32(0)
	// mock ReceiveMessage to prepare source messages
	recvMsg := sqsMock.On("ReceiveMessage", mock.Anything)
	// return an error just once, other calls will work
	timesCalled, errorOnCall := int32(0), int32(6)
	recvMsg.Run(func(args mock.Arguments) {
		// increment times called
		atomic.AddInt32(&timesCalled, 1)

		// if time for error, return error, otherwise proceed normally
		if timesCalled == errorOnCall {
			recvMsg.Return(&sqs.ReceiveMessageOutput{}, errors.New(errStr))
			return
		}

		in := args.Get(0).(*sqs.ReceiveMessageInput)
		n := *in.MaxNumberOfMessages
		msgs := make([]*sqs.Message, n)
		for i := range msgs {
			msgs[i] = &sqs.Message{
				MessageId: aws.String(strconv.Itoa(int(msgID))),
				Body:      aws.String("any content"),
			}
			atomic.AddInt32(&msgID, 1)
		}

		out := &sqs.ReceiveMessageOutput{Messages: msgs}
		recvMsg.Return(out, nil)
	})

	// make buffer larger than pending messages, so it won't block when code incorrectly sends more messages
	movedMessageCh := make(chan string, totalMsgs+20)
	// mock SendMessageBatch to return moved message
	sendMsg := sqsMock.On("SendMessageBatch", mock.Anything)
	sendMsg.Run(func(args mock.Arguments) {
		entries := args.Get(0).(*sqs.SendMessageBatchInput).Entries
		result := make([]*sqs.SendMessageBatchResultEntry, len(entries))
		for i, entry := range entries {
			result[i] = &sqs.SendMessageBatchResultEntry{
				Id: entry.Id,
			}
			movedMessageCh <- *entry.Id
		}

		out := &sqs.SendMessageBatchOutput{Successful: result}
		sendMsg.Return(out, nil)
	})

	deletedMessageCh := make(chan string, totalMsgs+20)
	delMsg := sqsMock.On("DeleteMessageBatch", mock.Anything)
	delMsg.Run(func(args mock.Arguments) {
		entries := args.Get(0).(*sqs.DeleteMessageBatchInput).Entries
		for _, entry := range entries {
			deletedMessageCh <- *entry.Id
		}
		delMsg.Return(&sqs.DeleteMessageBatchOutput{}, nil)
	})

	err := sc.MoveMessages(srcURL, dstURL, noLimit, parallel)
	fmt.Println("Messages sent:", len(movedMessageCh))
	fmt.Println("Messages deleted:", len(deletedMessageCh))
	require.Error(t, err)
	require.Less(t, len(movedMessageCh), totalMsgs, "not all messages are moved")
	require.EqualValuesf(t, len(movedMessageCh), len(deletedMessageCh),
		"number of moved messages and deleted messages are the same")
	close(movedMessageCh)

	// ensure each message was only sent once, no race between goroutines
	messageIds := map[int]bool{}
	for idStr := range movedMessageCh {
		idNum, err := strconv.Atoi(idStr)
		require.NoError(t, err, "should be number")
		require.False(t, messageIds[idNum], "duplicate found")
		messageIds[idNum] = true
	}

	// ensure all sent messages were deleted
	for idStr := range movedMessageCh {
		exists := existsInChan(idStr, deletedMessageCh)
		require.True(t, exists, "message was deleted")
	}
}

func existsInChan(val string, c chan string) bool {
	for s := range c {
		if val == s {
			return true
		}
	}
	return false
}
