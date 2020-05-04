package main

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMoveMessageInParallel(t *testing.T) {
	const (
		srcURL = "/srcQ"
		dstURL = "/dstQ"
	)

	sqsMock := &sqsMock{}
	sc := &SQSClient{sqsAPI: sqsMock}

	const TotalMsgs = 105 // choose a number not divisibe by 10 and goroutines
	const Parallel = 3

	sqsMock.On("GetQueueAttributes", mock.Anything).Return(
		&sqs.GetQueueAttributesOutput{Attributes: map[string]*string{"ApproximateNumberOfMessages": aws.String(strconv.Itoa(TotalMsgs))}}, nil)

	msgID := int32(0)
	movedMessageCh := make(chan string, TotalMsgs+20) // make buffer larger than pending messages, so it won't block when code incorrectly send more messages

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

	require.NoError(t, sc.MoveMessages(srcURL, dstURL, 0, Parallel))
	require.EqualValues(t, TotalMsgs, msgID, "all messages are read")
	require.EqualValues(t, TotalMsgs, len(movedMessageCh), "all messages must have moved now")
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
