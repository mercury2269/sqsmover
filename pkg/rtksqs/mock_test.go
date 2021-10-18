package rtksqs

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/mock"
)

type sqsMock struct {
	mock.Mock
}

func (m *sqsMock) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

func (m *sqsMock) GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.GetQueueAttributesOutput), args.Error(1)
}

func (m *sqsMock) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (m *sqsMock) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.SendMessageBatchOutput), args.Error(1)
}

func (m *sqsMock) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sqs.DeleteMessageBatchOutput), args.Error(1)
}
