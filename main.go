package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func resolveQueueUrl(queueName string, svc *sqs.SQS) (error, string) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	resp, err := svc.GetQueueUrl(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return err, ""
	}

	return nil, *resp.QueueUrl
}

func main() {

	var (
		sourceQueueName = flag.String("source", "", "Source queue name")
		destQueueName   = flag.String("dest", "", "Destination queue name")
	)

	flag.Parse()

	// Create an EC2 service object in the "us-west-2" region
	// Note that you can also configure your region globally by
	// exporting the AWS_REGION environment variable
	svc := sqs.New(session.New(), aws.NewConfig().WithRegion("us-west-2"))

	err, sourceUrl := resolveQueueUrl(*sourceQueueName, svc)

	if err != nil {
		return
	}

	err, destUrl := resolveQueueUrl(*destQueueName, svc)

	if err != nil {
		return
	}

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(sourceUrl), // Required
		VisibilityTimeout:   aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(1),
		MaxNumberOfMessages: aws.Int64(10),
	}

	for {
		fmt.Println("Starting new batch")

		resp, err := svc.ReceiveMessage(params)

		if len(resp.Messages) == 0 {
			fmt.Println("Batch doesn't have any messages, transfer complete")
			return
		}

		if err != nil {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			return
		}

		fmt.Println("Messages to transfer:")
		fmt.Println(resp.Messages)

		batch := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(destUrl),
			Entries:  convertToEntries(resp.Messages),
		}

		sendResp, err := svc.SendMessageBatch(batch)

		if err != nil {
			fmt.Println("Failed to unqueue messages to the destination queue")
			fmt.Println(err.Error())
			return
		}

		if len(sendResp.Failed) > 0 {
			fmt.Println("Failed to unqueue messages to the destination queue")
			fmt.Println(sendResp.Failed)
			return
		}

		fmt.Println("Unqueued to destination the following: ")
		fmt.Println(sendResp.Successful)

		if len(sendResp.Successful) == len(resp.Messages) {
			deleteMessageBatch := &sqs.DeleteMessageBatchInput{
				Entries:  convertSuccessfulMessageToBatchRequestEntry(resp.Messages),
				QueueUrl: aws.String(sourceUrl),
			}

			deleteResp, err := svc.DeleteMessageBatch(deleteMessageBatch)

			if err != nil {
				fmt.Println("Error deleting messages, exiting...")
				return
			}

			if len(deleteResp.Failed) > 0 {
				fmt.Println("Error deleting messages, the following were not deleted")
				fmt.Println(deleteResp.Failed)
				return
			}

			fmt.Printf("Deleted: %d messages \n", len(deleteResp.Successful))
			fmt.Println("========================")
		}
	}

}

func convertToEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	result := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		result[i] = &sqs.SendMessageBatchRequestEntry{
			MessageBody: message.Body,
			Id:          message.MessageId,
		}
	}

	return result
}

func convertSuccessfulMessageToBatchRequestEntry(messages []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	result := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		result[i] = &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
	}

	return result
}
