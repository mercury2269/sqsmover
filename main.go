package main

import (
	"fmt"
	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fatih/color"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	sourceQueue      = kingpin.Flag("source", "Source queue to move messages from").Short('s').Required().String()
	destinationQueue = kingpin.Flag("destination", "Destination queue to move messages to").Short('d').Required().String()
	region           = kingpin.Flag("region", "AWS Region for source and destination queues").Short('r').Default("us-west-2").String()
)

func resolveQueueUrl(svc *sqs.SQS, queueName string) (error, string) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	resp, err := svc.GetQueueUrl(params)

	if err != nil {
		return err, ""
	}

	return nil, *resp.QueueUrl
}

func main() {
	log.SetHandler(cli.Default)

	fmt.Println()
	defer fmt.Println()

	kingpin.UsageTemplate(kingpin.CompactUsageTemplate)
	kingpin.Parse()

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})

	if err != nil {
		log.Error(color.New(color.FgRed).Sprintf("Unable to create AWS session for region: ", *region))
		return
	}

	svc := sqs.New(sess)

	err, sourceQueueUrl := resolveQueueUrl(svc, *sourceQueue)

	if err != nil {
		log.Error(color.New(color.FgRed).Sprintf("Unable to locate the source queue with name: %s, check region and name", *sourceQueue))
		return
	}

	log.Info(color.New(color.FgCyan).Sprintf("Source queue url: %s", sourceQueueUrl))

	err, destinationQueueUrl := resolveQueueUrl(svc, *destinationQueue)

	if err != nil {
		log.Error(color.New(color.FgRed).Sprintf("Unable to locate the destination queue with name: %s, check region and name", *sourceQueue))
		return
	}

	log.Info(color.New(color.FgCyan).Sprintf("Destination queue url: %s", destinationQueueUrl))

	queueAttributes, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(sourceQueueUrl),
		AttributeNames: []*string{aws.String("All")},
	})

	log.Info(color.New(color.FgCyan).Sprintf("Approximate number of messages in the source queue: %s",
		*queueAttributes.Attributes["ApproximateNumberOfMessages"]))

	moveMessages(sourceQueueUrl, destinationQueueUrl, svc)

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

func moveMessages(sourceQueueUrl string, destinationQueueUrl string, svc *sqs.SQS) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(sourceQueueUrl),
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
			QueueUrl: aws.String(destinationQueueUrl),
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
				QueueUrl: aws.String(sourceQueueUrl),
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
