package main

import (
	"fmt"
	"strconv"
  "encoding/json"
  "crypto/rand"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fatih/color"
	"github.com/tj/go-progress"
	"github.com/tj/go/term"
	"gopkg.in/alecthomas/kingpin.v2"
)

// nolint: gochecknoglobals
var (
	version = "dev"
	commit  = ""
	date    = ""
	builtBy = ""
)

var (
	sourceQueue      = kingpin.Flag("source", "Source queue name to move messages from.").Short('s').Required().String()
	destinationQueue = kingpin.Flag("destination", "Destination queue name to move messages to.").Short('d').Required().String()
	region           = kingpin.Flag("region", "AWS region for source and destination queues.").Short('r').Default("us-west-2").String()
	profile          = kingpin.Flag("profile", "Use a specific profile from AWS credentials file.").Short('p').Default("").String()
	limit            = kingpin.Flag("limit", "Limits number of messages moved. No limit is set by default.").Short('l').Default("0").Int()
)

func main() {
	log.SetHandler(cli.Default)

	fmt.Println()
	defer fmt.Println()

	kingpin.Version(buildVersion(version, commit, date, builtBy))
	kingpin.UsageTemplate(kingpin.CompactUsageTemplate)
	kingpin.CommandLine.VersionFlag.Short('v')
	kingpin.CommandLine.HelpFlag.Short('h')

	kingpin.Parse()

	sess, err := session.NewSessionWithOptions(
		session.Options{
			Config:            aws.Config{Region: aws.String(*region)},
			Profile:           *profile,
			SharedConfigState: session.SharedConfigEnable,
		},
	)

	if err != nil {
		log.Error(color.New(color.FgRed).Sprintf("Unable to create AWS session for region \r\n", *region))
		return
	}

	svc := sqs.New(sess)

	sourceQueueUrl, err := resolveQueueUrl(svc, *sourceQueue)

	if err != nil {
		logAwsError("Failed to resolve source queue", err)
		return
	}

	log.Info(color.New(color.FgCyan).Sprintf("Source queue URL: %s", sourceQueueUrl))

	destinationQueueUrl, err := resolveQueueUrl(svc, *destinationQueue)

	if err != nil {
		logAwsError("Failed to resolve destination queue", err)
		return
	}

	log.Info(color.New(color.FgCyan).Sprintf("Destination queue URL: %s", destinationQueueUrl))

	queueAttributes, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(sourceQueueUrl),
		AttributeNames: []*string{aws.String("All")},
	})

	if err != nil {
		logAwsError("Failed to resolve queue attributes", err)
		return
	}

	numberOfMessages, _ := strconv.Atoi(*queueAttributes.Attributes["ApproximateNumberOfMessages"])

	log.Info(color.New(color.FgCyan).Sprintf("Approximate number of messages in the source queue: %d", numberOfMessages))

	if numberOfMessages == 0 {
		log.Info("Looks like nothing to move. Done.")
		return
	}

	if *limit > 0 && numberOfMessages > *limit {
		numberOfMessages = *limit
		log.Info(color.New(color.FgCyan).Sprintf("Limit is set, will only move %d messages", numberOfMessages))
	}

	moveMessages(sourceQueueUrl, destinationQueueUrl, svc, numberOfMessages)

}

func resolveQueueUrl(svc *sqs.SQS, queueName string) (string, error) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	resp, err := svc.GetQueueUrl(params)

	if err != nil {
		return "", err
	}

	return *resp.QueueUrl, nil
}

func logAwsError(message string, err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		log.Error(color.New(color.FgRed).Sprintf("%s. Error: %s", message, awsErr.Message()))
	} else {
		log.Error(color.New(color.FgRed).Sprintf("%s. Error: %s", message, err.Error()))
	}
}

func randToken(num int) string {
    b := make([]byte, num)
    rand.Read(b)
    return fmt.Sprintf("%x", b)
}

func convertToEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	result := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	for i, message := range messages {

    var msgData map[string]interface{}
    json.Unmarshal([]byte(*message.Body), &msgData)

    gid := "customer_" + fmt.Sprint(msgData["message_group_id"]) + randToken(8)
    ddupId := randToken(32)

    requestEntry := &sqs.SendMessageBatchRequestEntry{
			MessageBody:       message.Body,
			Id:                message.MessageId,
			MessageAttributes: message.MessageAttributes,
      MessageGroupId:    &gid,
      MessageDeduplicationId: &ddupId,
		}

		if messageGroupId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]; ok {
			requestEntry.MessageGroupId = messageGroupId
		}

		if messageDeduplicationId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]; ok {
			requestEntry.MessageDeduplicationId = messageDeduplicationId
		}

		result[i] = requestEntry
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

func moveMessages(sourceQueueUrl string, destinationQueueUrl string, svc *sqs.SQS, totalMessages int) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(sourceQueueUrl),
		VisibilityTimeout:     aws.Int64(2),
		WaitTimeSeconds:       aws.Int64(0),
		MaxNumberOfMessages:   aws.Int64(10),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameMessageGroupId),
			aws.String(sqs.MessageSystemAttributeNameMessageDeduplicationId)},
	}

	log.Info(color.New(color.FgCyan).Sprintf("Starting to move messages..."))
	fmt.Println()

	term.HideCursor()
	defer term.ShowCursor()

	b := progress.NewInt(totalMessages)
	b.Width = 40
	b.StartDelimiter = color.New(color.FgCyan).Sprint("|")
	b.EndDelimiter = color.New(color.FgCyan).Sprint("|")
	b.Filled = color.New(color.FgCyan).Sprint("█")
	b.Empty = color.New(color.FgCyan).Sprint("░")
	b.Template(`		{{.Bar}} {{.Text}}{{.Percent | printf "%3.0f"}}%`)

	render := term.Renderer()

	messagesProcessed := 0

	for {
		resp, err := svc.ReceiveMessage(params)

		if len(resp.Messages) == 0 || messagesProcessed == totalMessages {
			fmt.Println()
			log.Info(color.New(color.FgCyan).Sprintf("Done. Moved %s messages", strconv.Itoa(totalMessages)))
			return
		}

		if err != nil {
			logAwsError("Failed to receive messages", err)
			return
		}

		messagesToCopy := resp.Messages

		if len(resp.Messages)+messagesProcessed > totalMessages {
			messagesToCopy = resp.Messages[0 : totalMessages-messagesProcessed]
		}

		batch := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(destinationQueueUrl),
			Entries:  convertToEntries(messagesToCopy),
		}

		sendResp, err := svc.SendMessageBatch(batch)

		if err != nil {
			logAwsError("Failed to un-queue messages to the destination", err)
			return
		}

		if len(sendResp.Failed) > 0 {
			log.Error(color.New(color.FgRed).Sprintf("%s messages failed to enqueue, exiting", len(sendResp.Failed)))
			return
		}

		if len(sendResp.Successful) == len(messagesToCopy) {
			deleteMessageBatch := &sqs.DeleteMessageBatchInput{
				Entries:  convertSuccessfulMessageToBatchRequestEntry(messagesToCopy),
				QueueUrl: aws.String(sourceQueueUrl),
			}

			deleteResp, err := svc.DeleteMessageBatch(deleteMessageBatch)

			if err != nil {
				logAwsError("Failed to delete messages from source queue", err)
				return
			}

			if len(deleteResp.Failed) > 0 {
				log.Error(color.New(color.FgRed).Sprintf("Error deleting messages, the following were not deleted\n %s", deleteResp.Failed))
				return
			}

			messagesProcessed += len(messagesToCopy)
		}

		// Increase the total if the approximation was under - avoids exception
		if messagesProcessed > totalMessages {
			b.Total = float64(messagesProcessed)
		}

		b.ValueInt(messagesProcessed)
		render(b.String())
	}
}

func buildVersion(version, commit, date, builtBy string) string {
	var result = fmt.Sprintf("version: %s", version)
	if commit != "" {
		result = fmt.Sprintf("%s\ncommit: %s", result, commit)
	}
	if date != "" {
		result = fmt.Sprintf("%s\nbuilt at: %s", result, date)
	}
	if builtBy != "" {
		result = fmt.Sprintf("%s\nbuilt by: %s", result, builtBy)
	}
	return result
}
