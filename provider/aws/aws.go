package aws

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type AWSPubSubAdapter struct {
	session *session.Session
	snsSvc  *sns.SNS
	sqsSvc  sqsiface.SQSAPI
}

func NewAWSPubSubAdapter(region string, accessKeyId string, secretAccessKey string) (*AWSPubSubAdapter, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewStaticCredentials(
			accessKeyId,
			secretAccessKey,
			"", // a token will be created when the session it's used.
		),
	})
	if err != nil {
		return nil, err
	}

	snsSvc := sns.New(sess)
	sqsSvc := sqs.New(sess)

	return &AWSPubSubAdapter{
		session: sess,
		snsSvc:  snsSvc,
		sqsSvc:  sqsSvc,
	}, nil
}

func (ps *AWSPubSubAdapter) Publish(topicARN string, message interface{}, messageAttributes map[string]interface{}, isCompressed bool) error {
	finalPayload := message
	var err error
	if isCompressed {
		finalPayload, err = compressPayload(finalPayload)
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}
		messageAttributes["isCompressed"] = isCompressed
	}

	jsonString, err := json.Marshal(compressPayload)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	if messageAttributes["source"] == nil {
		return fmt.Errorf("should have source key in messageAttributes")
	}
	if messageAttributes["contains"] == nil {
		return fmt.Errorf("should have contains key in messageAttributes")
	}
	if messageAttributes["eventType"] == nil {
		return fmt.Errorf("should have eventType key in messageAttributes")
	}
	awsMessageAttributes := map[string]*sns.MessageAttributeValue{}
	if messageAttributes != nil {
		awsMessageAttributes, _ = BindAttributes(messageAttributes)
	}
	result, err := ps.snsSvc.Publish(&sns.PublishInput{
		Message:           aws.String(string(jsonString)),
		TopicArn:          aws.String(topicARN),
		MessageAttributes: awsMessageAttributes,
	})
	if err != nil {
		fmt.Println("Error publishing message to SNS:", err)
		return err
	}
	fmt.Println("Published message to SNS with ID:", *result.MessageId)
	return nil
}

func (ps *AWSPubSubAdapter) PollMessages(topicARN string, handler func(message string) error) error {
	result, err := ps.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(topicARN),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(5),
		WaitTimeSeconds:     aws.Int64(20),
	})

	if err != nil {
		return err
	}

	for _, message := range result.Messages {
		fmt.Println("Received message to SQS:", message)
		newData, err := decompressPayload([]byte(*message.Body))
		if err != nil {
			return err
		}
		err = handler(fmt.Sprintf("%v", newData))
		if err != nil {
			return err
		}

		_, err = ps.sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(topicARN),
			ReceiptHandle: message.ReceiptHandle,
		})

		if err != nil {
			return err
		}
	}
	return nil
}

// not using this for v1
// func (ps *AWSPubSubAdapter) Subscribe(topicARN string, handler func(message string) error) error {
// 	subscribeOutput, err := ps.snsSvc.Subscribe(&sns.SubscribeInput{
// 		Protocol: aws.String("sqs"),
// 		Endpoint: aws.String(topicARN),
// 		TopicArn: aws.String(topicARN),
// 	})

// 	if err != nil {
// 		return err
// 	}
// 	subscriptionARN := *subscribeOutput.SubscriptionArn

// 	go ps.PollMessages(topicARN, handler)

// 	// Wait for termination signals to unsubscribe and cleanup
// 	ps.waitForTermination(topicARN, &subscriptionARN)

// 	return nil
// }

// func (ps *AWSPubSubAdapter) waitForTermination(topicARN string, subscriptionARN *string) {
// 	sigCh := make(chan os.Signal, 1)
// 	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

// 	<-sigCh // Wait for termination signal

// 	// Unsubscribe from the topic
// 	_, err := ps.snsSvc.Unsubscribe(&sns.UnsubscribeInput{
// 		SubscriptionArn: subscriptionARN,
// 	})
// 	if err != nil {
// 		log.Println("Error unsubscribing from the topic:", err)
// 	}

// 	// Delete the SQS queue
// 	_, err = ps.sqsSvc.DeleteQueue(&sqs.DeleteQueueInput{
// 		QueueUrl: aws.String(topicARN),
// 	})
// 	if err != nil {
// 		log.Println("Error deleting the queue:", err)
// 	}

// 	os.Exit(0) // Terminate the program
// }

func BindAttributes(attributes map[string]interface{}) (map[string]*sns.MessageAttributeValue, error) {
	boundAttributes := make(map[string]*sns.MessageAttributeValue)

	for key, value := range attributes {
		attrValue, _ := convertToAttributeValue(value)
		boundAttributes[key] = attrValue
	}
	return boundAttributes, nil
}

func convertToAttributeValue(value interface{}) (*sns.MessageAttributeValue, error) {
	// Perform type assertions or conversions based on the expected types of attributes
	// and create the appropriate sns.MessageAttributeValue object.

	switch v := value.(type) {
	case string:
		return &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}, nil
	case int, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return &sns.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(v)),
		}, nil
	case []string:
		return &sns.MessageAttributeValue{
			DataType:    aws.String("String.Array"),
			StringValue: aws.String(strings.Join(v, ",")),
		}, nil
	// Add more cases for other data types as needed

	default:
		return nil, fmt.Errorf("unsupported attribute value type: %T", value)
	}
}

func compressPayload(payload interface{}) ([]byte, error) {
	// Marshal the payload to JSON
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	// Create a buffer to store the compressed data
	compressedBuffer := new(bytes.Buffer)

	// Create a gzip writer with the compressed buffer
	gzipWriter := gzip.NewWriter(compressedBuffer)

	// Write the JSON payload to the gzip writer
	_, err = gzipWriter.Write(jsonBytes)
	if err != nil {
		return nil, err
	}

	// Close the gzip writer to flush any remaining data
	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}

	// Get the compressed data as a byte slice
	compressedPayload := compressedBuffer.Bytes()

	return compressedPayload, nil
}

func decompressPayload(compressedPayload []byte) (interface{}, error) {
	compressedReader := bytes.NewReader(compressedPayload)

	// Create a new GZIP reader
	gzipReader, err := gzip.NewReader(compressedReader)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	// Read the decompressed JSON payload from the GZIP reader
	decompressedBytes, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}

	var decompressedPayload interface{}

	// Unmarshal the JSON payload to the original payload type
	err = json.Unmarshal(decompressedBytes, &decompressedPayload)
	if err != nil {
		return nil, err
	}

	return decompressedPayload, nil
}
