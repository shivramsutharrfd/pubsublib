package redis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/go-redis/redis/v8"
)

type RedisPubSubAdapter struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisPubSubAdapter(addr string) (*RedisPubSubAdapter, error) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisPubSubAdapter{
		client: client,
		ctx:    ctx,
	}, nil
}

func (r *RedisPubSubAdapter) Publish(topicARN string, message interface{}, messageAttributes map[string]interface{}, isCompressed bool) error {
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
	if messageAttributes["source"] == nil {
		return fmt.Errorf("should have source key in messageAttributes")
	}
	if messageAttributes["contains"] == nil {
		return fmt.Errorf("should have contains key in messageAttributes")
	}
	if messageAttributes["eventType"] == nil {
		return fmt.Errorf("should have eventType key in messageAttributes")
	}
	messageWithAtrributs := map[string]interface{}{
		"messageAttributs": messageAttributes,
		"message":          message,
	}
	err = r.client.Publish(r.ctx, topicARN, messageWithAtrributs).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisPubSubAdapter) PollMessages(topic string, handler func(message string)) error {
	pubsub := r.client.Subscribe(r.ctx, topic)
	defer pubsub.Close()

	_, err := pubsub.ReceiveMessage(r.ctx)
	if err != nil {
		return err
	}

	channel := pubsub.Channel()
	for msg := range channel {
		handler(string(*&msg.Payload))
	}
	return nil
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
