package main

import (
	"context"
	"os"
	"strings"
	"time"

	glkafka "github.com/Skyrin/go-lib/kafka"
	kafka_aws_ec2 "github.com/Skyrin/go-lib/kafka/aws/ec2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// In the case of a state machine, keys can be used with log.cleaner.enable to deduplicate entries with the same key. In that case, Kafka assumes that your application only cares about the most recent instance of a given key and the log cleaner deletes older duplicates of a given key only if the key is not null. This form of log compaction is controlled by the log.cleaner.delete.retention property and requires keys.

const (
	topic   = "test-topic"
	groupID = "test-groupCode"
)

func main() {
	dev := os.Getenv("DEV") == "true"
	url := os.Getenv("KAFKA_URL")
	region := os.Getenv("KAFKA_REGION")

	urlList := strings.Split(url, ",")
	if len(urlList) == 0 {
		log.Fatal().Msgf("no kafka url specified")
	}

	// Connect to Kafka
	var sm sasl.Mechanism
	var err error
	connConf := glkafka.ConnectionConfig{
		AddressList: urlList,
	}
	if dev {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		connConf.NoTLS = true
	} else {
		sm, err = kafka_aws_ec2.NewSASLMechanism(kafka_aws_ec2.SASLMechanismConfig{
			Region: region,
		})
		if err != nil {
			log.Fatal().Err(err).Msgf("failed to get sasl mechanism")
		}

		connConf.SASLMechanism = sm
	}

	c, err := glkafka.NewConn(connConf)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to get connection")
	}
	defer func() {
		log.Info().Msg("cleanup topics")

		// Cleanup the test topics
		if err := c.DeleteTopics(topic); err != nil {
			log.Warn().Err(err).Msgf("failed to delete topic: %s", topic)
		}

		if err := c.Close(); err != nil {
			log.Warn().Err(err).Msgf("failed to close connection")
		}
	}()

	// Create a test topic
	tc := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	if err := c.CreateTopics(tc); err != nil {
		log.Fatal().Err(err).Msgf("failed to create topic")
	}

	// Initialize a Kafka writer
	w := c.NewWriter(topic)
	defer func() {
		log.Info().Msg("closing writer")
		if err := w.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close writer")
		}
	}()

	// Send messages immediately
	w.BatchSize = 1
	w.Completion = func(mList []kafka.Message, err error) {
		if err != nil {
			log.Info().Err(err).Msg("error writing messages")
		}
		for _, m := range mList {
			log.Info().Msgf("wrote message: %s", m.Key)
		}
	}

	// Initialize a Kafka reader
	r := c.NewReader(kafka.ReaderConfig{
		Topic:            topic,
		GroupID:          groupID,
		MinBytes:         1,
		MaxBytes:         10000,
		ReadBatchTimeout: 1 * time.Second,
		RetentionTime:    60 * time.Second,
	})
	defer func() {
		log.Info().Msg("closing reader")
		if err := r.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close reader")
		}
	}()

	// Write a message
	b := []byte(`{"id":1,"value":"test"}`)
	wm := kafka.Message{
		Key:   []byte("test.key.1"),
		Value: b,
	}
	if err := w.WriteMessages(context.TODO(), wm); err != nil {
		log.Error().Err(err).Msgf("failed write message, topic: %s", w.Topic)
	}

	// Read a message
	rm, err := r.ReadMessage(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msgf("failed read message")
	} else {
		log.Info().Msgf("read message key: %s, value: %+v", rm.Key, rm.Value)
	}
}
