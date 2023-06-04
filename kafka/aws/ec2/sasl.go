package kafka_aws_ec2

import (
	"context"

	"github.com/Skyrin/go-lib/e"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

const (
	// Error constants
	ECode080101 = e.Code0801 + "01"
	ECode080102 = e.Code0801 + "02"
)

// SASLMechanismConfig configuration options for NewSASLMechanism
type SASLMechanismConfig struct {
	Region string
}

// NewSASLMechanism returns a new SASL mechanism using the ec2 role credentials
func NewSASLMechanism(c SASLMechanismConfig) (sm sasl.Mechanism, err error) {
	if c.Region == "" {
		return nil, e.N(ECode080101, "region not specified")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, e.W(err, ECode080102)
	}
	cfg.Region = c.Region
	cfg.Credentials = aws.NewCredentialsCache(ec2rolecreds.New())

	return aws_msk_iam_v2.NewMechanism(cfg), nil
}
