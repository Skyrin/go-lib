package kafka

import (
	"context"
	"crypto/tls"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

const (
	// Error constants
	ECode080001 = e.Code0800 + "01"
	ECode080002 = e.Code0800 + "02"
	ECode080003 = e.Code0800 + "03"
	ECode080004 = e.Code0800 + "04"
	ECode080005 = e.Code0800 + "05"
	ECode080006 = e.Code0800 + "06"
	ECode080007 = e.Code0800 + "07"
	ECode080008 = e.Code0800 + "08"
	ECode080009 = e.Code0800 + "09"
	ECode08000A = e.Code0800 + "0A"
	ECode08000B = e.Code0800 + "0B"
	ECode08000C = e.Code0800 + "0C"
	ECode08000D = e.Code0800 + "0D"
	ECode08000E = e.Code0800 + "0E"
	ECode08000F = e.Code0800 + "0F"
)

// ConnectionConfig for NewConn
type ConnectionConfig struct {
	AddressList   []string
	Context       context.Context
	NoTLS         bool
	SASLMechanism sasl.Mechanism
	Timeout       *time.Duration
	TLS           *tls.Config
}

// Connection a kafka connection with pre-initialized address list, dialer,
// transport and SASL mechanism
type Connection struct {
	Context context.Context

	addressList   []string
	conn          *kafka.Conn
	dialer        *kafka.Dialer
	saslMechanism sasl.Mechanism
	transport     *kafka.Transport
}

// NewConn create a new Kafka connection
func NewConn(conf ConnectionConfig) (c *Connection, err error) {
	if len(conf.AddressList) == 0 {
		return nil, e.N(ECode080001, "no address")
	}

	c = &Connection{
		addressList: conf.AddressList,
	}

	if conf.Context != nil {
		c.Context = conf.Context
	} else {
		c.Context = context.TODO()
	}

	if conf.SASLMechanism != nil {
		c.saslMechanism = conf.SASLMechanism

		dialer := &kafka.Dialer{
			DualStack: true,
			Timeout:   10 * time.Second,
		}
		transport := &kafka.Transport{}

		if conf.Timeout != nil {
			dialer.Timeout = *conf.Timeout
		}
		if conf.TLS != nil {
			dialer.TLS = conf.TLS
			transport.TLS = conf.TLS
		} else if !conf.NoTLS {
			dialer.TLS = &tls.Config{}
			transport.TLS = &tls.Config{}
		}

		dialer.SASLMechanism = c.saslMechanism
		transport.SASL = c.saslMechanism

		c.SetDialer(dialer)
		c.SetTransport(transport)
	} else {
		c.SetDialer(kafka.DefaultDialer)
		c.SetTransport(&kafka.Transport{})
	}

	if err := c.Connect(); err != nil {
		return c, e.W(err, ECode080002)
	}
	return c, nil
}

// SetDialer sets the connection's dialer
func (c *Connection) SetDialer(dialer *kafka.Dialer) {
	c.dialer = dialer
}

// SetTransport sets the connection's transport
func (c *Connection) SetTransport(transport *kafka.Transport) {
	c.transport = transport
}

// Reconnect closes and reopens a connection
func (c *Connection) Reconnect() (err error) {
	if err := c.Close(); err != nil {
		return e.W(err, ECode080003)
	}

	if err := c.Connect(); err != nil {
		return e.W(err, ECode080004)
	}

	return nil
}

// Connect opens a connection
func (c *Connection) Connect() (err error) {
	// If already connected, do nothing
	if c.conn != nil {
		return e.N(ECode080005, "already connected")
	}

	// Pick a random address in the list
	idx := rand.Intn(len(c.addressList))
	// c.conn, err = c.Dialer.DialContext(c.Context, "tcp", c.AddressList[idx])
	c.conn, err = c.dialer.DialContext(c.Context, "tcp", c.addressList[idx])
	if err != nil {
		return e.W(err, ECode080006)
	}

	return nil
}

// Close closes the connection
func (c *Connection) Close() (err error) {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return e.W(err, ECode080007)
		}

		c.conn = nil
	}

	return nil
}

// CreateTopics creates topics using the associated dialer
func (c *Connection) CreateTopics(tcList ...kafka.TopicConfig) (err error) {
	broker, err := c.conn.Controller()
	if err != nil {
		return e.W(err, ECode080008)
	}

	cc, err := c.dialer.DialContext(context.TODO(), "tcp",
		net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return e.W(err, ECode080009)
	}
	defer func() {
		if err := cc.Close(); err != nil {
			log.Warn().Err(err).Msgf("[%s]failed to close connection", ECode08000A)
		}
	}()

	if err := cc.CreateTopics(tcList...); err != nil {
		return e.W(err, ECode08000B)
	}

	return nil
}

// DeleteTopics creates topics using the associated dialer
func (c *Connection) DeleteTopics(topicList ...string) (err error) {
	broker, err := c.conn.Controller()
	if err != nil {
		return e.W(err, ECode08000C)
	}

	cc, err := c.dialer.DialContext(context.TODO(), "tcp",
		net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return e.W(err, ECode08000D)
	}
	defer func() {
		if err := cc.Close(); err != nil {
			log.Warn().Err(err).Msgf("[%s]failed to close connection", ECode08000E)
		}
	}()

	if err := cc.DeleteTopics(topicList...); err != nil {
		return e.W(err, ECode08000F)
	}

	return nil
}

// NewReader helper to return a new kafka reader using this connection's
// address list and dialer. If brokers or a dialer is set in the config, then
// those will be used instead
func (c *Connection) NewReader(rc kafka.ReaderConfig) (r *kafka.Reader) {
	if len(rc.Brokers) == 0 {
		rc.Brokers = c.addressList
	}

	if rc.Dialer == nil {
		rc.Dialer = c.dialer
	}

	return kafka.NewReader(rc)
}

// NewWriter helper to return a new kafka writer using this connection's
// address list and transport
func (c *Connection) NewWriter(topic string) (w *kafka.Writer) {
	return &kafka.Writer{
		Addr:      kafka.TCP(c.addressList...),
		Topic:     topic,
		Transport: c.transport,
	}
}
