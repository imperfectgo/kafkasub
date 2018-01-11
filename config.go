package kafkasub

import (
	"regexp"
	"time"

	"github.com/Shopify/sarama"
)

var minVersion = sarama.V0_9_0_0

type ConsumerMode uint8

const (
	ConsumerModeMultiplex ConsumerMode = iota
	ConsumerModePartitions
)

// Config extends sarama.Config with Sub specific namespace
type Config struct {
	sarama.Config

	// Sub is the namespace for subscription management properties
	Sub struct {
		// By default, messages and errors from the subscribed topics and partitions are all multiplexed and
		// made available through the consumer's Messages() and Errors() channels.
		//
		// Users who require low-level access can enable ConsumerModePartitions where individual partitions
		// are exposed on the Partitions() channel. Messages and errors must then be consumed on the partitions
		// themselves.
		Mode ConsumerMode

		Offsets struct {
			Synchronization struct {
				// The duration allowed for other clients to commit their offsets before resumption in this client, e.g. during a rebalance
				// NewConfig sets this to the Consumer.MaxProcessingTime duration of the Sarama configuration
				DwellTime time.Duration
			}
		}

		Session struct {
			// The allowed session timeout for registered consumers (defaults to 30s).
			// Must be within the allowed server range.
			Timeout time.Duration
		}

		Heartbeat struct {
			// Interval between each heartbeat (defaults to 3s). It should be no more
			// than 1/3rd of the Sub.Session.Timout setting
			Interval time.Duration
		}

		Topics struct {
			// An additional whitelist of topics to subscribe to.
			Whitelist *regexp.Regexp
			// An additional blacklist of topics to avoid. If set, this will precede over
			// the Whitelist setting.
			Blacklist *regexp.Regexp
		}
	}
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{
		Config: *sarama.NewConfig(),
	}
	c.Sub.Offsets.Synchronization.DwellTime = c.Consumer.MaxProcessingTime
	c.Sub.Session.Timeout = 30 * time.Second
	c.Sub.Heartbeat.Interval = 3 * time.Second
	c.Config.Version = minVersion
	return c
}

// Validate checks a Config instance. It will return a
// sarama.ConfigurationError if the specified values don't make sense.
func (c *Config) Validate() error {
	if c.Sub.Heartbeat.Interval%time.Millisecond != 0 {
		sarama.Logger.Println("Sub.Heartbeat.Interval only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Sub.Session.Timeout%time.Millisecond != 0 {
		sarama.Logger.Println("Sub.Session.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if !c.Version.IsAtLeast(minVersion) {
		sarama.Logger.Println("Version is not supported; 0.9. will be assumed.")
		c.Version = minVersion
	}
	if err := c.Config.Validate(); err != nil {
		return err
	}

	// validate the Sub values
	switch {
	case c.Sub.Heartbeat.Interval <= 0:
		return sarama.ConfigurationError("Sub.Heartbeat.Interval must be > 0")
	case c.Sub.Session.Timeout <= 0:
		return sarama.ConfigurationError("Sub.Session.Timeout must be > 0")
	case !c.Metadata.Full && c.Sub.Topics.Whitelist != nil:
		return sarama.ConfigurationError("Metadata.Full must be enabled when Sub.Topics.Whitelist is used")
	case !c.Metadata.Full && c.Sub.Topics.Blacklist != nil:
		return sarama.ConfigurationError("Metadata.Full must be enabled when Sub.Topics.Blacklist is used")
	}

	return nil
}
