package kafkasub

// An Option configures a consumer.
type Option interface {
	apply(*Consumer)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*Consumer)

func (f optionFunc) apply(c *Consumer) {
	f(c)
}

func WithBootstrapOffsets(offsets map[TopicPartition]int64) Option {
	return optionFunc(func(c *Consumer) {
		c.bootstrapOffsets = offsets
	})
}
