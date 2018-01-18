package kafkasub

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	client    *Client
	ownClient bool

	consumer sarama.Consumer
	subs     *partitionMap

	bootstrapOffsets map[TopicPartition]int64
	coreTopics       []string
	extraTopics      []string

	dying, dead chan none
	closeOnce   sync.Once

	consuming  int32
	messages   chan *sarama.ConsumerMessage
	errors     chan error
	partitions chan PartitionConsumer
}

// NewConsumer initializes a new consumer
func NewConsumer(addrs []string, topics []string, config *Config, options ...Option) (*Consumer, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	consumer, err := NewConsumerFromClient(client, topics, options...)
	if err != nil {
		return nil, err
	}
	consumer.ownClient = true
	return consumer, nil
}

// NewConsumerFromClient initializes a new consumer from an existing client.
//
// Please note that clients cannot be shared between consumers (due to Kafka internals),
// they can only be re-used which requires the user to call Close() on the first consumer
// before using this method again to initialize another one. Attempts to use a client with
// more than one consumer at a time will return errors.
func NewConsumerFromClient(client *Client, topics []string, options ...Option) (*Consumer, error) {
	if !client.claim() {
		return nil, errClientInUse
	}

	consumer, err := sarama.NewConsumerFromClient(client.Client)
	if err != nil {
		client.release()
		return nil, err
	}

	sort.Strings(topics)
	c := &Consumer{
		client:   client,
		consumer: consumer,
		subs:     newPartitionMap(),

		coreTopics: topics,

		dying: make(chan none),
		dead:  make(chan none),

		messages:   make(chan *sarama.ConsumerMessage),
		errors:     make(chan error, client.config.ChannelBufferSize),
		partitions: make(chan PartitionConsumer, 1),
	}

	for _, opt := range options {
		opt.apply(c)
	}

	go c.mainLoop()
	return c, nil
}

// Messages returns the read channel for the messages that are returned by
// the broker.
//
// This channel will only return if Config.Group.Mode option is set to
// ConsumerModeMultiplex (default).
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// Partitions returns the read channels for individual partitions of this broker.
//
// This will channel will only return if Config.Group.Mode option is set to
// ConsumerModePartitions.
//
// The Partitions() channel must be listened to for the life of this consumer;
// when a rebalance happens old partitions will be closed (naturally come to
// completion) and new ones will be emitted. The returned channel will only close
// when the consumer is completely shut down.
func (c *Consumer) Partitions() <-chan PartitionConsumer { return c.partitions }

// Errors returns a read channel of errors that occur during offset management, if
// enabled. By default, errors are logged and not returned over this channel. If
// you want to implement any custom error handling, set your config's
// Consumer.Return.Errors setting to true, and read from this channel.
func (c *Consumer) Errors() <-chan error { return c.errors }

// HighWaterMarks returns the current high water marks for each topic and partition
// Consistency between partitions is not guaranteed since high water marks are updated separately.
func (c *Consumer) HighWaterMarks() map[string]map[int32]int64 { return c.consumer.HighWaterMarks() }

// MarkOffset marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (c *Consumer) MarkOffset(msg *sarama.ConsumerMessage) {
	c.subs.Fetch(msg.Topic, msg.Partition).MarkOffset(msg.Offset + 1)
}

// MarkPartitionOffset marks an offset of the provided topic/partition as processed.
// See MarkOffset for additional explanation.
func (c *Consumer) MarkPartitionOffset(topic string, partition int32, offset int64) {
	c.subs.Fetch(topic, partition).MarkOffset(offset + 1)
}

// Subscriptions returns the consumed topics and partitions.
func (c *Consumer) Subscriptions() map[string][]int32 {
	return c.subs.Info()
}

// Offsets returns current offsets for consumed topics and partitions.
func (c *Consumer) Offsets() map[TopicPartition]int64 {
	return c.subs.Snapshot()
}

// Close safely closes the consumer and releases all resources
func (c *Consumer) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.dying)
		<-c.dead

		if e := c.release(); e != nil {
			err = e
		}
		if e := c.consumer.Close(); e != nil {
			err = e
		}
		close(c.messages)
		close(c.errors)
		close(c.partitions)

		// drain
		for range c.messages {
		}
		for range c.errors {
		}
		for p := range c.partitions {
			_ = p.Close()
		}

		c.client.release()
		if c.ownClient {
			if e := c.client.Close(); e != nil {
				err = e
			}
		}
	})
	return
}

func (c *Consumer) mainLoop() {
	defer close(c.dead)
	defer atomic.StoreInt32(&c.consuming, 0)

	for {
		atomic.StoreInt32(&c.consuming, 0)

		// Check if close was requested
		select {
		case <-c.dying:
			return
		default:
		}

		// Start next consume cycle
		c.nextTick(c.bootstrapOffsets)

		// Remember previous offsets
		c.bootstrapOffsets = c.subs.Snapshot()
	}
}

func (c *Consumer) nextTick(snapshot map[TopicPartition]int64) {
	// Release subscriptions
	if err := c.release(); err != nil {
		c.rebalanceError(err)
		return
	}

	// Rebalance, fetch new subscriptions
	subs, err := c.rebalance()
	if err != nil {
		c.rebalanceError(err)
		return
	}

	// Coordinate loops, make sure everything is stopped on exit
	tomb := newLoopTomb()
	defer tomb.Close()

	// Subscribe to topic/partitions
	if err := c.subscribe(tomb, subs, snapshot); err != nil {
		c.rebalanceError(err)
		return
	}

	// Start topic watcher loop
	tomb.Go(c.twLoop)

	// Start partition watcher loop
	tomb.Go(c.pwLoop)
	atomic.StoreInt32(&c.consuming, 1)

	// Wait for signals
	select {
	case <-tomb.Dying():
	case <-c.dying:
	}
}

// topic watcher loop, triggered by the mainLoop
func (c *Consumer) twLoop(stopped <-chan none) {
	ticker := time.NewTicker(c.client.config.Metadata.RefreshFrequency / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			topics, err := c.client.Topics()
			if err != nil {
				c.handleError(&Error{Ctx: "topics", error: err})
				return
			}

			for _, topic := range topics {
				if !c.isKnownCoreTopic(topic) &&
					!c.isKnownExtraTopic(topic) &&
					c.isPotentialExtraTopic(topic) {
					return
				}
			}
		case <-stopped:
			return
		case <-c.dying:
			return
		}
	}
}

// partition watcher loop, triggered by the mainLoop
func (c *Consumer) pwLoop(stopped <-chan none) {
	ticker := time.NewTicker(c.client.config.Metadata.RefreshFrequency / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			topics := append(c.coreTopics, c.extraTopics...)
			for _, topic := range topics {
				partitions, err := c.client.Partitions(topic)
				if err != nil {
					c.handleError(&Error{Ctx: "topics", error: err})
					return
				}

				if !c.isKnownPartitions(topic, partitions) {
					return
				}
			}
		case <-stopped:
			return
		case <-c.dying:
			return
		}
	}
}

func (c *Consumer) rebalanceError(err error) {
	switch err {
	case sarama.ErrRebalanceInProgress:
	default:
		c.handleError(&Error{Ctx: "rebalance", error: err})
	}

	select {
	case <-c.dying:
	case <-time.After(c.client.config.Metadata.Retry.Backoff):
	}
}

func (c *Consumer) handleError(e *Error) {
	if c.client.config.Consumer.Return.Errors {
		select {
		case c.errors <- e:
		case <-c.dying:
			return
		}
	} else {
		sarama.Logger.Printf("%s error: %s", e.Ctx, e.Error())
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
func (c *Consumer) release() (err error) {
	// Stop all consumers
	c.subs.Stop()

	// Clear subscriptions on exit
	defer c.subs.Clear()

	// Wait for messages to be processed
	timeout := time.NewTimer(c.client.config.Sub.Offsets.Synchronization.DwellTime)
	defer timeout.Stop()

	select {
	case <-c.dying:
	case <-timeout.C:
	}

	return
}

// Performs a rebalance, part of the mainLoop()
func (c *Consumer) rebalance() (map[string][]int32, error) {
	sarama.Logger.Printf("kafkasub/consumer rebalance")

	allTopics, err := c.client.Topics()
	if err != nil {
		return nil, err
	}
	c.extraTopics = c.selectExtraTopics(allTopics)
	sort.Strings(c.extraTopics)

	topics := append(c.coreTopics, c.extraTopics...)
	subs := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		parts, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		subs[topic] = parts
	}
	return subs, nil
}

// Performs the subscription, part of the mainLoop()
func (c *Consumer) subscribe(tomb *loopTomb, subs map[string][]int32, snapshot map[TopicPartition]int64) (err error) {
	// fetch offsets
	offsets, err := c.fetchOffsets(subs, snapshot)
	if err != nil {
		return err
	}

	// create consumers in parallel
	var mu sync.Mutex
	var wg sync.WaitGroup

	for topic, partitions := range subs {
		for _, partition := range partitions {
			offset := offsets[topic][partition]

			wg.Add(1)
			go func(topic string, partition int32) {
				if e := c.createConsumer(tomb, topic, partition, offset); e != nil {
					mu.Lock()
					err = e
					mu.Unlock()
				}
				wg.Done()
			}(topic, partition)
		}
	}
	wg.Wait()

	if err != nil {
		_ = c.release()
	}
	return
}

// Fetches latest offsets for all subscriptions
func (c *Consumer) fetchOffsets(subs map[string][]int32, snapshot map[TopicPartition]int64) (map[string]map[int32]int64, error) {
	offsets := make(map[string]map[int32]int64, len(subs))
	for topic, partitions := range subs {
		offsets[topic] = make(map[int32]int64, len(partitions))
		for _, partition := range partitions {
			// Merge snapshot into new offsets
			offset, ok := snapshot[TopicPartition{Topic: topic, Partition: partition}]
			if !ok {
				offset = c.client.config.Consumer.Offsets.Initial
			}
			offsets[topic][partition] = offset
		}
	}

	return offsets, nil
}

func (c *Consumer) createConsumer(tomb *loopTomb, topic string, partition int32, offset int64) error {
	sarama.Logger.Printf("kafkasub/consumer: consume %s/%d", topic, partition)

	// Create partitionConsumer
	pc, err := newPartitionConsumer(c.consumer, topic, partition, offset)
	if err != nil {
		return err
	}

	// Store in subscriptions
	c.subs.Store(topic, partition, pc)

	// Start partition consumer goroutine
	tomb.Go(func(stopper <-chan none) {
		if c.client.config.Sub.Mode == ConsumerModePartitions {
			pc.WaitFor(stopper, c.errors)
		} else {
			pc.Multiplex(stopper, c.messages, c.errors)
		}
	})

	if c.client.config.Sub.Mode == ConsumerModePartitions {
		c.partitions <- pc
	}
	return nil
}

func (c *Consumer) selectExtraTopics(allTopics []string) []string {
	extra := allTopics[:0]
	for _, topic := range allTopics {
		if !c.isKnownCoreTopic(topic) && c.isPotentialExtraTopic(topic) {
			extra = append(extra, topic)
		}
	}
	return extra
}

func (c *Consumer) isKnownCoreTopic(topic string) bool {
	pos := sort.SearchStrings(c.coreTopics, topic)
	return pos < len(c.coreTopics) && c.coreTopics[pos] == topic
}

func (c *Consumer) isKnownExtraTopic(topic string) bool {
	pos := sort.SearchStrings(c.extraTopics, topic)
	return pos < len(c.extraTopics) && c.extraTopics[pos] == topic
}

func (c *Consumer) isPotentialExtraTopic(topic string) bool {
	rx := c.client.config.Sub.Topics
	if rx.Blacklist != nil && rx.Blacklist.MatchString(topic) {
		return false
	}
	if rx.Whitelist != nil && rx.Whitelist.MatchString(topic) {
		return true
	}
	return false
}

func (c *Consumer) isKnownPartitions(topic string, partitions []int32) bool {
	info := c.subs.Info()
	curParts := info[topic]
	if len(curParts) != len(partitions) {
		return false
	}

	for i := 0; i < len(partitions); i++ {
		if partitions[i] != curParts[i] {
			return true
		}
	}
	return false
}
