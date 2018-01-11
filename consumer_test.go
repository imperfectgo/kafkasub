package kafkasub

import (
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {

	var newConsumerOf = func(topics ...string) (*Consumer, error) {
		config := NewConfig()
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		return NewConsumer(testKafkaAddrs, topics, config)
	}

	var subscriptionsOf = func(c *Consumer) GomegaAsyncAssertion {
		return Eventually(func() map[string][]int32 {
			return c.Subscriptions()
		}, "10s", "100ms")
	}

	It("should init and share", func() {
		// start CS1
		cs1, err := newConsumerOf(testTopics...)
		Expect(err).NotTo(HaveOccurred())

		// CS1 should consume all 8 partitions
		subscriptionsOf(cs1).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))

		// start CS2
		cs2, err := newConsumerOf(testTopics...)
		Expect(err).NotTo(HaveOccurred())
		defer cs2.Close()

		// CS1 and CS2 should consume all 8 partitions
		subscriptionsOf(cs1).Should(HaveLen(2))
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-b", HaveLen(4)))

		subscriptionsOf(cs2).Should(HaveLen(2))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-b", HaveLen(4)))

		// shutdown CS1, CS2 should consume all 8 partitions
		Expect(cs1.Close()).NotTo(HaveOccurred())
		subscriptionsOf(cs2).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))
	})

	It("should allow more consumers than partitions", func() {
		cs1, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs1.Close()
		cs2, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs2.Close()
		cs3, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs3.Close()
		cs4, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())

		// start 4 consumers
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs3).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs4).Should(HaveKeyWithValue("topic-a", HaveLen(4)))

		// add a 5th consumer
		cs5, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs5.Close()

		// Now cs5 should consume all partitions
		subscriptionsOf(cs5).Should(HaveKeyWithValue("topic-a", HaveLen(4)))

		// make sure no errors occurred
		Expect(cs1.Errors()).ShouldNot(Receive())
		Expect(cs2.Errors()).ShouldNot(Receive())
		Expect(cs3.Errors()).ShouldNot(Receive())
		Expect(cs4.Errors()).ShouldNot(Receive())
		Expect(cs5.Errors()).ShouldNot(Receive())

		// close 4th
		Expect(cs4.Close()).To(Succeed())
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs3).Should(HaveKeyWithValue("topic-a", HaveLen(4)))
		subscriptionsOf(cs4).Should(BeEmpty())
		subscriptionsOf(cs5).Should(HaveKeyWithValue("topic-a", HaveLen(4)))

		// there should still be no errors
		Expect(cs1.Errors()).ShouldNot(Receive())
		Expect(cs2.Errors()).ShouldNot(Receive())
		Expect(cs3.Errors()).ShouldNot(Receive())
		Expect(cs4.Errors()).ShouldNot(Receive())
		Expect(cs5.Errors()).ShouldNot(Receive())
	})

	It("should be allowed to subscribe to partitions via white/black-lists", func() {
		config := NewConfig()
		config.Consumer.Return.Errors = true
		config.Sub.Topics.Whitelist = regexp.MustCompile(`topic-\w+`)
		config.Sub.Topics.Blacklist = regexp.MustCompile(`[bcd]$`)

		cs, err := NewConsumer(testKafkaAddrs, nil, config)
		Expect(err).NotTo(HaveOccurred())
		defer cs.Close()

		subscriptionsOf(cs).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
		}))
	})

	It("should support manual mark", func() {
		cs, err := newConsumerOf("topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs.Close()

		subscriptionsOf(cs).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3}},
		))

		cs.MarkPartitionOffset("topic-a", 1, 3)
		cs.MarkPartitionOffset("topic-a", 2, 4)

		offsets, err := cs.fetchOffsets(cs.Subscriptions(), cs.subs.Snapshot())
		Expect(err).NotTo(HaveOccurred())
		Expect(offsets).To(Equal(map[string]map[int32]int64{
			"topic-a": {0: -2, 1: 4, 2: 5, 3: -2},
		}))
	})

	It("should consume partitions", func() {
		count := int32(0)
		consume := func(consumerID string) {
			defer GinkgoRecover()

			config := NewConfig()
			config.Sub.Mode = ConsumerModePartitions
			config.Consumer.Offsets.Initial = sarama.OffsetOldest

			cs, err := NewConsumer(testKafkaAddrs, testTopics, config)
			Expect(err).NotTo(HaveOccurred())
			defer cs.Close()

			for pc := range cs.Partitions() {
				go func(pc PartitionConsumer) {
					defer pc.Close()

					for msg := range pc.Messages() {
						atomic.AddInt32(&count, 1)
						cs.MarkOffset(msg)
					}
				}(pc)
			}
		}

		go consume("A")
		go consume("B")
		go consume("C")

		Eventually(func() int32 {
			return atomic.LoadInt32(&count)
		}, "30s", "100ms").Should(BeNumerically(">=", 2000))
	})

	It("should consume/mark/resume", func() {
		acc := make(chan *testConsumerMessage, 20000)
		consume := func(consumerID string, max int32) {
			defer GinkgoRecover()

			cs, err := NewConsumer(testKafkaAddrs, testTopics, nil)
			Expect(err).NotTo(HaveOccurred())
			defer cs.Close()

			for msg := range cs.Messages() {
				acc <- &testConsumerMessage{*msg, consumerID}
				cs.MarkOffset(msg)

				if atomic.AddInt32(&max, -1) <= 0 {
					return
				}
			}
		}

		go consume("C1", 5300)
		time.Sleep(10 * time.Second) // wait for consumers to subscribe to topics
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(Equal(5300))

		go consume("C2", 3700)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(Equal(9000))

		go consume("C3", 1000)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(Equal(10000))

		go consume("C4", 4000)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(Equal(14000))

		go consume("C5", 1000)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(Equal(15000))

		close(acc)

		uniques := make(map[string][]string)
		for msg := range acc {
			key := fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
			uniques[key] = append(uniques[key], msg.ConsumerID)
		}
		Expect(uniques).To(HaveLen(15000))
	})

	It("should allow close to be called multiple times", func() {
		cs, err := newConsumerOf(testTopics...)
		Expect(err).NotTo(HaveOccurred())
		Expect(cs.Close()).NotTo(HaveOccurred())
		Expect(cs.Close()).NotTo(HaveOccurred())
	})

})
