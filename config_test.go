package kafkasub

import (
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	var subject *Config

	BeforeEach(func() {
		subject = NewConfig()
	})

	It("should init", func() {
		Expect(subject.Sub.Session.Timeout).To(Equal(30 * time.Second))
		Expect(subject.Sub.Heartbeat.Interval).To(Equal(3 * time.Second))
		Expect(subject.Metadata.Retry.Max).To(Equal(3))
		Expect(subject.Config.Version).To(Equal(sarama.V0_9_0_0))
	})

})
