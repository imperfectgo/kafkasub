package kafkasub

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("partitionConsumer", func() {
	var subject *partitionConsumer

	BeforeEach(func() {
		var err error
		subject, err = newPartitionConsumer(&mockConsumer{}, "topic", 0, 2000)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		close(subject.dead)
		Expect(subject.Close()).NotTo(HaveOccurred())
	})

	It("should set offset", func() {
		Expect(subject.Offset()).To(BeEquivalentTo(2000))
	})

	It("should recover from default offset if requested offset is out of bounds", func() {
		pc, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, 200)
		Expect(err).NotTo(HaveOccurred())
		defer pc.Close()
		close(pc.dead)

		offset := pc.Offset()
		Expect(offset).To(Equal(int64(-1)))
	})

	It("should update offset", func() {
		subject.MarkOffset(2001) // should set offset
		Expect(subject.Offset()).To(BeEquivalentTo(2001))

		subject.MarkOffset(1999) // should not update offset
		Expect(subject.Offset()).To(BeEquivalentTo(2001))

		subject.MarkOffset(2002) // should bump offset
		Expect(subject.Offset()).To(BeEquivalentTo(2002))
	})

	It("should not fail when nil", func() {
		blank := (*partitionConsumer)(nil)
		Expect(func() {
			_ = blank.Offset()
			blank.MarkOffset(2001)
		}).NotTo(Panic())
	})
})

var _ = Describe("partitionMap", func() {
	var subject *partitionMap

	BeforeEach(func() {
		subject = newPartitionMap()
	})

	It("should fetch/store", func() {
		Expect(subject.Fetch("topic", 0)).To(BeNil())

		pc, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, 2000)
		Expect(err).NotTo(HaveOccurred())

		subject.Store("topic", 0, pc)
		Expect(subject.Fetch("topic", 0)).To(Equal(pc))
		Expect(subject.Fetch("topic", 1)).To(BeNil())
		Expect(subject.Fetch("other", 0)).To(BeNil())
	})

	It("should return info", func() {
		pc0, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, 2000)
		Expect(err).NotTo(HaveOccurred())
		pc1, err := newPartitionConsumer(&mockConsumer{}, "topic", 1, 2000)
		Expect(err).NotTo(HaveOccurred())
		subject.Store("topic", 0, pc0)
		subject.Store("topic", 1, pc1)

		info := subject.Info()
		Expect(info).To(HaveLen(1))
		Expect(info).To(HaveKeyWithValue("topic", []int32{0, 1}))
	})

	It("should create snapshots", func() {
		pc0, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, 2000)
		Expect(err).NotTo(HaveOccurred())
		pc1, err := newPartitionConsumer(&mockConsumer{}, "topic", 1, 2000)
		Expect(err).NotTo(HaveOccurred())

		subject.Store("topic", 0, pc0)
		subject.Store("topic", 1, pc1)
		subject.Fetch("topic", 1).MarkOffset(2001)

		Expect(subject.Snapshot()).To(Equal(map[TopicPartition]int64{
			{"topic", 0}: 2000,
			{"topic", 1}: 2001,
		}))
	})
})
