package kafkasub

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("int32Slice", func() {

	It("should diff", func() {
		Expect(((int32Slice)(nil)).Diff(int32Slice{1, 3, 5})).To(BeNil())
		Expect(int32Slice{1, 3, 5}.Diff((int32Slice)(nil))).To(Equal([]int32{1, 3, 5}))
		Expect(int32Slice{1, 3, 5}.Diff(int32Slice{1, 3, 5})).To(BeNil())
		Expect(int32Slice{1, 3, 5}.Diff(int32Slice{1, 2, 3, 4, 5})).To(BeNil())
		Expect(int32Slice{1, 3, 5}.Diff(int32Slice{2, 3, 4})).To(Equal([]int32{1, 5}))
		Expect(int32Slice{1, 3, 5}.Diff(int32Slice{1, 4})).To(Equal([]int32{3, 5}))
		Expect(int32Slice{1, 3, 5}.Diff(int32Slice{2, 5})).To(Equal([]int32{1, 3}))
	})

})
