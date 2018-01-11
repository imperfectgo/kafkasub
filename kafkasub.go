package kafkasub

// Error instances are wrappers for internal errors with a context and
// may be returned through the consumer's Errors() channel
type Error struct {
	Ctx string
	error
}
