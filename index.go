package bus

import "github.com/infrago/queue"

func Driver() queue.Driver {
	return &defaultDriver{}
}

func init() {
	queue.Register("default", Driver())
}
