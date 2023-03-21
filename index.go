package bus

import (
	"github.com/infrago/infra"
	"github.com/infrago/queue"
)

func Driver() queue.Driver {
	return &defaultDriver{}
}

func init() {
	infra.Register("default", Driver())
}
