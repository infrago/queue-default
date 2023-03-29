package bus

import (
	"errors"
	"sync"
	"time"

	"github.com/infrago/queue"
	"github.com/infrago/util"
)

// mo

var (
	errRunning    = errors.New("Queue is running")
	errNotRunning = errors.New("Queue is not running")
)

type (
	defaultDriver  struct{}
	defaultConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		instance *queue.Instance

		runner *util.Runner
		queues map[string]chan *defaultMsg
	}

	defaultMsg struct {
		name     string
		data     []byte
		attempts int
	}
)

// 连接
func (driver *defaultDriver) Connect(inst *queue.Instance) (queue.Connect, error) {
	return &defaultConnect{
		instance: inst, runner: util.NewRunner(),
		queues: make(map[string]chan *defaultMsg, 0),
	}, nil
}

// 打开连接
func (connect *defaultConnect) Open() error {
	return nil
}
func (connect *defaultConnect) Health() (queue.Health, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return queue.Health{Workload: connect.actives}, nil
}

// 关闭连接
func (connect *defaultConnect) Close() error {
	return nil
}

// 省点事，一起注册了
// 除了单机和nats，不打算支持其它总线驱动了
// 以后要支持其它总线驱动的时候，再说
// 还有种可能，就是，nats中队列单独定义使用jetstream做持久的时候
// 那也可以同一个Register方法，定义实体来注册，加入Type或其它方式来区分
func (connect *defaultConnect) Register(name string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.queues[name] = make(chan *defaultMsg, 10)

	return nil
}

// 开始订阅者
func (connect *defaultConnect) Start() error {
	if connect.running {
		return errRunning
	}

	for _, cccc := range connect.queues {
		connect.runner.Run(func() {
			for {
				select {
				case msg := <-cccc:
					req := queue.Request{
						msg.name, msg.data, msg.attempts, time.Now(),
					}
					res := connect.instance.Serve(req)

					if res.Retry {
						msg.attempts++
						connect.publising(msg, res.Delay)
					}

				case <-connect.runner.Stop():
					return
				}
			}
		})
	}

	connect.running = true
	return nil
}

// 停止订阅
func (connect *defaultConnect) Stop() error {
	if false == connect.running {
		return errNotRunning
	}

	connect.runner.End()

	connect.running = false
	return nil
}

func (connect *defaultConnect) publising(msg *defaultMsg, delays ...time.Duration) error {
	if len(delays) > 0 {
		if qqq, ok := connect.queues[msg.name]; ok {
			time.AfterFunc(delays[0], func() {
				qqq <- msg
			})
		}
	} else {
		if qqq, ok := connect.queues[msg.name]; ok {
			qqq <- msg
		}
	}
	return nil
}

func (connect *defaultConnect) Publish(name string, data []byte) error {
	return connect.publising(&defaultMsg{name, data, 1})
}

func (connect *defaultConnect) DeferredPublish(name string, data []byte, delay time.Duration) error {
	return connect.publising(&defaultMsg{name, data, 1}, delay)
}

//------------------------- 默认队列驱动 end --------------------------
