package notifier

import (
	"sync"
)

type SubscriptionToken int

type Notifier struct {
	mu        sync.Mutex
	callbacks map[SubscriptionToken]func(Notification)
	nextToken SubscriptionToken

	bufferSize int
	buffer     []Notification
	head       int
	count      int
}

func NewNotifier(bufferSize int) *Notifier {
	n := &Notifier{
		callbacks:  make(map[SubscriptionToken]func(Notification)),
		bufferSize: bufferSize,
	}
	if bufferSize > 0 {
		n.buffer = make([]Notification, bufferSize)
	}
	return n
}

func (n *Notifier) Subscribe(callback func(Notification)) SubscriptionToken {
	n.mu.Lock()
	defer n.mu.Unlock()
	token := n.nextToken
	n.nextToken++
	n.callbacks[token] = callback
	return token
}

func (n *Notifier) Unsubscribe(token SubscriptionToken) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.callbacks, token)
}

func (n *Notifier) Emit(notification Notification) {
	n.mu.Lock()
	callbacks := make([]func(Notification), 0, len(n.callbacks))
	for _, cb := range n.callbacks {
		callbacks = append(callbacks, cb)
	}

	if n.bufferSize > 0 {
		index := (n.head + n.count) % n.bufferSize
		n.buffer[index] = notification
		if n.count < n.bufferSize {
			n.count++
		} else {
			n.head = (n.head + 1) % n.bufferSize
		}
	}
	n.mu.Unlock()

	for _, callback := range callbacks {
		callback(notification)
	}
}

func (n *Notifier) History() []Notification {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.bufferSize == 0 || n.count == 0 {
		return []Notification{}
	}

	history := make([]Notification, n.count)
	for i := 0; i < n.count; i++ {
		index := (n.head + i) % n.bufferSize
		history[i] = n.buffer[index]
	}
	return history
}
