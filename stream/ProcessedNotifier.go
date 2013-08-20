package stream

type ProcessedNotifier interface {
	NotificationChannel() <-chan uint
	Notify(count uint)
	Blocking() bool
}

type SimpleProcessedNotifier struct {
	notify chan uint
	Block  bool
}

func NewProcessedNotifier() ProcessedNotifier {
	return &SimpleProcessedNotifier{make(chan uint, 1), true}
}

func NewNonBlockingProcessedNotifier(slack int) ProcessedNotifier {
	return &SimpleProcessedNotifier{make(chan uint, slack), false}
}

func (n *SimpleProcessedNotifier) Notify(count uint) {
	if n.Block {
		n.notify <- count
	} else {
		select {
		case n.notify <- count:
		case oldCount := <-n.notify:
			n.notify <- oldCount + count
		}
	}
}

func (n *SimpleProcessedNotifier) NotificationChannel() <-chan uint {
	return n.notify
}

func (n *SimpleProcessedNotifier) Blocking() bool {
	return n.Block
}
