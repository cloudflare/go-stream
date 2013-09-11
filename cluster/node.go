package cluster

const (
	MAX_WEIGHT        = 100
	MEDIAN_WEIGHT     = 50
	MIN_DISK_TO_WORRY = 0.000809804
	DEGRADED_WEIGHT   = 30
)

type Node interface {
	Name() string
}

type GoServiceNode interface {
	Name() string
	Ip() string
	Port() string
}

type SimpleNode struct {
	name string
	ip   string
	port string
}

func NewSimpleNode(name string, ip string, port string) *SimpleNode {
	return &SimpleNode{name, ip, port}
}

func (n *SimpleNode) Name() string {
	return n.name
}

func (n *SimpleNode) Ip() string {
	return n.ip
}

func (n *SimpleNode) Port() string {
	return n.port
}

type WeightedNode struct {
	name   string
	ip     string
	port   string
	weight uint32
}

func NewWeightedNode(name string, ip string, port string, disk float32, load float32) *WeightedNode {
	wn := WeightedNode{name, ip, port, MEDIAN_WEIGHT}
	// For right now, keep it simple. Once a disk falls below a given threshold, trottle it a bit.
	// Don't pay attention to load now, to avoid flapping.
	if disk < MIN_DISK_TO_WORRY {
		wn.weight = DEGRADED_WEIGHT
	}
	return &wn
}

func (n *WeightedNode) Name() string {
	return n.name
}

func (n *WeightedNode) Ip() string {
	return n.ip
}

func (n *WeightedNode) Port() string {
	return n.port
}
