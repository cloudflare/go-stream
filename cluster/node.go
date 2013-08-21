package cluster

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
