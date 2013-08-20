package cluster

type Node interface {
	Name() string
	Ip() string
	Config() map[string]interface{} //??
}

type SimpleNode struct {
	name   string
	ip     string
	config map[string]interface{}
}

func NewSimpleNode(name string, ip string) *SimpleNode {
	return &SimpleNode{name, ip, make(map[string]interface{})}
}

func (n *SimpleNode) Name() string {
	return n.name
}

func (n *SimpleNode) Ip() string {
	return n.ip
}

func (n *SimpleNode) Config() map[string]interface{} {
	return n.config
}

func (n *SimpleNode) ConfigOption(name string) interface{} {
	return n.config[name]
}

func (n *SimpleNode) SetConfigOption(option string, value interface{}) *SimpleNode {
	n.config[option] = value
	return n
}
