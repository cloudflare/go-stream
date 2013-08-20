package cluster

type Era interface {
	GetNodes() []Node
}

type SimpleEra struct {
	nodes []Node
}

func NewSimpleEra() *SimpleEra {
	return &SimpleEra{make([]Node, 0, 0)}
}

func (s *SimpleEra) Add(n Node) {
	s.nodes = append(s.nodes, n)
}

func (s *SimpleEra) GetNodes() []Node {
	return s.nodes
}
