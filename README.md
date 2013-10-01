go-stream
=========

This library is a framework for stream processing analysis.

It is made up of a graph connecting a source to 1 or more operators, terminating at a sink. 
Operators pass data from one to another with go channels. An example graph is:

	input := make(chan stream.Object)

	passthruFn := func(in int) []int {
		return []int{in}
	}

	FirstOp := mapper.NewOp(passthruFn, "First PT no")
	FirstOp.SetIn(input)
	SecondOp := mapper.NewOp(passthruFn, "2nd PT no")

	ch := stream.NewChain()
	ch.Add(FirstOp)
	ch.Add(SecondOp)
	
	ch.Start()

Now any data sent through the input channel will be processed by the library.

Compiling:
	go build
	
Testing:
	go test
