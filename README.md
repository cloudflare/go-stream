go-stream
=========

This library is a framework for stream processing analysis. It is meant to be used as a library for go programs
that need to do stream processing of large volumes of data.

It is made up of a graph connecting a source to 1 or more operators, terminating at a sink. 
Operators pass data from one to another with go channels. An example graph to encode objects to snappy is:

	var from *util.MemoryBuffer
	// fill up from
	
	var to *util.MemoryBuffer

 	ch := stream.NewOrderedChain()
 	ch.Add(source.NewNextReaderSource(from))
	timingOp, _, dur := timing.NewTimingOp()
 	ch.Add(timingOp)
 	ch.Add(compress.NewSnappyEncodeOp())
 	ch.Add(sink.NewWriterSink(to))
 
 	ch.Start()
 	
 	log.Printf("RES: Compress Snappy.\t\tRatio %v", float64(to.ByteSize())/float64(from.ByteSize()))
 	log.Printf("RES: Compress Snappy.\t\tBuffered Items: %d\tItem: %v\ttook: %v\trate: %d\tsize: %E\tsize/item: %E", to.Len(), *counter, *dur, int(    float64(*counter)/(*dur).Seconds()), float64(to.ByteSize()), float64(to.ByteSize())/float64(*counter))

Operators are the main components of a chain.
They process tuples to produce results.  Sources are operators with no output. Sinks are operators
with no input. Operators implement stream.Operator. If it takes input implements stream.In; if it produces output implements stream.Out. 

Mappers give a simple way to implement operators. mapper.NewOp() takes a function of the form 
func(input stream.Object, out Outputer) which processes the input and outputs it to the Outputer object. 
Mappers are automatically parallelized. Generators give a way to give mappers thread-local storage through closures.
You can also give mappers special functionality after they have finished processing the last tuple.

You can also split the data of a chain into other chains. stream.Fanout takes input and copies them to N other chains. 
Distributor takes input and puts it onto 1 of N chains according to a mapping function.

Chains can be ordered or unordered. Ordered chains preserve the order of tuples from input to output 
(although the operators still use parallelism).  

Compiling:
	go build
	
Testing:
	go test
