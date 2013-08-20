package stream

import "testing"
import "reflect"

var count int

func sender(c chan int, n int) {
	for i := 0; i < n; i++ {
		c <- 1
	}
}

func proc(i int) {
	count += i
}

func BenchmarkDirectSend(b *testing.B) {
	var c chan int = make(chan int)
	go sender(c, b.N)
	for i := 0; i < b.N; i++ {
		j := <-c
		proc(j)
	}
}

func BenchmarkReflectSend(b *testing.B) {
	c := make(chan int)
	go sender(c, b.N)

	sc := make([]reflect.SelectCase, 1)
	sc[0].Dir = reflect.SelectRecv
	sc[0].Chan = reflect.ValueOf(c)
	//sc[0].Send = reflect.ValueOf(0)

	f := reflect.ValueOf(proc)
	v := make([]reflect.Value, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ret, _ := reflect.Select(sc)
		v[0] = ret
		f.Call(v)
	}
}

/*
func BenchmarkReflectMakeFunc(b *testing.B) {

	p := func(in []reflect.Value) []reflect.Value {
		//channel
		select {
		case ret := <-in[0]:
			in[1](ret)
		}
		return nil
	}

	makeX := func(fptr interface{}) {
		// fptr is a pointer to a function.
		// Obtain the function value itself (likely nil) as a reflect.Value
		// so that we can query its type and then set the value.
		fn := reflect.ValueOf(fptr).Elem()

		// Make a function of the right type.
		v := reflect.MakeFunc(fn.Type(), p)

		// Assign it to the value fn represents.
		fn.Set(v)
	}

	var process func(chan int, func(int))
	makeX(&process)

	var c chan int = make(chan int)
	go sender(c, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		process(c, proc)
	}

}*/
