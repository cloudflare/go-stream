package mapper

import (
	"github.com/cloudflare/golog/logger"
	"reflect"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/util/slog"
)

type Worker interface {
	Map(input stream.Object, out Outputer)
	Validate(inCh chan stream.Object, typeName string) bool
}

type CallbackWorker struct {
	callback           reflect.Value
	closeCallback      func()
	finalItemsCallback *reflect.Value
	typename           string
}

func (w *CallbackWorker) sendSlice(slice *reflect.Value, out Outputer) {
	ch := out.Out(slice.Len())
	for i := 0; i < slice.Len(); i++ {
		value := slice.Index(i)
		ch <- value.Interface()
	}
}

func (w *CallbackWorker) Close(out Outputer) {
	if w.closeCallback != nil {
		w.closeCallback()
	}
	if w.finalItemsCallback != nil {
		res := w.finalItemsCallback.Call(nil)
		w.sendSlice(&(res[0]), out)
	}
}

func (w *CallbackWorker) Map(input stream.Object, out Outputer) {
	procArg := []reflect.Value{reflect.ValueOf(input)}
	//make([]reflect.Value, 1)
	//procArg[0] = reflect.ValueOf(input)
	//println(w.typename, " Type = ", procArg[0].Type().String())
	res := w.callback.Call(procArg)
	w.sendSlice(&(res[0]), out)
}

func (w *CallbackWorker) Validate(inCh chan stream.Object, typeName string) bool {

	calltype := w.callback.Type()

	slog.Logf(logger.Levels.Info, "Checking %s", typeName)

	//TODO: forbid struct results pass pointers to structs instead

	if calltype.Kind() != reflect.Func {
		slog.Fatalf("%s: `Processor` should be %s but got %s", typeName, reflect.Func, calltype.Kind())
	}
	if calltype.NumIn() != 1 {
		slog.Fatalf("%s: `Processor` should have 1 parameter but it has %d parameters", typeName, calltype.NumIn())
	}
	/*if !intype.AssignableTo(calltype.In(0)) {
		log.Panicf("%s: `Processor` should have a parameter or type %s but is %s", typeName, calltype.In(0), intype)
	}*/
	if calltype.NumOut() != 1 {
		slog.Fatalf("%s `Processor` should return 1 value but it returns %d values", typeName, calltype.NumOut())
	}
	if calltype.Out(0).Kind() != reflect.Slice {
		slog.Fatalf("%s `Processor` should return a slice but return %s", typeName, calltype.Out(0).Kind())
	}
	/*if calltype.Out(0).Elem() != outtype {
		log.Panicf("%s `Processor` should return a slice of %s but is %s", typeName, outtype, calltype.Out(0).Elem())
	}*/
	return true
}

/* avoids Value.Call on fast path */

type EfficientWorker struct {
	outCh              chan stream.Object
	callback           func(obj stream.Object, out Outputer)
	closeCallback      func()
	finalItemsCallback func(out chan stream.Object) (n int)
	typename           string
}

func (w *EfficientWorker) Start(out chan stream.Object) {
	w.outCh = out
}

func (w *EfficientWorker) Close() int {
	if w.closeCallback != nil {
		w.closeCallback()
		return 0
	}
	if w.finalItemsCallback != nil {
		return w.finalItemsCallback(w.outCh)
	}
	return 0
}

func (w *EfficientWorker) Map(input stream.Object, out Outputer) {
	w.callback(input, out)
}

func (w *EfficientWorker) Validate(inCh chan stream.Object, typeName string) bool {
	slog.Logf(logger.Levels.Info, "Checking %s", typeName)
	return true
}
