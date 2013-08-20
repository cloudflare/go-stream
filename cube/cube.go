package cube

import (
	"log"
	"reflect"
	"time"
)

type Dimensions interface{}
type Aggregates interface{}

type TimeIndexedDimensions interface {
	TimeIndex() time.Time
}

type Cuber interface {
	Insert(dimensions Dimensions, aggregates Aggregates)
	Visit(func(Dimensions, Aggregates))
	//	Data() map[Dimensions]Aggregates
}

type CubeDescriber interface {
	GetDimensions() Dimensions
	GetAggregates() Aggregates
}

type Cube struct {
	Dimensions Dimensions
	Aggregates Aggregates
	store      map[Dimensions]Aggregates
}

func validateDimensions(dim Dimensions) {
	d := reflect.ValueOf(dim)
	for i := 0; i < d.NumField(); i++ {
		if d.Field(i).Type().Kind() == reflect.Ptr {
			log.Fatal(d.Type(), " has a pointer field ", d.Type().Field(i).Name, " , which is not allowed in a dimension definition")
		}
	}
}

func validateAggregates(agg Aggregates) {
	a := reflect.ValueOf(agg)
	for i := 0; i < a.NumField(); i++ {
		if !a.Field(i).CanInterface() {
			log.Fatal(a.Type(), " is an aggregates struct with a unexported (lower case name) field ", a.Type().Field(i).Name, " , which is not allowed")
		}
	}
}

func NewCube(dimensions Dimensions, aggregates Aggregates) *Cube {
	validateDimensions(dimensions)
	validateAggregates(aggregates)
	st := make(map[Dimensions]Aggregates)
	return &Cube{dimensions, aggregates, st}
}

func (c *Cube) Insert(dimensions Dimensions, aggregates Aggregates) {
	val, ok := c.store[dimensions]
	if ok {
		aggValue := reflect.ValueOf(val)
		paramValue := reflect.ValueOf(aggregates)
		for i := 0; i < aggValue.NumField(); i++ {
			aggValue.Field(i).Interface().(Aggregate).Merge(paramValue.Field(i).Interface().(Aggregate))
		}
	} else {
		c.store[dimensions] = aggregates
	}

}

func (c *Cube) Data() map[Dimensions]Aggregates {
	return c.store
}

func (c *Cube) Visit(v func(Dimensions, Aggregates)) {
	for d, c := range c.store {
		v(d, c)
	}
}

func (c Cube) GetDimensions() Dimensions {
	return c.Dimensions
}

func (c Cube) GetAggregates() Aggregates {
	return c.Aggregates
}
