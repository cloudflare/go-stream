package pg

import "stash.cloudflare.com/go-stream/cube"
import (
	"log"
	"reflect"
)

func VisitWrapper(wrapper reflect.Value, visitor func(fieldValue reflect.Value, fieldDescription reflect.StructField)) {
	for i := 0; i < wrapper.NumField(); i++ {
		if wrapper.Field(i).Kind() == reflect.Struct && wrapper.Type().Field(i).Anonymous == true {
			VisitDimensions(wrapper.Field(i), visitor)
		} else {
			visitor(wrapper.Field(i), wrapper.Type().Field(i))
		}
	}
}

func VisitDimensions(wrapper reflect.Value, visitor func(fieldValue reflect.Value, fieldDescription reflect.StructField)) {
	VisitWrapper(wrapper, visitor)
}

func VisitAggregates(wrapper reflect.Value, visitor func(fieldValue reflect.Value, fieldDescription reflect.StructField)) {
	VisitWrapper(wrapper, visitor)
}

func AddDimensions(table *Table, rDims reflect.Value) {
	i := 0
	visitor := func(fieldValue reflect.Value, fieldDescription reflect.StructField) {
		col := getDimensionPgTypeVisit(fieldValue, fieldDescription)
		if 0 == i {
			_, ok := col.(*TimeCol)
			if ok {
				table.SetTimeCol(col)
			} else {
				log.Fatal("Expecting the primary time col as first dimension")
			}
		}
		i += 1
		table.AddDim(col)
	}
	VisitDimensions(rDims, visitor)
}

func AddAggregates(table *Table, rAggs reflect.Value) {
	visitor := func(fieldValue reflect.Value, fieldDescription reflect.StructField) {
		col := getAggregatePgTypeVisit(fieldValue, fieldDescription)
		table.AddAgg(col)
	}
	VisitAggregates(rAggs, visitor)
}

/*
func AddDimensions(table *Table, rDims reflect.Value) {
	for i := 0; i < rDims.NumField(); i++ {
		if rDims.Field(i).Kind() == reflect.Struct && rDims.Type().Field(i).Anonymous == true {
			AddDimensions(table, rDims.Field(i))
		} else {
			col := getDimensionPgType(i, rDims)
			if 0 == i {
				_, ok := col.(*TimeCol)
				if ok {
					table.SetTimeCol(col)
				} else {
					log.Fatal("Expecting the primary time col as first dimension")
				}
			}
			table.AddDim(col)
		}
	}
}*/

func MakeTable(name string, cd cube.CubeDescriber) *Table {
	rDims := reflect.ValueOf(cd.GetDimensions())
	rAggs := reflect.ValueOf(cd.GetAggregates())

	table := NewTable(name)

	AddDimensions(table, rDims)
	AddAggregates(table, rAggs)
	return table
}

func getDimensionPgTypeVisit(fieldValue reflect.Value, fieldDescription reflect.StructField) Column {
	name := fieldDescription.Name
	tagName := fieldDescription.Tag.Get("db")
	if tagName != "" {
		name = tagName
	}

	switch dt := fieldValue.Interface().(type) {
	default:
		log.Fatal("Unknown Dimension type ", reflect.TypeOf(dt), " for field ", name)
	case cube.TimeDimension:
		return &TimeCol{NewDefaultCol(name)}
	case cube.IntDimension:

		tn := "INT"
		tagtype := fieldDescription.Tag.Get("dbtype")
		if tagtype != "" {
			tn = tagtype
		}

		return &IntCol{NewDefaultCol(name), tn}
	case cube.StringDimension:
		return &StringCol{NewDefaultCol(name)}
	}

	return nil
}

func getAggregatePgTypeVisit(fieldValue reflect.Value, fieldDescription reflect.StructField) AggregateColumn {
	name := fieldDescription.Name
	tagName := fieldDescription.Tag.Get("db")
	if tagName != "" {
		name = tagName
	}

	var ca *cube.CountAggregate
	switch da := fieldValue.Type(); da {
	default:
		log.Fatal("Unknown Aggregate type", da, "for field", name)
	case reflect.TypeOf(ca):

		tn := "INT"
		tagtype := fieldDescription.Tag.Get("dbtype")
		if tagtype != "" {
			tn = tagtype
		}

		return &CountCol{&IntCol{NewDefaultCol(name), tn}}
	}
	return nil

}

/*
func getDimensionPgType(i int, dims reflect.Value) Column {
	name := dims.Type().Field(i).Name
	tagName := dims.Type().Field(i).Tag.Get("db")
	if tagName != "" {
		name = tagName
	}

	switch dt := dims.Field(i).Interface().(type) {
	default:
		log.Fatal("Unknown Dimension type ", reflect.TypeOf(dt), " for field ", dims.Type().Field(i).Name)
	case cube.TimeDimension:
		return &TimeCol{NewDefaultCol(name, i)}
	case cube.IntDimension:
		return &IntCol{NewDefaultCol(name, i)}
	case cube.StringDimension:
		return &IntCol{NewDefaultCol(name, i)}
	}

	return nil
}

func getAggregatePgType(index int, aggs reflect.Value) AggregateColumn {
	name := aggs.Type().Field(index).Name
	tagName := aggs.Type().Field(index).Tag.Get("db")
	if tagName != "" {
		name = tagName
	}

	var ca *cube.CountAggregate
	switch da := aggs.Field(index).Type(); da {
	default:
		log.Fatal("Unknown Aggregate type", da, "for field", aggs.Type().Field(index).Name)
	case reflect.TypeOf(ca):
		return &CountCol{&IntCol{NewDefaultCol(name, index)}}
	}
	return nil
}*/
