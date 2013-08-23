package pg

import (
	"bytes"
	//	"log"
	//"database/sql"
	"fmt"
	"reflect"
	"stash.cloudflare.com/go-stream/cube"
	"strings"
	"time"
)

type Column interface {
	Name() string
	TypeName() string
	PrintFormat() string
	PrintInterface(in interface{}) interface{}
}

type AggregateColumn interface {
	Column
	UpdateSql(intoTableName string, updateTableName string) string
}

type DefaultCol struct {
	name string
}

func NewDefaultCol(name string) *DefaultCol {
	return &DefaultCol{name}
}

func (c *DefaultCol) Name() string {
	return c.name
}

func (c *DefaultCol) PrintFormat() string {
	return "%v"
}

func (c *DefaultCol) PrintInterface(in interface{}) interface{} {
	return in
}

type IntCol struct {
	*DefaultCol
	tn string
}

func (c *IntCol) TypeName() string {
	return c.tn
}

type StringCol struct {
	*DefaultCol
}

func (c *StringCol) TypeName() string {
	return "VARCHAR(255)"
}

type TimeCol struct {
	*DefaultCol
}

func (c *TimeCol) TypeName() string {
	return "INT"
}

func (c *TimeCol) PrintInterface(in interface{}) interface{} {
	td := in.(cube.TimeDimension)
	return td.Unix()
}

type CountCol struct {
	*IntCol
}

func (c *CountCol) UpdateSql(intoTableName string, updateTableName string) string {
	cn := c.Name()
	return fmt.Sprintf("%s = %s.%s + %s.%s", cn, intoTableName, cn, updateTableName, cn)
}

type Partition interface {
	GetTableName(basename string) string
	GetConstraint(t *Table) string
}

type TimePartition struct {
	*cube.TimePartition
}

func (p TimePartition) GetTableName(basename string) string {
	return fmt.Sprintf("%s_%d", basename, p.Time().Unix())
}

func (p TimePartition) GetConstraint(t *Table) string {
	start := p.Time()
	end := start.Add(p.Duration()).Add(-time.Millisecond)
	return fmt.Sprintf("CHECK ( %s BETWEEN %d AND %d ) ", t.timecol.Name(), start.Unix(), end.Unix())
}

/*
type Col struct {
	name string
	typ  string
}

func (c *Col) Name() string {
	return c.name
}

func (c *Col) Type() string {
	return c.typ
}

func CreateSimpleCol(name string, typ string) Column {
	return &Col{name, typ}
}*/

type Table struct {
	name    string
	dimcols []Column
	aggcols []AggregateColumn
	format  string
	timecol Column
}

func NewTable(name string) *Table {
	return &Table{name, make([]Column, 0, 5), make([]AggregateColumn, 0, 3), "", nil}
}

func (t *Table) AddDim(c Column) {
	t.dimcols = append(t.dimcols, c)
}

func (t *Table) AddAgg(c AggregateColumn) {
	t.aggcols = append(t.aggcols, c)
}

func (t *Table) SetTimeCol(c Column) {
	t.timecol = c
}

func (t *Table) PrimaryKeySql() string {
	pkstr := make([]string, 0, len(t.dimcols))
	for _, col := range t.dimcols {
		pkstr = append(pkstr, fmt.Sprintf("%s", col.Name()))
	}
	return fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(pkstr, ", "))
}

func (t *Table) ColumnDefinitionsSql() string {
	cstr := make([]string, 0, len(t.aggcols)+len(t.dimcols))
	for _, col := range t.dimcols {
		cstr = append(cstr, fmt.Sprintf("%s %s", col.Name(), col.TypeName()))
	}
	for _, col := range t.aggcols {
		cstr = append(cstr, fmt.Sprintf("%s %s", col.Name(), col.TypeName()))
	}
	return strings.Join(cstr, ", ")
}

func (t *Table) CreateTableSql(temp bool) string {
	return t.CreateTableNameSql(temp, t.name)
}
func (t *Table) CreateTableNameSql(temp bool, name string) string {
	tempSql := " "
	if temp {
		tempSql = " TEMP "
	}

	return fmt.Sprintf("CREATE%sTABLE IF NOT EXISTS %s (%s, %s)", tempSql, name, t.ColumnDefinitionsSql(), t.PrimaryKeySql())
}

func (t *Table) CreateForeignTableSql(serverName string) string {
	name := t.ForeignTableName(serverName)
	return fmt.Sprintf("CREATE FOREIGN TABLE IF NOT EXISTS %s (%s) SERVER %s", name, t.ColumnDefinitionsSql(), serverName)
}

func (t *Table) DropForeignTableSql(serverName string) string {
	name := t.ForeignTableName(serverName)
	return fmt.Sprintf("DROP FOREIGN TABLE IF EXISTS %s CASCADE", name)
}

func (t *Table) CreateForeignTableViewSql(serverNames []string, includeSelf bool) string {
	lines := make([]string, 0, len(serverNames)+1)
	for _, sn := range serverNames {
		lines = append(lines, fmt.Sprintf("SELECT * FROM %s", t.ForeignTableName(sn)))
	}
	if includeSelf {
		lines = append(lines, fmt.Sprintf("SELECT * FROM %s", t.BaseTableName()))
	}
	union := strings.Join(lines, " UNION ")

	return fmt.Sprintf("CREATE VIEW %s %s", t.ForeignTablesViewName(), union)
}

func (t *Table) DropForeignTableViewSql() string {
	return fmt.Sprintf("DROP VIEW IF EXISTS %s", t.ForeignTablesViewName())
}

func (t *Table) BaseTableName() string {
	return t.name
}

func (t *Table) ForeignTableName(serverName string) string {
	return fmt.Sprintf("%s_ft_%s", t.BaseTableName(), serverName)
}

func (t *Table) ForeignTablesViewName() string {
	return fmt.Sprintf("%s_view", t.BaseTableName())
}

/*func (t *Table) GetPartitionTableName(start time.Time) string {
	return fmt.Sprintf("%s_%d", t.BaseTableName(), start.Unix())
}*/

func (t *Table) CreatePartitionTableSql(p Partition) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ( %s, %s ) INHERITS(%s)", p.GetTableName(t.BaseTableName()), t.PrimaryKeySql(), p.GetConstraint(t), t.BaseTableName())
}

func (t *Table) GetTemporaryCopyTableName(p Partition) string {
	return fmt.Sprintf("%s_copy_t", p.GetTableName(t.BaseTableName()))
}

func (t *Table) CreateTemporaryCopyTableSql(p Partition) string {
	return fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP", t.GetTemporaryCopyTableName(p), p.GetTableName(t.BaseTableName()))
}

func (t *Table) CopyTableSql(p Partition) string {
	return fmt.Sprintf("COPY %s FROM STDIN", t.GetTemporaryCopyTableName(p))
}

func (t *Table) DropTableSql() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t.BaseTableName())
}

func (t *Table) DropPartitionTableSql(p Partition) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", p.GetTableName(t.BaseTableName()))
}

func (t *Table) UpdateAggregateSql(intoTableName string, updateTableName string) string {
	cstr := make([]string, 0, len(t.aggcols))
	for _, col := range t.aggcols {
		cstr = append(cstr, fmt.Sprintf("%s", col.UpdateSql(intoTableName, updateTableName)))
	}
	return strings.Join(cstr, ", ")
}

func (t *Table) PrimaryKeyJoinConstraintsSql(intoTableName string, updateTableName string) string {
	pkstr := make([]string, 0, len(t.dimcols))
	for _, col := range t.dimcols {
		pkstr = append(pkstr, fmt.Sprintf("%s.%s = %s.%s", intoTableName, col.Name(), updateTableName, col.Name()))
	}
	return strings.Join(pkstr, " AND ")
}

func (t *Table) PrimaryKeyListSql(intoTableName string) string {
	pkstr := make([]string, 0, len(t.dimcols))
	for _, col := range t.dimcols {
		pkstr = append(pkstr, fmt.Sprintf("%s.%s", intoTableName, col.Name()))
	}
	return strings.Join(pkstr, ", ")
}

func (t *Table) GetOnePkColSql(intoTableName string) string {
	return fmt.Sprintf("%s.%s", intoTableName, t.dimcols[0].Name())
}

func (t *Table) MergeCopySql(p Partition) string {
	into := p.GetTableName(t.BaseTableName())
	update := t.GetTemporaryCopyTableName(p)
	sql := fmt.Sprintf(`WITH u as (UPDATE %s SET %s FROM %s AS up WHERE %s RETURNING %s)
	INSERT INTO %s SELECT %s.* FROM %s LEFT OUTER JOIN u ON (%s) WHERE %s IS NULL`,
		into, t.UpdateAggregateSql(into, "up"), update, t.PrimaryKeyJoinConstraintsSql(into, "up"), t.PrimaryKeyListSql(into),
		into, update, update, t.PrimaryKeyJoinConstraintsSql(update, "u"), t.GetOnePkColSql("u"))
	return sql
}

func (t *Table) ListColumnsSql() string {
	cstr := make([]string, 0, len(t.aggcols)+len(t.dimcols))
	for _, col := range t.dimcols {
		cstr = append(cstr, fmt.Sprintf("%s", col.Name()))
	}
	for _, col := range t.aggcols {
		cstr = append(cstr, fmt.Sprintf("%s", col.Name()))
	}

	return strings.Join(cstr, ", ")
}

func (t *Table) genCopyDataLineFormatString() string {
	fstr := make([]string, 0, len(t.aggcols)+len(t.dimcols))
	for _, col := range t.dimcols {
		fstr = append(fstr, col.PrintFormat())
	}
	for _, col := range t.aggcols {
		fstr = append(fstr, col.PrintFormat())
	}
	return fmt.Sprintf("%s\n", strings.Join(fstr, "\t"))
}

func (t *Table) getCopyDataLineFormatString() string {
	if t.format == "" {
		t.format = t.genCopyDataLineFormatString()
	}
	return t.format
}

func (t *Table) CopyDataLine(dims cube.Dimensions, aggs cube.Aggregates) string {
	ints := make([]interface{}, 0, len(t.aggcols)+len(t.dimcols))

	vDims := reflect.ValueOf(dims)
	vAggs := reflect.ValueOf(aggs)

	i := 0
	visitor := func(fieldValue reflect.Value, fieldDescription reflect.StructField) {
		ints = append(ints, t.dimcols[i].PrintInterface(fieldValue.Interface()))
		i += 1
	}
	VisitDimensions(vDims, visitor)

	j := 0
	visitorAgg := func(fieldValue reflect.Value, fieldDescription reflect.StructField) {
		ints = append(ints, t.aggcols[j].PrintInterface(fieldValue.Elem().Interface()))
		j += 1
	}

	VisitAggregates(vAggs, visitorAgg)
	/*
			for _, col := range t.dimcols {
				ints = append(ints, col.PrintInterface(vDims.Field(col.Index()).Interface()))
			}
		for _, col := range t.aggcols {
			ints = append(ints, col.PrintInterface(vAggs.Field(col.Index()).Elem().Interface()))
		}*/

	fs := t.getCopyDataLineFormatString()
	return fmt.Sprintf(fs, ints...)
}

func (t *Table) CopyDataFull(c cube.Cuber) []byte {
	var data bytes.Buffer

	f := func(d cube.Dimensions, a cube.Aggregates) {
		data.WriteString(t.CopyDataLine(d, a))
	}

	c.Visit(f)
	return data.Bytes()
}

/*func (t *Table) CopyDataCube(tableName string, c *cube.Cube, conn driver.Conn) {

	cy := pq.NewCopierFromConn(conn)

	err := cy.Start(fmt.Sprintf("COPY %s (%s) FROM STDIN", tableName, t.ListColumnsSql()))
	if err != nil {
		log.Fatal(err)
	}
	err = cy.Send(t.CopyDataFull(c))
	if err != nil {
		log.Fatal(err)
	}
	err = cy.End()
	if err != nil {
		log.Fatal(err)
	}

}*/
