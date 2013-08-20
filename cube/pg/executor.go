package pg

import (
	"database/sql/driver"
	"stash.cloudflare.com/go-stream/cube"
	"github.com/cevian/pq"
	"log"
	"reflect"
)

type Executor struct {
	table *Table
	conn  driver.Conn
}

func NewExecutor(t *Table, c driver.Conn) *Executor {
	return &Executor{t, c}
}

func (e *Executor) ExecErr(sql string, args ...interface{}) (driver.Result, error) {
	exec := e.conn.(driver.Execer)

	dargs := make([]driver.Value, len(args))
	for n, arg := range args {
		var err error
		dargs[n], err = driver.DefaultParameterConverter.ConvertValue(arg)
		if err != nil {
			log.Fatalf("sql: converting Exec argument #%d's type: %v", n, err)
		}
	}
	return exec.Exec(sql, dargs)
}

func (e *Executor) Exec(sql string, args ...interface{}) driver.Result {
	res, err := e.ExecErr(sql, args...)
	if err != nil {
		log.Fatal("Sql:", sql, "Err:", err)
	}
	return res
}

func (e *Executor) CreateBaseTable() {
	e.Exec(e.table.CreateTableSql(false))
}

func (e *Executor) DropAllTables() {
	e.Exec(e.table.DropTableSql())
}

func getPartition(p cube.Partition) Partition {
	switch pt := p.(type) {
	case cube.TimePartition:
		return &TimePartition{&pt}
	default:
		log.Fatal("Unknown Partition Type", reflect.TypeOf(pt))
	}
	panic("Never Here")
}

func (e *Executor) DropPartition(p cube.Partition) {
	e.Exec(e.table.DropPartitionTableSql(getPartition(p)))
}

func (e *Executor) UpsertCube(p cube.Partition, c cube.Cuber) {
	e.UpsertCubes(p, []cube.Cuber{c})
}

func (e *Executor) UpsertCubes(p cube.Partition, c []cube.Cuber) {
	tx, err := e.conn.Begin()
	if err != nil {
		log.Fatal("Error starting transaction", err)
	}

	part := getPartition(p)

	//TODO: have a cache of existing partition tables...dont recreate if not necessary
	e.Exec(e.table.CreatePartitionTableSql(part))

	e.Exec(e.table.CreateTemporaryCopyTableSql(part))
	cy := pq.NewCopierFromConn(e.conn)
	err = cy.Start(e.table.CopyTableSql(part))
	if err != nil {
		log.Fatal("Error starting copy", err)
	}

	for _, cube := range c {
		err = cy.Send(e.table.CopyDataFull(cube))
		if err != nil {
			log.Fatal("Error copying ", err)
		}
	}

	err = cy.Close()
	if err != nil {
		log.Fatal("Error Ending Copy ", err)
	}

	e.Exec(e.table.MergeCopySql(part))

	err = tx.Commit()
	if err != nil {
		log.Fatal("Error Committing tx ", err)
	}

}
