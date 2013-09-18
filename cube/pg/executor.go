package pg

import (
	"database/sql/driver"
	"github.com/cevian/pq"
	"logger"
	"reflect"
	"stash.cloudflare.com/go-stream/cube"
	"stash.cloudflare.com/go-stream/util/slog"
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
			slog.Fatalf("sql: converting Exec argument #%d's type: %v", n, err)
		}
	}
	return exec.Exec(sql, dargs)
}

func (e *Executor) Exec(sql string, args ...interface{}) driver.Result {
	res, err := e.ExecErr(sql, args...)
	if err != nil {
		slog.Logf(logger.Levels.Error, "Sql: %v Err: %v", sql, err)
	}
	return res
}

func (e *Executor) CreateBaseTable() {
	e.Exec(e.table.CreateTableSql(false))
}

func (e *Executor) DropAllTables() {
	e.Exec(e.table.DropTableSql())
}

func (e *Executor) CreateForeignTable(serverName string) {
	e.Exec(e.table.CreateForeignTableSql(serverName))
}

func (e *Executor) DropForeignTable(serverName string) {
	e.Exec(e.table.DropForeignTableSql(serverName))
}

func (e *Executor) CreateForeignTableView(serverNames []string, selfServerName string) {
	e.Exec(e.table.CreateForeignTableViewSql(serverNames, selfServerName))
}

func (e *Executor) DropForeignTableView() {
	e.Exec(e.table.DropForeignTableViewSql())
}

func getPartition(p cube.Partition) Partition {
	switch pt := p.(type) {
	case cube.TimePartition:
		return &TimePartition{&pt}
	default:
		slog.Fatalf("Unknown Partition Type %v", reflect.TypeOf(pt))
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
		slog.Fatalf("Error starting transaction %v", err)
	}

	part := getPartition(p)

	//TODO: have a cache of existing partition tables...dont recreate if not necessary
	e.Exec(e.table.CreatePartitionTableSql(part))

	e.Exec(e.table.CreateTemporaryCopyTableSql(part))
	cy := pq.NewCopierFromConn(e.conn)
	err = cy.Start(e.table.CopyTableSql(part))
	if err != nil {
		slog.Fatalf("Error starting copy %v", err)
	}

	for _, cube := range c {
		err = cy.Send(e.table.CopyDataFull(cube))
		if err != nil {
			slog.Fatalf("Error copying %v", err)
		}
	}

	err = cy.Close()
	if err != nil {
		slog.Fatalf("Error Ending Copy %v", err)
	}

	e.Exec(e.table.MergeCopySql(part))

	err = tx.Commit()
	if err != nil {
		slog.Fatalf("Error Committing tx %v ", err)
	}

}
