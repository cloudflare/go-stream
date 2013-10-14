package pg

import "github.com/cloudflare/go-stream/cube"
import "github.com/cloudflare/go-stream/stream"
import "github.com/cloudflare/go-stream/stream/mapper"

import (
	"database/sql"
	"log"
)

func NewUpsertOp(dbconnect string, tableName string, cd cube.CubeDescriber) (stream.Operator, stream.ProcessedNotifier, *Executor) {
	db, err := sql.Open("postgres", dbconnect)
	if err != nil {
		log.Fatal(err)
	}
	drv := db.Driver()
	conn, err := drv.Open(dbconnect)
	if err != nil {
		log.Fatal(err)
	}

	table := MakeTable(tableName, cd)

	exec := NewExecutor(table, conn)

	//exec.CreateBaseTable()

	ready := stream.NewNonBlockingProcessedNotifier(2)

	f := func(input stream.Object, out mapper.Outputer) {
		in := input.(*cube.TimeRepartitionedCube)
		visitor := func(part cube.Partition, c cube.Cuber) {
			exec.UpsertCube(part, c)
		}
		in.VisitPartitions(visitor)
		ready.Notify(1)
	}

	exit := func() {
		log.Println("Db Upser Exit: ")
	}

	op := mapper.NewOpExitor(f, exit, "DbUpsert")
	op.Parallel = false
	return op, ready, exec
}
