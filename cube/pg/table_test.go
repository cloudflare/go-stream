package pg

import (
	"database/sql"
	"database/sql/driver"
	"github.com/cloudflare/go-stream/cube"
	"fmt"
	"log"
	"time"

	"testing"
)

func getConnParams() string {
	return "user=test dbname=test password=test"

}

func getDb() *sql.DB {
	db, err := sql.Open("postgres", getConnParams())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func getNewConn() driver.Conn {
	db := getDb()

	drv := db.Driver()
	conn, err := drv.Open(getConnParams())
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

type TestCubeDimensions struct {
	D1 cube.TimeDimension `db:"d1"`
	D2 cube.IntDimension  `db:"d2"`
}

type TestCubeAggregates struct {
	A1 *cube.CountAggregate `db:"a1"`
	A2 *cube.CountAggregate `db:"a2"`
}

func NewTestCube() *cube.Cube {
	return cube.NewCube(TestCubeDimensions{}, TestCubeAggregates{})
}

func InsertTestCube(c *cube.Cube, d1 time.Time, d2 int, A1 int, A2 int) {
	d := TestCubeDimensions{*cube.NewTimeDimension(d1), *cube.NewIntDimension(d2)}
	a := TestCubeAggregates{cube.NewCountAggregate(A1), cube.NewCountAggregate(A2)}
	c.Insert(d, a)
}

func ExampleCreate2() {
	cub := NewTestCube()

	table := MakeTable("Test", cub)

	fmt.Println(table.CreateTableSql(false))

	// Output: CREATE TABLE IF NOT EXISTS Test (d1 INT, d2 INT, a1 INT, a2 INT, PRIMARY KEY(d1, d2))
}

func ExampleDrop() {
	cub := NewTestCube()

	table := MakeTable("Test", cub)

	fmt.Println(table.DropTableSql())

	// Output: DROP TABLE IF EXISTS Test CASCADE
}

func ExampleInsertLine() {
	c := NewTestCube()

	table := MakeTable("Test", c)

	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), 1, 1, 1)
	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 1, 0, 0, time.UTC), 1, 1, 1)
	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 2, 0, 0, time.UTC), 1, 1, 1)

	for idim, iagg := range c.Data() {
		fmt.Print(table.CopyDataLine(idim, iagg))
	}
	// Output: 1257894000	1	1	1
	// 1257894060	1	1	1
	// 1257894120	1	1	1
}

func checkTable(table *Table, a1Value int, a2Value int, start time.Time, t *testing.T) {
	db := getDb()

	rows, err := db.Query("SELECT * FROM " + table.BaseTableName() + " ORDER BY d1 ASC")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var d1 int
		var d2 int
		var a1 int
		var a2 int
		rows.Scan(&d1, &d2, &a1, &a2)
		if a1 != a1Value {
			t.Fatal("A1 should be ", a1Value, " but is, ", a1)
		}
		if a2 != a2Value {
			t.Fatal("A2 should be", a2Value, " but is, ", a2)
		}

		if int64(d1) != start.Unix()+int64(count*60) {
			t.Fatal("Wrong time", d1)
		}
		if d2 != 1 {
			t.Fatal("D2 should be 1, ", d2)
		}

		count += 1
	}

	if count != 3 {
		t.Fatal("Wrong number of rows (exp 3):", count)
	}

}

func TestInsertToDb(t *testing.T) {
	c := NewTestCube()

	table := MakeTable("Test", c)

	start := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	exec := NewExecutor(table, getNewConn())

	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), 1, 1, 2)
	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 1, 0, 0, time.UTC), 1, 1, 2)
	InsertTestCube(c, time.Date(2009, time.November, 10, 23, 2, 0, 0, time.UTC), 1, 1, 2)

	part := cube.NewTimePartition(start, time.Hour)

	exec.CreateBaseTable()
	exec.DropPartition(part)
	exec.UpsertCube(part, c)

	checkTable(table, 1, 2, start, t)
	exec.UpsertCube(part, c)
	checkTable(table, 2, 4, start, t)
	exec.UpsertCube(part, c)
	checkTable(table, 3, 6, start, t)
}
