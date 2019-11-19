package test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
)

func WithMysqlDatabase(action func(db *sql.DB)) {
	ctx := context.Background()
	mysql := startMysqlContainer(ctx)
	defer terminateContainer(mysql, ctx)
	db, err := openDatabase(mysql, ctx)
	if err != nil {
		log.Panic(err)
	}
	defer closeResource(db)
	action(db)
}

func terminateContainer(c testcontainers.Container, ctx context.Context) {
	log.Println("Terminating mysql container")
	err := c.Terminate(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func startMysqlContainer(ctx context.Context) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.18",
		ExposedPorts: []string{"3306"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "test",
			"MYSQL_DATABASE":      "event_store",
			"MYSQL_USER":          "test",
			"MYSQL_PASSWORD":      "test",
		},
		WaitingFor: wait.ForLog("port: 3306"),
	}
	mysql, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Panic(err)
	}

	log.Println("Started mysql container")
	return mysql
}

func openDatabase(mysql testcontainers.Container, ctx context.Context) (*sql.DB, error) {
	port, err := mysql.MappedPort(ctx, "3306")
	if err != nil {
		log.Panic(err)
	}

	return sql.Open("mysql", fmt.Sprintf("test:test@tcp(127.0.0.1:%v)/event_store", port.Port()))
}

func closeResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Panic(err)
	}
}
