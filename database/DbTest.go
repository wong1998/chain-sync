package database

import (
	"context"

	"github.com/wong1998/chain-sync/config"
)

func SetupDb() *DB {
	dbConfig := config.DbConfigTest()

	newDB, _ := NewDB(context.Background(), *dbConfig)
	return newDB
}
