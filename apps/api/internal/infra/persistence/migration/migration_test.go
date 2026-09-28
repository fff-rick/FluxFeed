package migration

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestConcurrentMigrationErrorsAreRetryable(t *testing.T) {
	for _, number := range []uint16{1050, 1060, 1061} {
		if !isConcurrentMigrationError(&mysql.MySQLError{Number: number}) {
			t.Fatalf("mysql error %d should be retryable", number)
		}
	}
	if isConcurrentMigrationError(&mysql.MySQLError{Number: 1062}) {
		t.Fatal("duplicate data error must not be treated as a concurrent migration")
	}
	if isConcurrentMigrationError(errors.New("plain error")) {
		t.Fatal("non-MySQL error must not be retryable")
	}
}
