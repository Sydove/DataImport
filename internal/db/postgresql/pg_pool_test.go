package postgresql

import (
	"testing"
	"time"

	_ "DataImport/internal/pkg/config"
)

func TestBatchInsert(t *testing.T) {
	InitDB()
	insertData := []Record{
		{
			Content:   "test content",
			Title:     "test title",
			OriginId:  "test origin",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			AccountId: 1,
		},
		{
			Content:   "test content2",
			Title:     "test title2",
			OriginId:  "test origin2",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			AccountId: 1,
		},
	}
	err := BatchInsert(insertData)
	if err != nil {
		t.Error(err)
	}
}
