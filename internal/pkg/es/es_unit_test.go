package es

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateStatsCountsFailuresAndConflicts(t *testing.T) {
	resp := BulkResponse{
		Items: []struct {
			Index struct {
				Index   string `json:"_index"`
				ID      string `json:"_id"`
				Version int    `json:"_version"`
				Result  string `json:"result"`
				Shards  struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				} `json:"_shards"`
				SeqNo       int `json:"_seq_no"`
				PrimaryTerm int `json:"_primary_term"`
				Status      int `json:"status"`
				Error       struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error,omitempty"`
			} `json:"index"`
		}{
			{Index: struct {
				Index   string `json:"_index"`
				ID      string `json:"_id"`
				Version int    `json:"_version"`
				Result  string `json:"result"`
				Shards  struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				} `json:"_shards"`
				SeqNo       int `json:"_seq_no"`
				PrimaryTerm int `json:"_primary_term"`
				Status      int `json:"status"`
				Error       struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error,omitempty"`
			}{ID: "1", Result: "created", Status: 201}},
			{Index: struct {
				Index   string `json:"_index"`
				ID      string `json:"_id"`
				Version int    `json:"_version"`
				Result  string `json:"result"`
				Shards  struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				} `json:"_shards"`
				SeqNo       int `json:"_seq_no"`
				PrimaryTerm int `json:"_primary_term"`
				Status      int `json:"status"`
				Error       struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error,omitempty"`
			}{ID: "2", Status: 409, Error: struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			}{Type: "version_conflict_engine_exception"}}},
		},
	}

	stats, failedIDs := resp.GenerateStats()
	if stats.Total != 2 {
		t.Fatalf("total = %d, want 2", stats.Total)
	}
	if stats.Created != 1 {
		t.Fatalf("created = %d, want 1", stats.Created)
	}
	if stats.Failed != 1 {
		t.Fatalf("failed = %d, want 1", stats.Failed)
	}
	if stats.Conflicts != 1 {
		t.Fatalf("conflicts = %d, want 1", stats.Conflicts)
	}
	if len(failedIDs) != 1 || failedIDs[0] != "2" {
		t.Fatalf("failed ids = %v, want [2]", failedIDs)
	}
}

func TestBuildBulkRequestBodyRejectsMarshalError(t *testing.T) {
	_, _, _, err := buildBulkRequestBody("article", []map[string]interface{}{
		{
			"id":      1,
			"invalid": make(chan int),
		},
	}, 1024)
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
}

func TestBuildBulkRequestBodyFormatsDocumentIDs(t *testing.T) {
	body, startID, endID, err := buildBulkRequestBody("article", []map[string]interface{}{
		{"id": json.Number("101"), "title": "first"},
		{"id": 102.0, "title": "second"},
	}, 1024)
	if err != nil {
		t.Fatalf("build bulk body: %v", err)
	}

	if startID != "101" || endID != "102" {
		t.Fatalf("range = %s-%s, want 101-102", startID, endID)
	}
	if !strings.Contains(body, `"_id":"101"`) {
		t.Fatalf("bulk body missing first id: %s", body)
	}
	if !strings.Contains(body, `"_id":"102"`) {
		t.Fatalf("bulk body missing second id: %s", body)
	}
}
