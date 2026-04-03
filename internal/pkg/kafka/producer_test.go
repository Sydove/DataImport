package newkafka

import (
	"errors"
	"testing"
)

func TestProducerStatsDoNotTreatFatalErrorsAsDeliveryFailures(t *testing.T) {
	producer := &Producer{}

	producer.markSent()
	producer.markFatalError(errors.New("all brokers down"))

	stats := producer.Stats()
	if stats.SentCount != 1 {
		t.Fatalf("sent count = %d, want 1", stats.SentCount)
	}
	if stats.FailedCount != 0 {
		t.Fatalf("failed count = %d, want 0", stats.FailedCount)
	}
	if stats.FatalErrorCount != 1 {
		t.Fatalf("fatal error count = %d, want 1", stats.FatalErrorCount)
	}
	if stats.PendingCount != 1 {
		t.Fatalf("pending count = %d, want 1", stats.PendingCount)
	}
	if producer.LastFatalError() == nil {
		t.Fatalf("last fatal error should be recorded")
	}
}

func TestProducerStatsClampPendingCountAtZero(t *testing.T) {
	producer := &Producer{}

	producer.markSent()
	producer.markDelivery(nil)
	producer.markDelivery(errors.New("delivery failed"))

	stats := producer.Stats()
	if stats.PendingCount != 0 {
		t.Fatalf("pending count = %d, want 0", stats.PendingCount)
	}
	if stats.SuccessCount != 1 {
		t.Fatalf("success count = %d, want 1", stats.SuccessCount)
	}
	if stats.FailedCount != 1 {
		t.Fatalf("failed count = %d, want 1", stats.FailedCount)
	}
}
