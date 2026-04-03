package newkafka

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type fakeCloser struct {
	name       string
	closed     int
	closeErr   error
	closeOrder *[]string
}

func (f *fakeCloser) Close() error {
	f.closed++
	if f.closeOrder != nil {
		*f.closeOrder = append(*f.closeOrder, f.name)
	}
	return f.closeErr
}

func TestManagerCloseClosesTrackedClosersInReverseOrder(t *testing.T) {
	manager := &Manager{
		closers: make([]managedCloser, 0, 3),
	}

	order := make([]string, 0, 3)
	first := &fakeCloser{name: "first", closeOrder: &order}
	second := &fakeCloser{name: "second", closeOrder: &order}
	third := &fakeCloser{name: "third", closeOrder: &order}

	if err := manager.register(first); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := manager.register(second); err != nil {
		t.Fatalf("register second: %v", err)
	}
	if err := manager.register(third); err != nil {
		t.Fatalf("register third: %v", err)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	wantOrder := []string{"third", "second", "first"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("close order mismatch, got %v want %v", order, wantOrder)
	}

	if first.closed != 1 || second.closed != 1 || third.closed != 1 {
		t.Fatalf("tracked closers should close exactly once, got first=%d second=%d third=%d", first.closed, second.closed, third.closed)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("second close should be no-op, got %v", err)
	}

	if first.closed != 1 || second.closed != 1 || third.closed != 1 {
		t.Fatalf("second close should not close resources again, got first=%d second=%d third=%d", first.closed, second.closed, third.closed)
	}
}

func TestManagerRegisterAfterCloseClosesResourceAndReturnsError(t *testing.T) {
	manager := &Manager{
		closers: make([]managedCloser, 0, 1),
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	closer := &fakeCloser{name: "late"}
	err := manager.register(closer)
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("register after close error = %v, want %v", err, ErrManagerClosed)
	}
	if closer.closed != 1 {
		t.Fatalf("late closer should be closed immediately, got %d", closer.closed)
	}
}

func TestManagerRegisterAfterCloseReturnsWrappedCloseError(t *testing.T) {
	manager := &Manager{
		closers: make([]managedCloser, 0, 1),
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	closeErr := fmt.Errorf("close failed")
	err := manager.register(&fakeCloser{name: "late", closeErr: closeErr})
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("register after close error = %v, want %v", err, ErrManagerClosed)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("register after close should include closer error, got %v", err)
	}
}
