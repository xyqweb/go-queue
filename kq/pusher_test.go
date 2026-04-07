package kq

import (
	"testing"

	"github.com/xyqweb/go-queue/qtypes"
)

func TestPusher_Send(t *testing.T) {
	err := NewPusher().Send(&qtypes.QueueData{
		Name:    "test-queue",
		Type:    "test",
		Body:    map[string]string{"key": "value"},
		Attempt: 0,
	})
	if err == nil {
		t.Log("Send succeeded")
	} else {
		t.Fatalf("Send failed error: %v", err)
	}
}
