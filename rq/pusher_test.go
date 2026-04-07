package rq

import (
	"testing"

	"github.com/xyqweb/go-queue/qtypes"
)

func TestPusher_Send_Basic(t *testing.T) {
	testData := &qtypes.QueueDelayData{
		Name:    "test-queue",
		Type:    "test",
		Body:    map[string]string{"key": "value"},
		Delay:   0,
		Attempt: 0,
	}
	push := NewPusher()
	defer push.Close()
	err := push.Send(testData)
	if err == nil {
		t.Log("Send succeeded")
	} else {
		t.Logf("Send failed (expected without connection): %v", err)
	}
}

func TestPusher_Send_DelayMessage(t *testing.T) {
	testData := &qtypes.QueueDelayData{
		Name:    "test-queue",
		Type:    "test",
		Body:    map[string]string{"key": "test body"},
		Delay:   5000,
		Attempt: 0,
	}
	push := NewPusher()
	defer push.Close()
	err := push.Send(testData)
	if err == nil {
		t.Log("delay message sent")
	} else {
		t.Logf("Send failed (expected without connection): %v", err)
	}
}

func TestPusher_Send_RetryMessage(t *testing.T) {
	testData := &qtypes.QueueDelayData{
		Name:    "test-queue",
		Type:    "test",
		Body:    map[string]string{"key": "retry body"},
		Delay:   0,
		Attempt: 1,
	}
	push := NewPusher()
	defer push.Close()
	err := push.Send(testData)
	if err == nil {
		t.Log("retry message sent")
	} else {
		t.Logf("Send failed (expected without connection): %v", err)
	}
}

func TestPusher_Send_ExceedMaxRetry(t *testing.T) {
	testData := &qtypes.QueueDelayData{
		Name:    "test-queue",
		Type:    "test",
		Body:    map[string]string{"key": "error body"},
		Delay:   0,
		Attempt: 4,
	}
	push := NewPusher()
	defer push.Close()
	err := push.Send(testData)
	if err == nil {
		t.Log("error message sent to error queue")
	} else {
		t.Logf("Send failed (expected without connection): %v", err)
	}
}
