package adapter

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"
)

type Producer interface {
	Publish(ctx context.Context, topic string, key string, payload []byte) error
}

type MockPulsarProducer struct {
	failRatePercent int64
	published       atomic.Int64
}

func NewMockPulsarProducer(failRatePercent int64) *MockPulsarProducer {
	return &MockPulsarProducer{failRatePercent: failRatePercent}
}

// Publish simulates broker-ack publish with random failure.
func (p *MockPulsarProducer) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	_ = topic
	_ = key
	_ = payload

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(rand.Intn(30)+5) * time.Millisecond):
	}

	if rand.Int63n(100) < p.failRatePercent {
		return errors.New("mock pulsar publish failed")
	}

	p.published.Add(1)
	return nil
}
