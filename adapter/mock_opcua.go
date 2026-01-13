package adapter

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SourceMessage struct {
	Event RawWarehouseEvent
	Ack   func()
}

type ARWSource interface {
	Subscribe(ctx context.Context, fromCursor int64) (<-chan SourceMessage, error)
}

// MockOPCUASource simulates one OPC-UA server defined by URL and subscribed node list.
type MockOPCUASource struct {
	mu        sync.Mutex
	sourceSys string
	sourceID  string
	url       string
	sub       SubscriptionConfig

	nextCursor  int64
	history     []RawWarehouseEvent
	unackedByID map[int64]RawWarehouseEvent
}

func NewMockOPCUASource(sourceSystem, sourceID, url string, sub SubscriptionConfig) *MockOPCUASource {
	return &MockOPCUASource{
		sourceSys:   sourceSystem,
		sourceID:    sourceID,
		url:         url,
		sub:         sub,
		nextCursor:  1,
		history:     make([]RawWarehouseEvent, 0, 2048),
		unackedByID: make(map[int64]RawWarehouseEvent),
	}
}

func (m *MockOPCUASource) ack(cursor int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.unackedByID, cursor)
}

func (m *MockOPCUASource) generateEvent() RawWarehouseEvent {
	m.mu.Lock()
	defer m.mu.Unlock()

	cursor := m.nextCursor
	m.nextCursor++

	// Pick one subscribed node randomly
	node := m.sub.Nodes[rand.Intn(len(m.sub.Nodes))]

	// Use repeating UnitIDs across sources on purpose -> key namespacing matters
	unit := fmt.Sprintf("UNIT-%03d", rand.Intn(50)+1)
	from := fmt.Sprintf("%s:A-%02d", m.sourceID, rand.Intn(20)+1)
	to := fmt.Sprintf("%s:B-%02d", m.sourceID, rand.Intn(20)+1)
	mission := fmt.Sprintf("%s:MIS-%06d", m.sourceID, rand.Intn(999999)+1)

	fields := map[string]string{
		"unitId":    unit,
		"missionId": mission,
	}

	// Add move-ish fields for the move node
	if node.EventType == "InventoryMoved" {
		fields["from"] = from
		fields["to"] = to
	}

	ev := RawWarehouseEvent{
		Cursor:    cursor,
		NodeId:    node.NodeId,
		TimeStamp: time.Now().UTC(),
		Fields:    fields,
	}

	m.history = append(m.history, ev)
	m.unackedByID[cursor] = ev
	return ev
}

func (m *MockOPCUASource) Subscribe(ctx context.Context, fromCursor int64) (<-chan SourceMessage, error) {
	out := make(chan SourceMessage, 128)

	go func() {
		defer close(out)

		// Replay retained events > fromCursor
		m.mu.Lock()
		replay := make([]RawWarehouseEvent, 0, len(m.history))
		for _, ev := range m.history {
			if ev.Cursor > fromCursor {
				replay = append(replay, ev)
			}
		}
		m.mu.Unlock()

		for _, ev := range replay {
			select {
			case <-ctx.Done():
				return
			case out <- SourceMessage{
				Event: ev,
				Ack:   func() { m.ack(ev.Cursor) },
			}:
			}
		}

		interval := m.sub.PublishingIntervalMs
		if interval <= 0 {
			interval = 200
		}

		ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ev := m.generateEvent()
				out <- SourceMessage{
					Event: ev,
					Ack:   func() { m.ack(ev.Cursor) },
				}
			}
		}
	}()

	return out, nil
}
