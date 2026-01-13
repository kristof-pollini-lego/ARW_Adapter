package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StandardizedEvent is the standardized wrapper that the rest of the system consumes.
type StandardizedEvent struct {
	EventId       string `json:"eventId"`
	EventType     string `json:"eventType"`
	SchemaVersion string `json:"schemaVersion"`
	SourceSystem  string `json:"sourceSystem"`
	SourceId      string `json:"sourceId"`
	SourceUrl     string `json:"sourceUrl"`

	NodeId string `json:"NodeId"`

	Key           string `json:"key"` // correlation key for FIFO per key
	CorrelationID string `json:"correlationId,omitempty"`

	OccurredAt time.Time `json:"occurredAt"`
	ReceivedAt time.Time `json:"receivedAt"`

	Payload json.RawMessage `json:"payload"`
}

// RawWarehouseEvent represents an ARW event (e.g., from OPC-UA).
type RawWarehouseEvent struct {
	Cursor    int64
	NodeId    string
	TimeStamp time.Time
	Fields    map[string]string
}

// ValidateRaw ensures we have enough information to build a canonical event.
func ValidateRaw(e RawWarehouseEvent) error {
	if e.Cursor <= 0 {
		return errors.New("missing/invalid cursor")
	}
	if e.NodeId == "" {
		return errors.New("missing NodeId")
	}
	if e.TimeStamp.IsZero() {
		return errors.New("missing timestamp")
	}
	if e.Fields == nil {
		return errors.New("missing fields")
	}
	return nil
}

func RawToStandardized(sourceSystem, sourceId, sourceUrl string, node NodeConfig, raw RawWarehouseEvent) (StandardizedEvent, error) {
	// Derive key based on node config
	keyValue := raw.Fields[node.KeyField]
	if keyValue == "" {
		return StandardizedEvent{}, fmt.Errorf("missing keyField %q in event fields", node.KeyField)
	}

	// Namespace key to avoid collisions across sources
	key := fmt.Sprintf("%s:%s", sourceId, keyValue)

	// CorrelationId
	corr := raw.Fields["missionId"]

	// Payload: normalize into a standardized payload shape (still lightweight)
	payloadStruct := map[string]any{
		"fields":       raw.Fields,
		"sourceCursor": raw.Cursor,
		"sourceTS_utc": raw.TimeStamp.UTC().Format(time.RFC3339Nano),
	}

	payloadBytes, err := json.Marshal(payloadStruct)
	if err != nil {
		return StandardizedEvent{}, fmt.Errorf("payload marshal: %w", err)
	}

	// Deterministic event ID includes source + NodeId + cursor + key value
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%d|%s",
		sourceSystem, sourceId, sourceUrl, node.NodeId, raw.Cursor, keyValue)))
	eventID := hex.EncodeToString(h[:])

	event := StandardizedEvent{
		EventId:       eventID,
		EventType:     node.EventType,
		SchemaVersion: "1.0",
		SourceSystem:  sourceSystem,
		SourceId:      sourceId,
		SourceUrl:     sourceUrl,
		NodeId:        node.NodeId,
		Key:           key,
		CorrelationID: corr,
		OccurredAt:    raw.TimeStamp.UTC(),
		ReceivedAt:    time.Now().UTC(),
		Payload:       payloadBytes,
	}

	if event.EventId == "" || event.EventType == "" || event.Key == "" {
		return StandardizedEvent{}, errors.New("canonical eventelope missing required fields")
	}

	return event, nil
}
