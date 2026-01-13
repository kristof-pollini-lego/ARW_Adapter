package adapter

import (
	"encoding/json"
	"fmt"
	"os"
)

type ConnectionConfig struct {
	SourceSystem string         `json:"sourceSystem"`
	Topic        string         `json:"topic"`
	Sources      []SourceConfig `json:"sources"`
}

type SourceConfig struct {
	SourceId       string             `json:"sourceId"`
	Protocol       string             `json:"protocol"`
	Url            string             `json:"url"`
	CheckpointPath string             `json:"checkpointPath"`
	Subscription   SubscriptionConfig `json:"subscription"`
}

type SubscriptionConfig struct {
	PublishingIntervalMs int          `json:"publishingIntervalMs"`
	Nodes                []NodeConfig `json:"nodes"`
}

type NodeConfig struct {
	NodeId    string `json:"nodeId"`
	EventType string `json:"eventType"` // This is a mapping from nodeId -> to domain specific event type
	KeyField  string `json:"keyField"`
}

func LoadConfig(path string) (ConnectionConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return ConnectionConfig{}, fmt.Errorf("read config: %w", err)
	}
	var c ConnectionConfig
	if err := json.Unmarshal(b, &c); err != nil {
		return ConnectionConfig{}, fmt.Errorf("unmarshal config: %w", err)
	}

	if c.SourceSystem == "" || c.Topic == "" {
		return ConnectionConfig{}, fmt.Errorf("config missing sourceSystem or topic")
	}
	if len(c.Sources) == 0 {
		return ConnectionConfig{}, fmt.Errorf("config has no sources")
	}
	for _, s := range c.Sources {
		if s.SourceId == "" || s.Url == "" {
			return ConnectionConfig{}, fmt.Errorf("source missing sourceId or url")
		}
		if len(s.Subscription.Nodes) == 0 {
			return ConnectionConfig{}, fmt.Errorf("source %s has no subscription nodes", s.SourceId)
		}
	}
	return c, nil
}
