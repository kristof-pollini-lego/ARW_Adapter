package adapter

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type ArwAdapterConfig struct {
	SourceSystem   string
	SourceId       string
	SourceUrl      string
	Topic          string
	QueueSize      int
	Workers        int
	CheckpointPath string

	Subscription SubscriptionConfig
}

type Adapter struct {
	cfg        ArwAdapterConfig
	src        ARWSource
	prod       Producer
	checkpoint *CheckpointStore

	queue          chan SourceMessage
	lastCheckpoint atomic.Int64

	// Lookup: NodeId -> NodeConfig
	nodeMap map[string]NodeConfig
}

func NewAdapter(cfg ArwAdapterConfig, src ARWSource, prod Producer) *Adapter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	nodeMap := make(map[string]NodeConfig, len(cfg.Subscription.Nodes))
	for _, n := range cfg.Subscription.Nodes {
		nodeMap[n.NodeId] = n
	}

	return &Adapter{
		cfg:        cfg,
		src:        src,
		prod:       prod,
		checkpoint: NewCheckpointStore(cfg.CheckpointPath),
		queue:      make(chan SourceMessage, cfg.QueueSize),
		nodeMap:    nodeMap,
	}
}

func (a *Adapter) Run(ctx context.Context) error {
	lastCursor, err := a.checkpoint.Load()
	if err != nil {
		return &ApplicationError{
			Kind: ErrorKindCheckpoint, Operation: "checkpoint.load", Retryable: true,
			Message: "failed to load checkpoint", Cause: err, SourceId: a.cfg.SourceId,
		}
	}
	a.lastCheckpoint.Store(lastCursor)

	log := slog.Default().With(
		"component", "adapter",
		"sourceSystem", a.cfg.SourceSystem,
		"SourceId", a.cfg.SourceId,
		"url", a.cfg.SourceUrl,
		"topic", a.cfg.Topic,
	)

	log.Info("adapter started",
		"op", "run.start",
		"lastCursor", lastCursor,
		"workers", a.cfg.Workers,
		"queueSize", a.cfg.QueueSize,
		"checkpointPath", a.cfg.CheckpointPath,
		"nodes", len(a.cfg.Subscription.Nodes),
	)

	sub, err := a.src.Subscribe(ctx, lastCursor)
	if err != nil {
		return &ApplicationError{
			Kind: ErrorKindSource, Operation: "source.subscribe", Retryable: true,
			Message: "failed to subscribe to ARW source", Cause: err, SourceId: a.cfg.SourceId,
		}
	}

	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		defer close(a.queue)

		for {
			select {
			case <-ctx.Done():
				log.Info("ingest stopping (context done)", "op", "ingest.stop")
				return
			case Message, ok := <-sub:
				if !ok {
					log.Info("source channel closed", "op", "ingest.sourceClosed")
					return
				}
				a.queue <- Message
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(a.cfg.Workers)

	for i := 0; i < a.cfg.Workers; i++ {
		workerID := i + 1
		go func() {
			defer wg.Done()

			wlog := log.With("worker", workerID)
			for {
				select {
				case <-ctx.Done():
					wlog.Info("worker stopping (context done)", "op", "worker.stop")
					return
				case Message, ok := <-a.queue:
					if !ok {
						wlog.Info("queue closed, worker exiting", "op", "worker.queueClosed")
						return
					}
					if err := a.handleOne(ctx, Message, workerID); err != nil {
						wlog.Error("handle failed",
							"op", "handleOne",
							"retryable", IsRetryable(err),
							"error", err,
						)
					}
				}
			}
		}()
	}

	<-ingestDone
	wg.Wait()

	log.Info("adapter stopped", "op", "run.stop")
	return nil
}

func (a *Adapter) handleOne(ctx context.Context, Message SourceMessage, workerID int) error {
	raw := Message.Event
	log := slog.Default().With(
		"component", "adapter",
		"SourceId", a.cfg.SourceId,
		"url", a.cfg.SourceUrl,
		"worker", workerID,
	)

	if err := ValidateRaw(raw); err != nil {
		return &ApplicationError{
			Kind: ErrorKindValidation, Operation: "raw.validate", Retryable: false,
			Message: "invalid raw event", Cause: err,
			Cursor: raw.Cursor, SourceId: a.cfg.SourceId, NodeId: raw.NodeId,
		}
	}

	nodeCfg, ok := a.nodeMap[raw.NodeId]
	if !ok {
		return &ApplicationError{
			Kind: ErrorKindValidation, Operation: "node.lookup", Retryable: false,
			Message: "received event for unsubscribed/unknown NodeId", Cause: nil,
			Cursor: raw.Cursor, SourceId: a.cfg.SourceId, NodeId: raw.NodeId,
		}
	}

	env, err := RawToStandardized(a.cfg.SourceSystem, a.cfg.SourceId, a.cfg.SourceUrl, nodeCfg, raw)
	if err != nil {
		return &ApplicationError{
			Kind: ErrorKindNormalize, Operation: "event.normalize", Retryable: false,
			Message: "failed to normalize/enrich", Cause: err,
			Cursor: raw.Cursor, SourceId: a.cfg.SourceId, NodeId: raw.NodeId,
		}
	}

	payload, err := json.Marshal(env)
	if err != nil {
		return &ApplicationError{
			Kind: ErrorKindNormalize, Operation: "event.marshal", Retryable: false,
			Message: "failed to marshal canonical envelope", Cause: err,
			Cursor: raw.Cursor, EventId: env.EventId, Key: env.Key, SourceId: a.cfg.SourceId, NodeId: raw.NodeId,
		}
	}

	var pubErr error
	for attempt := 1; attempt <= 3; attempt++ {
		pubErr = a.prod.Publish(ctx, a.cfg.Topic, env.Key, payload)
		if pubErr == nil {
			break
		}

		log.Warn("publish failed, retrying (ARW not ACKed)",
			"op", "pulsar.publish",
			"attempt", attempt,
			"cursor", raw.Cursor,
			"EventId", env.EventId,
			"eventType", env.EventType,
			"key", env.Key,
			"NodeId", env.NodeId,
			"error", pubErr,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt*50) * time.Millisecond):
		}
	}

	if pubErr != nil {
		return &ApplicationError{
			Kind: ErrorKindPublish, Operation: "pulsar.publish", Retryable: true,
			Message: "publish failed after retries (ARW not ACKed)", Cause: pubErr,
			Cursor: raw.Cursor, EventId: env.EventId, Key: env.Key, SourceId: a.cfg.SourceId, NodeId: raw.NodeId,
		}
	}

	// Broker ACK achieved -> ACK source
	Message.Ack()

	a.maybeSaveCheckpoint(raw.Cursor, env)

	log.Info("event handed off (broker ACK -> ARW ACK)",
		"op", "handoff.success",
		"cursor", raw.Cursor,
		"EventId", env.EventId,
		"eventType", env.EventType,
		"key", env.Key,
		"NodeId", env.NodeId,
	)
	return nil
}

func (a *Adapter) maybeSaveCheckpoint(cursor int64, env StandardizedEvent) {
	log := slog.Default().With("component", "adapter", "SourceId", a.cfg.SourceId)

	for {
		prev := a.lastCheckpoint.Load()
		if cursor <= prev {
			return
		}
		if a.lastCheckpoint.CompareAndSwap(prev, cursor) {
			if err := a.checkpoint.Save(cursor); err != nil {
				log.Warn("checkpoint save failed (replay safe due to downstream idempotency)",
					"op", "checkpoint.save",
					"cursor", cursor,
					"EventId", env.EventId,
					"key", env.Key,
					"error", err,
				)
			} else {
				log.Debug("checkpoint saved", "op", "checkpoint.save", "cursor", cursor)
			}
			return
		}
	}
}
