# Case for Engineering Role in Warehousing & Site Logistics

## Design exercise

The proposed architecture design can be found in the [Architecture Design Document](Architecture_design.md).

## Coding Artifact

## Overview
This coding solution realizes a light-weight ingress adapter for integrating with an Automated Robot Warehouse (AWR).

The adapter connect to one or more OPC UA server nodes, which are assumed servers of the Warehouse.

The solution aims to demonstrate:
- Event-driven architecture principes
- Correctness under failure
- Ordering is partially implemented

### Notices:
The solution is aimed to be a prototype, a best effort POC.

Due to time constraints, the OPC-UA server and the Pulsar broker are mocked.
To achieve the mock, I have used GitHub Copilot and ChatGPT to generate the mocked versions to the specifications
which are not only close to real life examples, but also fit the problem space at hand.

Furthermore, Copilot was used to debug, review and comment some of the other parts of the code to provide not only the audience,
but also myself with more insights on the implementation.

## Architecture Context
This adapter corresponds to the [ADR](Architecture_design.md) Section 3.1 - ARW Adapter Ingress.

High level responsibilities:
- Connect to ARW system via OPC UA
- Subscribe to configured Nodes
- Normalize and enrich the incoming raw events, into a standardized event format
- Publish events to Pulsar
- Acknowledge the ARW source only after the broker handoff took place
- Persist per-source checkpoints for safe restart

## Running Locally

### Prerequisites
- Go 1.23 or higher
- Git

### Install dependencies
```bash
go mod download
```

### Configure the adapter
The adapter uses connection-config.json in the project root.
You can specify a different path using the CONFIG_PATH environment variable:
```bash
export CONFIG_PATH=/path/to/your/config.json
```

### Run the adapter
```bash
go run main.go
```

## Highlighted Concepts

### Standardized Event Format
The mocked OPC UA exposes the events, but they are raw,warehouse events, not necessarily business events

The adapter converts the raw events into domain specific events using configurable mapping rules.

Each emitted event follows a standardized format containing:
- EventId - Deterministic Hash for idempotency
- EventType - Domain specific event type
- Key - Partition key for FIFO ordering
- Timestamp - Event generation time
- Source information - Such as: System, Id, NodeIds

### FIFO Ordering:
Ordering is guaranteed per logical entity, not globally.
- Pulsar topic: aww.evets.canonical
- Message key: sourceId:keyFieldValue
Events with the same key are processed in FIFO order, while different keys are processed in parallel.

### ACK only after handoff:
The adapter acknowledges the ARW source only after:
- The event is successfully published to Pulsar
- Pulsar acknowledges the publishing

If publishing fails:
- The ARW event is not ACKed
- The source may replay the event

## Future improvements
### Connection resiliency and reconnect behaviour:

The current implementation assumes a stable connection to the OPC UA server.
In a production scenario, the adapter should implement reconnection logic.

The reconnection should be done in a continuous manner, in case the connection is disrupted, with keeping CPU usage
in mind. Additional 'circuit breaker' patterns may be implemented to avoid overwhelming the server with reconnection attempts.
The correct resubscription is extremely important, thus the implemented checkpoints should be utilized.

### Durable local stashing in case of Pulsar connection is down:
Currently, only in-memory buffering is implemented.
In a production scenario, a durable local storage should be used to persist events in case the Pulsar connection is down.

A simple SQLite service, or a segment file would already be a great improvement, with storing the cursor, eventId, payload and key.
This would ensure no events are lost in case of prolonged Pulsar outages.
