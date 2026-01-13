# Case for Engineering Role in Warehousing & Site Logistics - ADW Integration

## 1. Assumptions of the problem

- The system is integrating with an event-driven warehouse
- The events can represent:
  - Inventory state change
  - Robot movement
  - Storaging/retrival action
  - Operational events (errors, retries, maintenance)
- Real-time, continous operation is assumed
- Failures can delay:
  - Production
  - Inventory mismatches
  - Safety hazards or operational inefficiencies

## 2. Assumptions taken into consideration for the proposed solution

- The ARW system has a machine-readable interface::
  - OPC-UA servers
  - MQTT brokers
  - vendor API
- The events are immutable and timestamped
- A stable ordering of events is possible through available keys (e.g., timestamps, sequence numbers, IDs)
- Event order is meaningful
- Duplicate events are possible
  - Retries
  - Reconnects
  - Same event can be sent multiple times
- ARW system is capable of some level of retention of events
  - Keeping a local history/buffer of emitted events
  - Retention should be based on acknowledgement from the consuming system:
    - But can be time based (last N mintues/hours)
    - Or could be size based (last N events)
- The FIFO need is assumed to be a FIFO per correlated key
  - E.g., per robot, per inventory item, per storage location
  - This assumption is made due to a global FIFO would directly impact parallelism

## 3. Proposed Key Architectural Components

### 3.1 ARW Adapter (Ingress)

### 3.2 Event Handler (Broker)

### 3.3 Event Processing Services (Microservices)

### 3.4 Outbox Publisher

### 3.5 Downstream Integration services

### 3.6 Architecture Diagram

## 4. Data Flow

## 5. Failure Scenarios

## 6. Operational Considerations