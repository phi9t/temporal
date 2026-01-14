# Temporal Server Internals Deep Dive

This guide explains how core Temporal systems work: workflow orchestration, task execution, persistence, and signal/query handling.

---

## Table of Contents

1. [Workflow Orchestration](#1-workflow-orchestration)
2. [Task Execution System](#2-task-execution-system)
3. [Persistence Layer](#3-persistence-layer)
4. [Signal and Query Handling](#4-signal-and-query-handling)

---

## 1. Workflow Orchestration

### 1.1 Workflow Creation Flow

When a client starts a workflow, the request flows through these layers:

```
Client SDK
    ↓
Frontend Service (workflow_handler.go:382)
    ↓
History Service (handler.go:612)
    ↓
Starter (api/startworkflow/api.go:185)
    ↓
Persistence Layer
```

**Key Files:**
- `service/frontend/workflow_handler.go` - `StartWorkflowExecution()` validates and routes requests
- `service/history/api/startworkflow/api.go` - `Starter.Invoke()` orchestrates workflow creation

**Starter.Invoke() Steps:**
1. `prepare()` - Validates request, applies server defaults
2. `prepareNewWorkflow()` - Creates initial mutable state with events
3. `lockCurrentWorkflowExecution()` - Acquires lock on workflow ID
4. `createBrandNew()` - Persists to database
5. Handle conflicts if workflow ID already exists

### 1.2 Mutable State

Mutable state is the in-memory representation of a workflow execution. It contains all pending operations and can be reconstructed by replaying history.

**File:** `service/history/workflow/mutable_state_impl.go`

**Core State Maps:**
```go
pendingActivityInfoIDs    map[int64]*ActivityInfo       // Scheduled EventID → Info
pendingTimerInfoIDs       map[string]*TimerInfo         // Timer ID → Info
pendingChildExecutionInfoIDs map[int64]*ChildExecutionInfo
pendingRequestCancelInfoIDs  map[int64]*RequestCancelInfo
pendingSignalInfoIDs      map[int64]*SignalInfo
```

**Key Components:**
- `executionInfo` - Workflow type, namespace, task queue, timeouts
- `executionState` - Current state (RUNNING, COMPLETED, FAILED, etc.)
- `nextEventID` - Next event ID to assign
- `dbRecordVersion` - For optimistic locking

### 1.3 History Builder and Events

History is append-only. Each state change creates a history event.

**File:** `service/history/historybuilder/history_builder.go`

**Event Lifecycle:**
1. Events created via `EventFactory` methods
2. Batched in `memEventsBatches` (in-memory)
3. On transaction close, persisted to history store
4. `ScheduledIDToStartedID` maps track async completions

**Event Categories:**
- Workflow lifecycle: Started, Completed, Failed, TimedOut, Terminated
- Workflow task: Scheduled, Started, Completed, Failed, TimedOut
- Activity: Scheduled, Started, Completed, Failed, TimedOut, Canceled
- Timer: Started, Fired, Canceled
- Child workflow, signals, external workflow operations

### 1.4 Workflow Task State Machine

Workflow tasks drive workflow progress. Workers poll for tasks, execute workflow code, and return commands.

**File:** `service/history/workflow/workflow_task_state_machine.go`

**State Transitions:**
```
         Schedule
            ↓
    ┌──────────────┐
    │  Scheduled   │──────────────┐
    └──────────────┘              │
            ↓ Start               │ Timeout
    ┌──────────────┐              │
    │   Started    │──────────────┤
    └──────────────┘              │
       ↓         ↓                │
   Complete    Fail               ↓
       ↓         └────────> Transient Retry
   Commands                  (attempt > 1)
   Applied
```

**Task Types:**
- `NORMAL` - Standard workflow task
- `TRANSIENT` - Retry after failure (not persisted until success)
- `SPECULATIVE` - For optimistic updates (in-memory only)

**Key Methods:**
- `ApplyWorkflowTaskScheduledEvent()` - Creates task, sets workflow to RUNNING
- `ApplyWorkflowTaskStartedEvent()` - Worker picked up task
- `ApplyWorkflowTaskCompletedEvent()` - Processes worker commands

### 1.5 Transaction Pattern

Changes are batched and committed atomically.

**File:** `service/history/workflow/transaction_impl.go`

**Transaction Types:**
1. **Snapshot** - Brand new execution (all state created fresh)
2. **Mutation** - Incremental updates to existing execution
3. **Conflict Resolution** - Handles concurrent modification

**Commit Flow:**
```go
CloseTransactionAsMutation()
    → CreateWorkflowMutation()
    → Persistence.UpdateWorkflowExecution()
    → NotifyWorkflowMutationTasks()
    → Notify event watchers
```

### 1.6 Workflow Context and Locking

**File:** `service/history/workflow/context.go`

```go
type ContextImpl struct {
    workflowKey   WorkflowKey      // Namespace + Workflow + Run ID
    lock          *semaphore.PrioritySemaphore
    MutableState  MutableState
    updateRegistry update.Registry
}
```

**Lease Pattern:**
1. `Lock()` - Acquire exclusive access
2. `LoadMutableState()` - Load from DB if not cached
3. Perform operations
4. `CloseTransaction()` - Persist changes
5. `Unlock()` - Release lock

---

## 2. Task Execution System

### 2.1 Task Categories

**File:** `service/history/tasks/category.go`

| Category | ID | Purpose |
|----------|-----|---------|
| Transfer | 1 | Route tasks to matching service |
| Timer | 2 | Time-based triggers |
| Replication | 3 | Cross-cluster sync |
| Visibility | 4 | Search index updates |
| Archival | 5 | Archive completed workflows |
| MemoryTimer | 6 | In-memory speculative timers |
| Outbound | 7 | External system calls |

### 2.2 Task Generation

**File:** `service/history/workflow/task_generator.go`

Tasks are generated when events occur:

```go
// Workflow start
GenerateWorkflowStartTasks()
    → WorkflowRunTimeoutTask
    → WorkflowExecutionTimeoutTask

// Workflow task scheduled
GenerateScheduleWorkflowTaskTasks()
    → WorkflowTaskTimeoutTask (schedule-to-start)
    → TransferTask (route to matching)

// Activity scheduled
GenerateActivityTasks()
    → ActivityTask (transfer to matching)
    → ActivityTimeoutTask
```

### 2.3 Matching Service Architecture

The matching service connects task producers (history) with consumers (workers).

**File:** `service/matching/matching_engine.go`

**Three-Level Hierarchy:**
```
matchingEngineImpl
    └── taskQueuePartitionManagerImpl (logical partition)
            └── physicalTaskQueueManagerImpl (physical queue)
                    └── priMatcher (priority matcher)
```

**Core Operations:**
- `AddWorkflowTask()` / `AddActivityTask()` - Producer adds task
- `PollWorkflowTaskQueue()` / `PollActivityTaskQueue()` - Worker polls for task

### 2.4 Priority Matcher

**File:** `service/matching/pri_matcher.go`

Maintains two priority queues:
- `pollerPQ` - Waiting workers (task forwarders first, then FIFO)
- `taskPQ` - Waiting tasks (by priority level + backlog age)

**Matching Algorithm:**
```
AddTask arrives:
    1. Try sync match: MatchTaskImmediately()
       → Pop available poller from pollerPQ
       → Send task via responseC channel
    2. If no poller: SpoolTask() → persist to DB

PollTask arrives:
    1. Check taskPQ for waiting task
    2. If found: immediate match
    3. If empty: add poller to pollerPQ, wait
```

### 2.5 Sync vs Async Task Dispatch

**Sync Match (Fast Path):**
```
History Service → AddTask()
    → TrySyncMatch() → Poller waiting?
    → YES: Match immediately, return to worker
    → NO: Fall through to async
```

**Async Match (Backlog):**
```
AddTask() → SpoolTask()
    → taskWriter.Append() → DB
    → taskReader polls DB periodically
    → dispatchBufferedTasks() → priMatcher
    → Worker polls later → match
```

### 2.6 Task Queue Backlog

**File:** `service/matching/backlog_manager.go`

**Components:**
- `taskWriter` - Batches writes, allocates task IDs
- `taskReader` - Background DB polling
- `taskGC` - Garbage collection
- `ackManager` - Tracks completed tasks

**ID Allocation:**
```go
// Allocate IDs in blocks for efficiency
rangeID := atomic.AddInt64(&taskIDBlock, blockSize)
taskID := rangeID - blockSize + offset
```

### 2.7 Sticky Execution

Workers can cache workflow state. "Sticky" queues route tasks back to the same worker.

**File:** `service/matching/matching_engine.go:3133`

```go
func stickyWorkerAvailable(pm taskQueuePartitionManager) bool {
    return pm != nil && pm.HasPollerAfter("",
        time.Now().Add(-stickyPollerUnavailableWindow))  // 10 seconds
}
```

**Flow:**
1. Workflow task completes, worker reports sticky queue
2. Next task scheduled to sticky queue
3. If sticky worker polled within 10s: dispatch there
4. Otherwise: fall back to normal queue

### 2.8 Task Token

Task tokens identify specific task instances for completion.

```go
type taskToken struct {
    NamespaceID     string
    WorkflowID      string
    RunID           string
    ScheduledEventID int64
    Attempt         int32
    Clock           *VectorClock  // For consistency
}
```

Serialized and returned to worker on poll, sent back on completion.

---

## 3. Persistence Layer

### 3.1 Core Interfaces

**File:** `common/persistence/persistence_interface.go`

**Key Interfaces:**
```go
type ExecutionStore interface {
    CreateWorkflowExecution(ctx, request) (*Response, error)
    UpdateWorkflowExecution(ctx, request) (*Response, error)
    ConflictResolveWorkflowExecution(ctx, request) (*Response, error)
    GetWorkflowExecution(ctx, request) (*Response, error)

    // History operations
    AppendHistoryNodes(ctx, request) error
    ReadHistoryBranch(ctx, request) (*Response, error)
    ForkHistoryBranch(ctx, request) error
    DeleteHistoryBranch(ctx, request) error

    // Task operations
    AddHistoryTasks(ctx, request) error
    GetHistoryTasks(ctx, request) (*Response, error)
    CompleteHistoryTask(ctx, request) error
}

type ShardStore interface {
    GetOrCreateShard(ctx, request) (*Response, error)
    UpdateShard(ctx, request) error
    AssertShardOwnership(ctx, request) error
}
```

### 3.2 Execution State Storage

**Workflow Mutable State Blob:**
```go
type InternalWorkflowMutableState struct {
    ExecutionInfo     *DataBlob                    // WorkflowExecutionInfo
    ExecutionState    *DataBlob                    // State + status
    NextEventID       int64
    DBRecordVersion   int64                        // Optimistic locking

    ActivityInfos     map[int64]*DataBlob          // ScheduledEventID → blob
    TimerInfos        map[string]*DataBlob         // TimerID → blob
    ChildExecutionInfos map[int64]*DataBlob
    RequestCancelInfos  map[int64]*DataBlob
    SignalInfos       map[int64]*DataBlob

    BufferedEvents    []*DataBlob                  // Events not yet in history
    Checksum          *DataBlob                    // State integrity check
}
```

### 3.3 History Storage Model

History uses a tree structure supporting branches (for reset/continue-as-new).

**Tables:**
```
history_node:
    tree_id      - Root of history tree
    branch_id    - Specific branch
    node_id      - First event ID of batch
    txn_id       - Transaction ID (higher wins)
    prev_txn_id  - Chain to previous transaction
    data         - Serialized event batch

history_tree:
    tree_id
    branch_id
    branch       - HistoryBranch metadata with ancestors
```

**Branch Forking:**
```
Original: A → B → C → D → E
                    ↓ Fork at C
Forked:   A → B → C → F → G

Forked branch stores: Ancestors = [{original branch, up to node C}]
```

### 3.4 Optimistic Locking

**Pattern:** Version-based conflict detection prevents concurrent write conflicts.

**Cassandra (CAS):**
```go
batch.Query(templateUpdateExecution,
    newData,
    newVersion,            // New version to write
    shardID, namespaceID, workflowID, runID,
).SerialConsistency(gocql.Serial)

// Condition: IF db_record_version = expectedVersion
```

**SQL:**
```go
result, err := tx.ExecContext(ctx,
    `UPDATE executions SET data = ?, db_record_version = ?
     WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?
     AND db_record_version = ?`,
    newData, newVersion,
    shardID, namespaceID, workflowID, runID,
    expectedVersion)
rowsAffected := result.RowsAffected()  // 0 = conflict
```

### 3.5 Shard Ownership

Shards partition data across history service instances. Only the owner can write.

**File:** `common/persistence/shard_manager.go`

```go
type ShardInfo struct {
    ShardID           int32
    Owner             string     // Host:Port of owner
    RangeID           int64      // Incremented on ownership change
    StolenSinceRenew  int32      // Detect frequent stealing
    UpdateTime        *time.Time
    // ... queue states, replication info
}
```

**Ownership Verification:**
```go
// Every write includes shard range ID check
batch.Query(templateUpdateLease,
    request.RangeID,
    shardID,
).SerialConsistency(gocql.Serial)

// IF range_id = expectedRangeID
// Fails with ShardOwnershipLostError if mismatch
```

### 3.6 Cassandra Schema

```sql
CREATE TABLE executions (
    shard_id int,
    type int,                              -- Row type discriminator
    namespace_id uuid,
    workflow_id text,
    run_id uuid,
    visibility_ts bigint,
    task_id bigint,

    -- Execution state
    execution blob,
    execution_state blob,
    next_event_id bigint,
    db_record_version bigint,

    -- Pending operation maps
    activity_map map<bigint, blob>,
    timer_map map<text, blob>,
    child_executions_map map<bigint, blob>,
    request_cancel_map map<bigint, blob>,
    signal_map map<bigint, blob>,

    -- Shard metadata (for shard rows)
    range_id bigint,
    shard blob,

    PRIMARY KEY (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
);
```

### 3.7 SQL Schema

```sql
CREATE TABLE executions (
    shard_id INT NOT NULL,
    namespace_id BINARY(16) NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    run_id BINARY(16) NOT NULL,

    data LONGBLOB,
    state LONGBLOB,
    next_event_id BIGINT,
    db_record_version BIGINT,

    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

-- Separate tables for maps (normalized)
CREATE TABLE activity_info_maps (
    shard_id INT,
    namespace_id BINARY(16),
    workflow_id VARCHAR(255),
    run_id BINARY(16),
    activity_id BIGINT,
    data LONGBLOB,
    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, activity_id)
);
```

### 3.8 Serialization

**File:** `common/persistence/serialization/serializer.go`

```go
type DataBlob struct {
    EncodingType enumspb.EncodingType  // PROTO3 or JSON
    Data         []byte
}

// Default: Protocol Buffers (compact binary)
func (s *serializerImpl) serialize(msg proto.Message) (*DataBlob, error) {
    data, err := proto.Marshal(msg)
    return &DataBlob{EncodingType: PROTO3, Data: data}, err
}
```

---

## 4. Signal and Query Handling

### 4.1 Signals Overview

Signals are asynchronous messages sent to running workflows. They:
- Mutate workflow state (create events)
- Are persisted in history
- May trigger a workflow task
- Support idempotency via request ID

### 4.2 Signal Flow

**File:** `service/history/api/signalworkflow/api.go`

```
Client: SignalWorkflowExecution()
    ↓
Frontend: workflow_handler.go:2029
    ↓
History: signalworkflow/api.go:Invoke()
    ├─ Idempotency check: IsSignalRequested(requestID)?
    ├─ Validation: blob size, signal count, workflow state
    ├─ Create event: AddWorkflowExecutionSignaledEvent()
    ├─ Maybe schedule workflow task
    └─ Persist changes
```

**Idempotency:**
```go
// Track signal request IDs in mutable state
if mutableState.IsSignalRequested(request.RequestId) {
    return nil  // Already processed, success
}
mutableState.AddSignalRequested(request.RequestId)
```

### 4.3 Signal Event

**File:** `service/history/historybuilder/event_factory.go:802`

```go
type WorkflowExecutionSignaledEventAttributes struct {
    SignalName                string
    Input                     *Payloads
    Identity                  string
    Header                    *Header
    ExternalWorkflowExecution *WorkflowExecution  // If from another workflow
}
```

### 4.4 SignalWithStart

Atomically signals or starts a workflow.

**File:** `service/history/api/signalwithstartworkflow/signal_with_start_workflow.go`

**Logic:**
```go
func SignalWithStart(ctx, request) {
    // Try to signal existing workflow
    currentMutableState := loadCurrentExecution()

    if currentMutableState != nil && currentMutableState.IsWorkflowExecutionRunning() {
        // Workflow running: just signal
        return SignalWorkflow(ctx, signalRequest)
    }

    // Workflow not running: create new with signal
    return NewWorkflowWithSignal(ctx, startRequest, signalRequest)
}
```

**NewWorkflowWithSignal creates:**
1. `WorkflowExecutionStartedEvent`
2. `WorkflowExecutionSignaledEvent`
3. `WorkflowTaskScheduledEvent`

### 4.5 Signal Buffering

Signals during workflow task backoff are buffered:

```go
// Check if we should create a workflow task
if !mutableState.IsWorkflowPendingOnWorkflowTaskBackoff() {
    // Create workflow task immediately
    mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
} else {
    // Signal buffered in history, task created when backoff expires
}
```

### 4.6 Queries Overview

Queries are synchronous reads of workflow state. They:
- Do NOT mutate state
- Are NOT persisted
- Execute on the worker with current state
- Timeout quickly (no long-running)

### 4.7 Query Flow

**File:** `service/history/api/queryworkflow/api.go`

```
Client: QueryWorkflow()
    ↓
Frontend: workflow_handler.go:2850
    ↓
History: queryworkflow/api.go:Invoke()
    ├─ Validation: reject conditions, workflow state
    │
    ├─ Path A: Direct Dispatch (safe scenarios)
    │   └─ Send to matching → worker executes → response
    │
    └─ Path B: Buffered Query (pending workflow task)
        ├─ Create QueryRegistryEntry with UUID
        ├─ Wait on completion channel
        ├─ Workflow task completion sets result
        └─ Return result
```

### 4.8 Direct vs Buffered Query

**Direct Dispatch (Fast):**
Safe when no pending workflow task that could change state:
- Namespace not active (standby cluster)
- Workflow not running
- No pending/started workflow task

```go
if !nsActive || !mutableState.IsWorkflowExecutionRunning() ||
   (!mutableState.HasPendingWorkflowTask() && !mutableState.HasStartedWorkflowTask()) {
    // Safe to query directly
    return QueryDirectlyThroughMatching(ctx, request)
}
```

**Buffered Query:**
When a workflow task is pending, query must wait:

```go
// Add to query registry
queryID := uuid.New()
registry.AddQuery(queryID, request)

// Wait for completion (or timeout)
select {
case <-queryEntry.CompletionCh:
    return queryEntry.Result
case <-ctx.Done():
    return nil, ctx.Err()
}
```

### 4.9 Query Registry

**File:** `service/history/workflow/query_registry.go`

```go
type QueryRegistry struct {
    buffered  map[string]*QueryEntry  // Waiting for task completion
    completed map[string]*QueryEntry  // Has result
    unblocked map[string]*QueryEntry  // Safe to dispatch directly
    failed    map[string]*QueryEntry  // Query failed
}

type QueryEntry struct {
    ID           string
    Request      *querypb.WorkflowQuery
    CompletionCh chan struct{}
    Result       *querypb.WorkflowQueryResult
    State        QueryState
}
```

**State Transitions:**
```
buffered → completed (task handler set result)
         → unblocked (safe to dispatch directly)
         → failed (error occurred)
```

### 4.10 Signals vs Queries Comparison

| Aspect | Signals | Queries |
|--------|---------|---------|
| State Mutation | Yes | No |
| Persistence | Yes (history events) | No |
| Workflow Task | May create | Never creates |
| Idempotency | Yes (request ID) | No |
| Timeout | Long (workflow timeout) | Short (query timeout) |
| Buffering | In history if backoff | In registry if task pending |
| Response | Ack only | Actual result |

### 4.11 External Signals

Workflows can signal other workflows:

**File:** `service/history/workflow/mutable_state_impl.go`

```go
// Initiator workflow creates pending signal
AddSignalInfo(signalInfo *SignalInfo) {
    ms.pendingSignalInfoIDs[initiatedEventID] = signalInfo
}

// History service sends signal to target
// On completion, add SignalExternalWorkflowExecutionCompletedEvent
```

---

## Key Architectural Patterns

### Event Sourcing
All state changes recorded as immutable events. State reconstructible via replay.

### Optimistic Locking
Version-based conflict detection. No locks held during processing.

### Sharding
Data partitioned by shard ID. Each shard owned by one history instance.

### Task Queue Abstraction
Decouples task production (history) from consumption (workers) via matching service.

### Consistency via CAS
Atomic updates via Compare-And-Swap operations in database.

### Lease Pattern
Exclusive access via lock acquisition before state modification.
