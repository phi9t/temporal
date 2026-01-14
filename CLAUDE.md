# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Temporal is a durable execution platform that enables developers to build scalable, resilient applications. The server executes Workflows in a fault-tolerant manner using event sourcing - each workflow maintains an append-only history of events that can be replayed to reconstruct state.

## Common Commands

```bash
# Build
make                    # Full build + tests (comprehensive, first-time setup)
make bins               # Build binaries only (temporal-server, tools)
make proto              # Regenerate protobuf files

# Testing
make unit-test          # Run unit tests (fastest feedback)
make integration-test   # Run integration tests (requires dependencies)
make functional-test    # Run E2E functional tests

# Single test
go test -v <path> -run <TestSuite> -testify.m <TestName>
# Example:
go test -v github.com/temporalio/temporal/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow

# Linting (REQUIRED before commits)
make lint-code          # Code linting with golangci-lint
make fmt-imports        # Format imports

# Running server
make start              # Default: SQLite in-memory
make start-sqlite-file  # SQLite with file persistence
make start-postgres12   # PostgreSQL (requires make install-schema-postgresql first)
make start-mysql8       # MySQL (requires make install-schema-mysql first)
make start-cass-es      # Cassandra + ES (requires make install-schema-cass-es first)

# Dependencies
make start-dependencies # Start Docker services (Cassandra, ES, Postgres, MySQL)
make stop-dependencies  # Stop Docker services

# Code generation
make update-go-api      # Update go.temporal.io/api to latest
go generate ./...       # Run go:generate directives
```

## Architecture

### Four Core Services

1. **Frontend Service** (`/service/frontend`) - Entry point for client applications. Handles workflow creation, queries, cancellations, rate limiting, and authorization.

2. **History Service** (`/service/history`) - Core state management. Manages workflow execution lifecycle, processes tasks, maintains event history, handles multi-cluster replication.

3. **Matching Service** (`/service/matching`) - Task queue management. Distributes workflow and activity tasks to workers, handles task stickiness and worker affinity.

4. **Worker Service** (`/service/worker`) - Executes Temporal's internal system workflows and background maintenance tasks.

### Key Directories

- `/api/` - Generated gRPC protobuf files from external api repository
- `/chasm/` - CHASM library (Coordinated Heterogeneous App State Machines) - new component-based workflow execution model
- `/client/` - Client libraries for inter-service communication (RPC wrappers)
- `/cmd/server/` - Main temporal-server binary entry point
- `/common/` - Shared packages (~78 directories): persistence, metrics, authorization, config, etc.
- `/common/persistence/` - Database abstraction layer with implementations for Cassandra, PostgreSQL, MySQL, SQLite
- `/components/` - Optional service components (callbacks, nexusoperations)
- `/config/` - YAML configuration files for different environments
- `/proto/internal/` - Internal protobuf definitions (not exposed externally)
- `/schema/` - Database DDL scripts for all supported backends
- `/service/` - Main service implementations
- `/tests/` - Functional/E2E tests including XDC (cross-datacenter) and NDC (multi-cluster) tests

### Design Patterns

- **Dependency Injection**: Uses `go.uber.org/fx` throughout. See `/temporal/fx.go` for main DI module.
- **Event Sourcing**: All workflow state changes recorded as immutable events.
- **Persistence Abstraction**: `/common/persistence/` provides interfaces with multiple backend implementations.

## Testing Guidelines

### Build Tags
- `test_dep` - Enables test hooks (required for some tests)
- `disable_grpc_modules` - Faster compilation for unit tests
- `TEMPORAL_DEBUG` - Extends functional test timeouts for debugging

### IDE Debugging (GoLand)
- Run Type: package
- Package path: `go.temporal.io/server/cmd/server`
- Program arguments: `--env development-postgres12 --allow-no-auth start`
- Go tool arguments: `-tags disable_grpc_modules,test_dep`

### Test Utilities
- `testvars` package - Generates consistent test variables (namespace, task queue, workflow ID, etc.)
- `taskpoller` package - Task polling utilities for E2E tests
- Prefer `require` over `assert` from testify
- Use `require.Eventually` instead of `time.Sleep`

## Code Conventions

- Mimic existing style, structure, and architectural patterns
- Comments should describe WHY, not WHAT
- Handle all errors explicitly - never ignore
- Use `logger.Fatal()` for core invariant violations
- Use `logger.DPanic()` for important non-fatal issues
- Leave `CONSIDER(name):` comments for future design improvements
- Do not introduce new third-party libraries unless specifically requested
- Run `make lint-code` before committing

## Working with Proto Changes

For local API changes:
1. Clone `api`, `api-go`, and optionally `sdk-go` repos
2. Make changes to `api` proto files
3. In `api-go`: point submodule at your branch, run `make proto`
4. In this repo, add to `go.mod`:
   ```
   replace (
       go.temporal.io/api => ../api-go
       go.temporal.io/sdk => ../sdk-go
   )
   ```
5. Run `make proto && make bins`

For merged API changes: `make update-go-api`
