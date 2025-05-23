# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [0.1.7] - 2025-05-14

### Features

- Add batch processing utility functions optimized for batch insertions

### Fixes

### Breaking

## [0.1.6] - 2025-05-09

### Features

- Refill connection pool by full deficit

### Fixes

### Breaking

## [0.1.5] - 2025-05-09

### Features

- Add utility function to get refill the pool with new connections `refill_connection_pool`
- Add exponential backoff for `get_connection()` method 

### Fixes

### Breaking

## [0.1.4] - 2025-05-09

### Features

- Add utility function to get the internal pool `get_pool`

### Fixes

### Breaking

## [0.1.3] - 2025-05-09

### Features

- Add utility function to calculate seconds since last pool recycle via `seconds_since_last_recycle`

### Fixes

### Breaking

## [0.1.2] - 2025-05-09

### Features

- Add periodic connection recycler to `PoolManager` using `recycle_idle_connections`

### Fixes

### Breaking

- Change name from `build_query()` to `build_select_query()`

## [0.1.1] - 2025-04-10

### Features

- Add trait Model for consistent functionality across all model objects.
- Add execute_select_with_retry for select queries with customizable where clauses, limits, and offset
- Update example for the same

### Fixes

### Breaking

## [0.1.0] - 2025-03-27

### Features

- Add ClickhouseConnectionPool
- Add configurable connection pool using ClickhouseConfig
- Add configurable retry mechanism using RetryConfig
- Add PoolManager to manage connection pools and retry using exponential backoff
- Add examples/simple-clickhouse
- Add configurable metrics tracking using Prometheus

### Fixes

### Breaking