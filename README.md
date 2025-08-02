<p align="center">
    <img src="https://github.com/NEMOlv/MechaKV/blob/master/logo/MechaKV_LOGO.png" width = "70%" height = "70%">
</p>

<div class="column" align="middle">
  <a><img src="https://img.shields.io/badge/MechaKV-beta-orange" /></a>
  <a><img src="https://img.shields.io/badge/go-v1.24.1-brightgreen" /></a>
  <a><img src="https://img.shields.io/badge/build-passing-brightgreen" /></a>
  <a><img src="https://img.shields.io/github/license/NEMOlv/MechaKV?color=blue" /></a>
</div>

# MechaKV
A simple, high-performance, and easily expandable key-value database implemented in Go.

## Overview

MechaKV is a lightweight key-value storage engine designed for simplicity and efficiency. It provides core KV operations with support for transactions, data persistence, and background compaction to optimize storage usage. The database is built with a focus on ease of use, reliability, and performance.

## Features

- **Basic KV Operations**: Supports `Put`, `Get`, and `Delete` operations with key-value pairs.
- **Advanced KV Operations**: Supports `BatchPut`, `BatchGet`, `BatchDelete`, `PUT_IF_NOT_EXISTS`, `PUT_IF_EXISTS`, `PUT_AND_RETURN_OLD_VALUE`, `APPEND_VALUE` and `UPDATE_TTL` operations with key-value pairs.
- **Transaction Support**: Handles atomic transactions to ensure data consistency. Currently only supports serialization of the highest level transactions. In the future, this project plans to add Repeatable Read(RR) level transaction.
- **Efficient Indexing**: Uses B-tree (via `github.com/google/btree`) for fast key lookup.
- **Data Persistence**: Stores data in disk files with CRC32 verification to ensure data integrity.
- **Background Compaction**: Automatically merges old data files to reclaim space from deleted or overwritten entries.
- Key Expiry Mechanism:
  - TTL Support: Allows setting time-to-live (TTL) for key-value pairs, with permanent storage as the default.
  - Passive Expiry Check: When retrieving a key via Get, the system checks if the key has expired using the IsExpired function, which compares the key's expiry time (timestamp + TTL) with the current time. Expired keys are not returned and are removed from the index.
  - Active Expiry Monitor: A background monitor (started via StartExpiryMonitor) periodically (every 100ms) selects random keys (up to 20 per check) from key slots, checks their expiry status, and deletes expired keys from the index to reclaim space proactively.

## Installation

```bash
go get github.com/NEMOlv/MechaKV
```

## Quick Start

The following examples demonstrate how to perform basic `Put`, `Get`, and `Delete` operations using the `Client` interface. All core functionalities must be invoked through the client interface:

### Normal Operation Example
```go
package main

import (
    "fmt"
    "MechaKV/client"
    "MechaKV/database"
)

func main() {
    // 1. Initialize database configuration
    opts := database.DefaultOptions
    opts.DirPath = "./mechakv_data"  // Directory for storing data files
    opts.DataFileMergeRatio = 0.6    // Optional: adjust merge trigger threshold

    // 2. Open database instance
    db, err := database.Open(opts)
    if err != nil {
        panic(fmt.Sprintf("Failed to open database: %v", err))
    }
    defer db.Close()  // Close the database at the end

    // 3. Initialize client (all operations are performed via the client)
    cli, err := client.OpenClient(db)
    if err != nil {
        panic(fmt.Sprintf("Failed to open client: %v", err))
    }
    defer cli.Close()  // Close the client before closing the database

    // 4. Perform Put operation (insert key-value pair)
    key := []byte("username")
    value := []byte("mechakv_user")
    err = cli.Put(key, value)
    if err != nil {
        panic(fmt.Sprintf("Put failed: %v", err))
    }
    fmt.Println("Put success: key=", string(key), ", value=", string(value))

    // 5. Perform Get operation (query value)
    getValue, err := cli.Get(key)
    if err != nil {
        panic(fmt.Sprintf("Get failed: %v", err))
    }
    fmt.Println("Get success: value=", string(getValue))

    // 6. Perform Delete operation (delete key)
    err = cli.Delete(key)
    if err != nil {
        panic(fmt.Sprintf("Delete failed: %v", err))
    }
    fmt.Println("Delete success: key=", string(key))

    // Verify deletion result
    deletedValue, err := cli.Get(key)
    if err != nil {
        fmt.Println("Key already deleted:", err)  // Expected: Key not found
    } else {
        fmt.Println("Unexpected value after delete:", string(deletedValue))
    }
}
```

### Expiration Time Example

Set the expiration time (in seconds) for a key using the `WithTTL` option:

```go
// Insert a key-value pair that expires in 60 seconds
err = cli.Put(
    []byte("temp_key"),
    []byte("temporary_value"),
    client.WithTTL(60),  // Set TTL to 60 seconds
)
if err != nil {
    panic(fmt.Sprintf("Put with TTL failed: %v", err))
}

// Check the expiration status of the key (Get will return KeyNotFound error after expiration)
```

### Batch Operation Example

Perform batch insertion and query via the client:

```go
// Batch insertion
keys := [][]byte{
    []byte("key1"),
    []byte("key2"),
    []byte("key3"),
}
values := [][]byte{
    []byte("value1"),
    []byte("value2"),
    []byte("value3"),
}
err = cli.BatchPut(keys, values)
if err != nil {
    panic(fmt.Sprintf("BatchPut failed: %v", err))
}

// Batch query (call Get in a loop)
for _, k := range keys {
    val, _ := cli.Get(k)
    fmt.Printf("BatchGet: key=%s, value=%s\n", k, val)
}
```

### Transaction Example

This example demonstrates how to use transactions in MechaKV, including creating transactions, performing operations, committing, and rolling back. Transactions support both automatic and manual commit modes, with configurable options like timeouts and batch limits.

### Manual-Commit Transaction
This example shows a read-write transaction with manual commit/rollback:

```go
package main

import (
    "fmt"
    "time"
    "MechaKV/client"
    "MechaKV/database"
    "MechaKV/transaction"
    . "MechaKV/comment"
)

func main() {
    // 1. Initialize database
    opts := database.DefaultOptions
    opts.DirPath = "./mechakv_tx_data"
    db, err := database.Open(opts)
    if err != nil {
        panic(fmt.Sprintf("Failed to open database: %v", err))
    }
    defer db.Close()

    // 2. Create transaction manager
    tm := transaction.NewTransactionManager(db)
    defer tm.Close()

    // 3. Configure transaction options (30s timeout, max 1000 operations)
    txOpts, err := transaction.NewTxOptions(
        transaction.WithTimeout(30*time.Second),
        transaction.WithMaxBatchNum(1000),
        transaction.WithSyncWrites(true), // Sync to disk on commit
    )
    if err != nil {
        panic(fmt.Sprintf("Invalid transaction options: %v", err))
    }

    // 4. Begin a read-write transaction (manual commit)
    tx, err := tm.Begin(true, false, txOpts) // isWrite=true, isAutoCommit=false
    if err != nil {
        panic(fmt.Sprintf("Failed to begin transaction: %v", err))
    }
    defer func() {
        // Rollback if not committed
        if tx.IsRunning() {
            if err := tm.Rollback(tx); err != nil {
                fmt.Printf("Rollback failed: %v\n", err)
            }
        }
    }()

    // 5. Perform operations within the transaction
    key := []byte("user:100")
    value := []byte("alice")

    // Put with normal condition
    _, err = tx.Put(key, value, PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
    if err != nil {
        panic(fmt.Sprintf("Put failed: %v", err))
    }

    // Get the value (visible within the transaction)
    getVal, err := tx.Get(key)
    if err != nil {
        panic(fmt.Sprintf("Get failed: %v", err))
    }
    fmt.Printf("Transaction Get: %s -> %s\n", key, getVal)

    // 6. Commit the transaction
    if err := tm.Commit(tx); err != nil {
        panic(fmt.Sprintf("Commit failed: %v", err))
    }
    fmt.Println("Transaction committed successfully")
}
```


### Auto-Commit Transaction
Transactions can be configured to auto-commit after operations:

```go
// Begin an auto-commit read-write transaction
tx, err := tm.Begin(true, true, txOpts) // isAutoCommit=true
if err != nil {
    panic(fmt.Sprintf("Failed to begin auto-commit tx: %v", err))
}

// Operation will auto-commit immediately
_, err = tx.Put([]byte("auto:key"), []byte("auto:val"), PERSISTENT, 0, PUT_NORMAL)
if err != nil {
    panic(fmt.Sprintf("Auto-commit put failed: %v", err))
}
// No need to call Commit() manually
```


### Conditional Operations in Transactions
Use `PutCondition` for conditional writes (e.g., put if not exists):

```go
tx, err := tm.Begin(true, false, txOpts)
if err != nil { /* handle error */ }
defer tm.Rollback(tx)

key := []byte("conditional:key")

// Put only if key does NOT exist
_, err = tx.Put(key, []byte("new_val"), PERSISTENT, 0, PUT_IF_NOT_EXISTS)
if err != nil {
    fmt.Printf("Conditional put failed: %v\n", err)
}

// Put only if key exists
_, err = tx.Put(key, []byte("updated_val"), PERSISTENT, 0, PUT_IF_EXISTS)
if err != nil {
    fmt.Printf("Put if exists failed: %v\n", err)
}

// Update TTL (time-to-live)
_, err = tx.Put(key, nil, 3600, 0, UPDATE_TTL) // 3600s expiration
if err != nil { /* handle error */ }

err = tm.Commit(tx)
```

### Key Notes:
- **Transaction Modes**: Use `isWrite=true` for write transactions (acquires write lock) and `isWrite=false` for read-only transactions (acquires read lock).
- **Auto-Commit**: When `isAutoCommit=true`, operations commit immediately; use `false` for multi-operation transactions.
- **Timeout**: Configure with `WithTimeout()` to auto-rollback long-running transactions.
- **Atomicity**: All operations in a transaction either commit together or rollback entirely.
- **Isolation**: Transactions see their own uncommitted changes (read-your-own-writes) but not changes from other concurrent transactions.

The above examples demonstrate the complete workflow for core operations using the `Client` interface. All data interactions must go through the client interface to ensure transaction consistency and operational standardization.

## License

MechaKV is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Uses `github.com/google/btree` for B-tree implementation.
- Uses `github.com/stretchr/testify` for unit testing.
- Inspired by bitcask model, RoesDB, NutsDB and other log-structured storage engines.
