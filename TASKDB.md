# TaskDBs <!-- omit in toc -->

## Table of Contents <!-- omit in toc -->

- [What is a TaskDB?](#what-is-a-taskdb)
- [TaskDB recommendations](#taskdb-recommendations)
- [Available TaskDBs](#available-taskdbs)
  - [Memory](#memory)
  - [DiskKV](#diskkv)
  - [CockroachDB](#cockroachdb)
  - [Cassandra/ScyllaDB](#cassandrascylladb)

## What is a TaskDB?

A TaskDB is the durability/persistence layer for UltraQueue. It stores the payload out of memory to reduce the required operating memory for the queue.

It is also fundamental in providing failover capabilities in disaster scenarios.

You may choose the TaskDB that best suits your performance, durability, and availability requirements

## TaskDB recommendations

**If you are going for Global capabilities, use CockroachDB or Cassandra/ScyllaDB.**

This will not only provide the greatest durability, but multi-region clusters can survive entire regional outages, allowing you to move resources to another region and pickup where the down partitions left off.

Between the two, it is best to use the DB that you are already using if one.

If you are not already using one of them, then you must evaluate your requirements. For raw insert performance ScyllaDB with a consistency of `ONE` will perform the best, but failover will be painfully slow with a required `ALL` consistency. A balance would be to use `QUORUM` or `LOCAL_QUORUM` depending on your regional survivability goals.

CockroachDB requires no tuning, and will have lower insert performance, but much greater failover recovery speed. CockroachDB is also far easier to maintain and scale, but per-resource performance will be lower than ScyllaDB.

If consistency is your greatest requirement, CockroachDB will suit you best.

If you can either tolerate lower consistency, slower failover times (to maintain consistency), or need to optimize for maximum insert performance, Cassandra/ScyllaDB will suit you best.

## Available TaskDBs

### Memory

An in-memory index for the payload data, ignores state transitions since they are lost during failover anyway.

Payload size will greatly impact queue size as the payloads are kept in memory.


| Performance | Durability | Failover |
| ----------- | ----------- | --- |
| Highest      | None | None |

### DiskKV

Uses Badger, a high performance disk-based KV store. Very high performance, but maintaining access to the same disk is required to survive failovers. Kubernetes StatefulSets help with node loses, but AZ or region loses may cause unavailability of data.

Use DiskKV if you are optimizing for speed and have no multi-AZ or multi-region needs, while still requiring durability of data.

i.e. If you are using a provider like DigitalOcean, or a single-AZ GKE cluster, this is the best option.

| Performance | Durability | Failover |
| ----------- | ----------- | --- |
| Very High      | Yes | If the same disk can be attached to. If losing a region (or an AZ) you may not have access to the data. |

### CockroachDB


| Performance | Durability | Failover |
| ----------- | ----------- | --- |
| Medium      | Highest | Global |

### Cassandra/ScyllaDB


| Performance | Durability | Failover |
| ----------- | ----------- | --- |
| Medium-High (tunable consistency) | High-Highest (tunable consistency) | Global, performance based on chosen consistency level |
