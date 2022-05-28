# UltraQueue

[TaskDBs](TASKDB.md)


## Why wait for write commits after local enqueue?

At this state we know that the payload has been stored, so the payload is durable. The item is also in the local queue, so it is eligible to be dequeued. At that time a new state will be written.

The only disaster scenario is that the partition goes down before it has written it's first state, but has written the payload. This is highly unlikely for 2 reasons:

1. The put payload and put state operations are very likely to be in the same batch (if batching), so the whole batch should fail
2. If we've just recently successfully written one batch, the next batch is very likely to be written as well.

In the extremely unlikely worse case, a payload could be orphaned in the TaskDB. The best way to solve this is to rotate partitions. That is, to not have them running for more than X days. The draining process should wipe all data from TaskDBs, including orphaned data.

The probability of this case is similar to many other distributed systems where you might see something committed, but it actually is not.

In our case, we won't consider it committed so the publisher will publish again. Again the worst case here is an extra orphaned payload in the TaskDB, no data is lost.

Maybe TaskDBs also have TTL features that could be set for VERY long durations, like many days or months.
