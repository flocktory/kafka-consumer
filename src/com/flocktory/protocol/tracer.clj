(ns com.flocktory.protocol.tracer)

(defprotocol ITracerName
  (tracer-name [this]))

(defprotocol IOnConsumerStart
  (on-consumer-start [this group-id]))

(defprotocol IOnConsumerStop
  (on-consumer-stop [this group-id]))

(defprotocol IOnConsumerFail
  (on-consumer-fail [this group-id error]))

(defprotocol IOnConsumerFailLoop
  (on-consumer-fail-loop [this group-id failed-ago]))

(defprotocol IBeforePoll
  (before-poll [this group-id]))

(defprotocol IAfterPoll
  (after-poll [this group-id records-count]))

(defprotocol IBeforeConsume
  (before-consume [this group-id records-count]))

(defprotocol IAfterConsume
  (after-consume [this group-id records-count]))

(defprotocol IOnConsumeError
  (on-consume-error [this group-id exception records-count]))

(defprotocol IBeforeConsumePartition
  (before-consume-partition [this group-id topic-partition records-count]))

(defprotocol IAfterConsumePartition
  (after-consume-partition [this group-id topic-partition records-count]))

(defprotocol IOnConsumePartitionError
  (on-consume-partition-error [this group-id topic-partition records-count exception]))

(defprotocol IBeforeConsumeRecord
  (before-consume-record [this group-id record]))

(defprotocol IAfterConsumeRecord
  (after-consume-record [this group-id record]))

(defprotocol IOnConsumeRecordError
  (on-consume-record-error [this group-id record exception]))

(defprotocol IBeforeCommit
  (before-commit [this group-id offsets]))

(defprotocol IAfterCommit
  (after-commit [this group-id offsets]))

(defprotocol IOnPartitionsAssigned
  (on-partitions-assigned [this group-id topic-partitions]))

(defprotocol IOnPartitionsRevoked
  (on-partitions-revoked [this group-id topic-partitions]))

(defprotocol IBeforePartitionsPaused
  (before-partitions-paused [this group-id topic-partitions]))

(defprotocol IAfterPartitionsPaused
  (after-partitions-paused [this group-id topic-partitions]))

(defprotocol IBeforePartitionsResumed
  (before-partitions-resumed [this group-id topic-partitions]))

(defprotocol IAfterPartitionsResumed
  (after-partitions-resumed [this group-id topic-partitions]))

(defprotocol IBeforeEndOffsets
  (before-end-offsets [this group-id]))

(defprotocol IAfterEndOffsets
  (after-end-offsets [this group-id offsets]))

(defprotocol IBeforeBeginningOffsets
  (before-beginning-offsets [this group-id]))

(defprotocol IAfterBeginningOffsets
  (after-beginning-offsets [this group-id offsets]))

(defprotocol ICurrentOffsets
  (current-offsets [this group-id offsets]))
