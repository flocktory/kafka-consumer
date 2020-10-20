(ns com.flocktory.protocol.tracer)

(defprotocol ITracerName
  (tracer-name [this]))

(defprotocol IOnConsumerStart
  (on-consumer-start [this group-id opts]))

(defprotocol IOnConsumerStop
  (on-consumer-stop [this group-id opts]))

(defprotocol IOnConsumerFail
  (on-consumer-fail [this group-id opts error]))

(defprotocol IOnConsumerFailLoop
  (on-consumer-fail-loop [this group-id opts failed-ago]))

(defprotocol IOnConsumerIncError
  (on-consumer-inc-error [this group-id opts error]))

(defprotocol IOnConsumerFailFast
  (on-consumer-fail-fast [this group-id opts]))

(defprotocol IBeforePoll
  (before-poll [this group-id opts]))

(defprotocol IAfterPoll
  (after-poll [this group-id opts records-count]))

(defprotocol IBeforeConsume
  (before-consume [this group-id opts records-count]))

(defprotocol IAfterConsume
  (after-consume [this group-id opts records-count]))

(defprotocol IOnConsumeError
  (on-consume-error [this group-id opts exception records-count]))

(defprotocol IBeforeConsumePartition
  (before-consume-partition [this group-id opts topic-partition records-count]))

(defprotocol IAfterConsumePartition
  (after-consume-partition [this group-id opts topic-partition records-count]))

(defprotocol IOnConsumePartitionError
  (on-consume-partition-error [this group-id opts topic-partition records-count exception]))

(defprotocol IBeforeConsumeRecord
  (before-consume-record [this group-id opts record]))

(defprotocol IAfterConsumeRecord
  (after-consume-record [this group-id opts record]))

(defprotocol IOnConsumeRecordError
  (on-consume-record-error [this group-id opts record exception]))

(defprotocol IBeforeCommit
  (before-commit [this group-id opts offsets]))

(defprotocol IAfterCommit
  (after-commit [this group-id opts offsets]))

(defprotocol IOnPartitionsAssigned
  (on-partitions-assigned [this group-id opts topic-partitions]))

(defprotocol IOnPartitionsRevoked
  (on-partitions-revoked [this group-id opts topic-partitions]))

(defprotocol IBeforePartitionsPaused
  (before-partitions-paused [this group-id opts topic-partitions]))

(defprotocol IAfterPartitionsPaused
  (after-partitions-paused [this group-id opts topic-partitions]))

(defprotocol IBeforePartitionsResumed
  (before-partitions-resumed [this group-id opts topic-partitions]))

(defprotocol IAfterPartitionsResumed
  (after-partitions-resumed [this group-id opts topic-partitions]))

(defprotocol IBeforeEndOffsets
  (before-end-offsets [this group-id opts]))

(defprotocol IAfterEndOffsets
  (after-end-offsets [this group-id opts offsets]))

(defprotocol IBeforeBeginningOffsets
  (before-beginning-offsets [this group-id opts]))

(defprotocol IAfterBeginningOffsets
  (after-beginning-offsets [this group-id opts offsets]))

(defprotocol ICurrentOffsets
  (current-offsets [this group-id opts offsets]))
