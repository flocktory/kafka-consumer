(ns com.flocktory.protocol.consumer)

(defprotocol IRecordConsumer
  (consume-record [this record]))

(defprotocol IPartitionConsumer
  (consume-partition [this topic-partition records]))

(defprotocol IConsumer
  (consume [this records]))
