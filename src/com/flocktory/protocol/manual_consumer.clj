(ns com.flocktory.protocol.manual-consumer)

(defprotocol IPartitionConsumer
  (consume-partition [this topic-partition records]))
