(ns com.flocktory.protocol.config
  (:import (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.common.serialization StringDeserializer)))

(defprotocol IKafkaConsumerConfig
  (kafka-consumer-config [this]
    "Returns map with any keys from org.apache.kafka.clients.consumer.ConsumerConfig.
    ConsumerConfig/GROUP_ID_CONFIG key is required.

    Example: {ConsumerConfig/GROUP_ID_CONFIG \"some-group-id\"
              ConsumerConfig/AUTO_OFFSET_RESET_CONFIG \"latest\"}

    Or strings as keys: {\"group.id\" \"some-group-id\"
                         \"max.poll.records\" 100}

    See defaults in KAFKA_CONSUMER_CONFIG_DEFAULTS."))

(defprotocol ITopics
  (topics [this]
    "List of topics (one or more) to subscribe.

    Example: [\"topic1\" \"topic2\"]"))

(def KAFKA_CONSUMER_CONFIG_DEFAULTS
  {ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false"
   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "none"
   ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG (* 100 1000)
   ConsumerConfig/HEARTBEAT_INTERVAL_MS_CONFIG (* 30 1000)
   ConsumerConfig/MAX_POLL_INTERVAL_MS_CONFIG (* 30 60 1000)
   ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG (+ (* 30 60 1000) (* 5 1000))})

(defprotocol IOptionalConfig
  "Optional configuration protocol"
  (optional-config [this]
    "Returns map with any of keys:

    :value-format - available formats to decode record value from (:json), no decoding if nil
    :poll-timeout-ms - kafka poll timeout
    :min-commit-interval-ms - minimal interval between commits
    :fail-fast? - use fail-fast mode (shutdown (system/exit 1) when kafka poll errors)
    :fail-loop? - use fail-loop mode (eternal loop without processing messages)
    :fail-budget - max errors count (only for fail-fast mode)

    Example: {:poll-timeout-ms 1000
              :value-format nil}

    See defaults in OPTIONAL_CONFIG_DEFAULTS"))

(def OPTIONAL_CONFIG_DEFAULTS
  {:poll-timeout-ms 5000
   :min-commit-interval-ms 5000
   :value-format :json
   :key-format nil
   :fail-fast? true
   :fail-budget 3
   :fail-loop? false
   :fail-loop-sleep-ms 5000})
