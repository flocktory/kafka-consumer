(ns com.flocktory.kafka-consumer
  (:require [com.flocktory.protocol.tracer :as tracer]
            [com.flocktory.protocol.consumer :as consumer-protocol]
            [com.flocktory.protocol.manual-consumer :as manual-consumer-protocol]
            [com.flocktory.protocol.config :as config-protocol]
            [clojure.tools.logging :as log]
            [com.climate.claypoole :as cp]
            [clojure.set :as set]
            [cheshire.core :as json]
            [clojure.string :as str])
  (:import (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer OffsetAndMetadata ConsumerRebalanceListener ConsumerRecord OffsetAndTimestamp ConsumerRecords)
           (org.apache.kafka.common TopicPartition PartitionInfo)
           (java.util.concurrent Executors ExecutorService TimeUnit)
           (org.apache.kafka.common.errors WakeupException)
           (com.fasterxml.jackson.core JsonParseException)))

(defn- group-id
  [consumer]
  (get (config-protocol/kafka-consumer-config consumer)
       ConsumerConfig/GROUP_ID_CONFIG))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; MAP UTILS ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn map-keys
  [f m]
  (persistent!
    (reduce (fn [res e]
              (assoc! res (f (key e)) (val e)))
            (transient {}) m)))

(defn map-vals
  [f m]
  (persistent!
    (reduce (fn [res e]
              (assoc! res (key e) (f (val e))))
            (transient {}) m)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; TRACER UTILS ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- filter-by-protocol
  [protocol consumer]
  (filter #(satisfies? protocol %) (vals consumer)))

(defn- notify-tracers [protocol protocol-fn consumer & args]
  (let [tracers (filter-by-protocol protocol consumer)]
    (if (seq tracers)
      (doseq [tracer tracers]
        (apply protocol-fn tracer (::group-id consumer) args))
      (log/trace "No tracers to notify"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; CONSUMER STATE UTILS ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update-current-offsets!
  [consumer new-offsets]
  (swap! (::current-offsets consumer) merge new-offsets))

(defn- reset-offsets!
  [consumer offsets]
  (reset! (::current-offsets consumer) offsets))

(defn- reset-last-commit-timestamp!
  [consumer timestamp]
  (reset! (::last-commit-timestamp consumer) timestamp))

(defn- reset-paused-partitions!
  [consumer paused-partitions]
  (reset! (::paused-partitions consumer) paused-partitions))

(defn- reset-assigned-partitions!
  [consumer assigned-partitions]
  (reset! (::assigned-partitions consumer) assigned-partitions))

(defn- reset-pending-records!
  [consumer pending-records]
  (reset! (::pending-records consumer) pending-records))

(defn update-consumer-state!
  [consumer offsets paused-partitions pending-records]
  (doto consumer
    (update-current-offsets! offsets)
    (reset-paused-partitions! paused-partitions)
    (reset-pending-records! pending-records)))

(defn- init-consumer-state!
  [consumer]
  (doto consumer
    (reset-offsets! {})
    (reset-pending-records! [])
    (reset-paused-partitions! #{})))

(defn- json-decode-strict [s] (json/decode-strict s true))

(defn make-transform-record-value-fn
  [consumer]
  (let [value-format (get-in consumer [::optional-config :value-format])]
    (case value-format
      :json (fn [record] (update record :value json-decode-strict))
      identity)))

(defn make-transform-record-key-fn
  [consumer]
  (let [value-format (get-in consumer [::optional-config :key-format])]
    (case value-format
      :json (fn [record] (update record :key json-decode-strict))
      identity)))

(defn make-transform-record-fn
  [consumer]
  ;;todo: validate keys in consumer
  (comp (make-transform-record-key-fn consumer)
        (make-transform-record-value-fn consumer)))

(defn- partition-process-result
  [status {:keys [topic partition offset timestamp]} & [pending-records]]
  (let [result {:status status
                :timestamp timestamp
                :topic-partition {:topic topic
                                  :partition partition}
                :offset (if status (inc offset) offset)}]
    (if pending-records
      (assoc result :pending-records pending-records)
      result)))

(def partition-process-success (partial partition-process-result true))

(defn partition-process-fail
  [records]
  (partition-process-result false (first records) records))

(defn ConsumerRecord->map
  [^ConsumerRecord record]
  {:topic (.topic record)
   :partition (.partition record)
   :offset (.offset record)
   :key (.key record)
   :value (.value record)
   :timestamp (.timestamp record)})

(defn TopicPartition->map
  [^TopicPartition topic-partition]
  {:topic (.topic topic-partition)
   :partition (.partition topic-partition)})

(defn- map->TopicPartition
  [{:keys [topic partition]}]
  (TopicPartition. topic partition))

(defn kafka-offsets
  [offsets]
  (reduce-kv
    (fn [out topic-partition offset]
      (assoc out (map->TopicPartition topic-partition)
                 (OffsetAndMetadata. offset))) {} offsets))

(defn- end-offsets!
  [{:keys [::kafka-consumer] :as consumer} topic-partitions]
  (when (seq (filter-by-protocol tracer/IAfterEndOffsets consumer))
    (notify-tracers tracer/IBeforeEndOffsets
                    tracer/before-end-offsets
                    consumer)
    (->> (.endOffsets kafka-consumer topic-partitions)
         (map-keys TopicPartition->map)
         (notify-tracers tracer/IAfterEndOffsets
                         tracer/after-end-offsets
                         consumer))))

(defn- beginning-offsets!
  [{:keys [::kafka-consumer] :as consumer} topic-partitions]
  (when (seq (filter-by-protocol tracer/IAfterBeginningOffsets consumer))
    (notify-tracers tracer/IBeforeBeginningOffsets
                    tracer/before-beginning-offsets
                    consumer)
    (->> (.beginningOffsets kafka-consumer topic-partitions)
         (map-keys TopicPartition->map)
         (notify-tracers tracer/IAfterBeginningOffsets
                         tracer/after-beginning-offsets
                         consumer))))

(defn- write-lag-metrics!
  ;;todo: write doc string
  [{:keys [::current-offsets ::assigned-partitions] :as consumer} & [topic-partitions]]
  (notify-tracers tracer/ICurrentOffsets
                  tracer/current-offsets
                  consumer @current-offsets)
  (let [topic-partitions (or topic-partitions
                             (mapv map->TopicPartition @assigned-partitions))]
    (when (seq topic-partitions)
      (end-offsets! consumer topic-partitions)
      (beginning-offsets! consumer topic-partitions))))

(defn commit-sync!
  [{:keys [::kafka-consumer ::current-offsets] :as consumer}]
  (let [offsets @current-offsets]
    (if (seq offsets)
      (do
        (notify-tracers tracer/IBeforeCommit
                        tracer/before-commit
                        consumer offsets)
        (.commitSync kafka-consumer (kafka-offsets offsets))
        (notify-tracers tracer/IAfterCommit
                        tracer/after-commit
                        consumer offsets)
        (reset-last-commit-timestamp! consumer (System/currentTimeMillis)))
      (log/tracef "[%s] nothing to commit" (::group-id consumer))))
  (write-lag-metrics! consumer))

(defn should-commit?
  [{:keys [::last-commit-timestamp ::optional-config]}]
  (or (nil? @last-commit-timestamp)
      (> (- (System/currentTimeMillis) @last-commit-timestamp)
         (:min-commit-interval-ms optional-config))))

(defn- maybe-commit!
  [consumer]
  (if (should-commit? consumer)
    (commit-sync! consumer)
    (log/tracef "[%s] skip commit" (::group-id consumer))))

(defn- poll
  ;;todo fix doc string
  "Call kafka poll with specified poll-timeout-ms.
  Transforms ConsumerRecord to clojure maps with (optional) value decoding."
  [{:keys [::kafka-consumer
           ::transform-record-fn
           ::pending-records
           ::optional-config] :as consumer}]
  (notify-tracers tracer/IBeforePoll
                  tracer/before-poll
                  consumer)
  (let [^ConsumerRecords records (.poll kafka-consumer (:poll-timeout-ms optional-config))]
    (notify-tracers tracer/IAfterPoll
                    tracer/after-poll
                    consumer (.count records))
    (->> records
         (map (comp transform-record-fn ConsumerRecord->map))
         (concat @pending-records))))

(defn get-new-offsets
  [results]
  (reduce (fn [m {:keys [topic-partition offset]}]
            (assoc m topic-partition offset)) {} results))

(defn- cleanup-deps
  [consumer]
  (dissoc consumer
          ::group-id
          ::kafka-consumer
          ::kafka-consumer-config
          ::optional-config
          ::running?
          ::current-offsets
          ::pending-records
          ::paused-partitions
          ::assigned-partitions
          ::last-commit-timestamp
          ::transform-record-fn
          ::consume-fn))

(defn- group-by-topic-partition
  [records]
  (group-by (fn [record] (select-keys record [:topic :partition])) records))

(defn safe-consume-fn
  [consumer]
  (fn [records]
    (let [records-count (count records)
          maybe-exception
          (try
            (notify-tracers tracer/IBeforeConsume
                            tracer/before-consume
                            consumer records-count)
            (consumer-protocol/consume (cleanup-deps consumer) records)
            (notify-tracers tracer/IAfterConsume
                            tracer/after-consume
                            consumer records-count)
            (catch Exception ex
              (notify-tracers tracer/IOnConsumeError
                              tracer/on-consume-error
                              consumer records-count ex)
              ex))]
      (->> records
           (group-by-topic-partition)
           (vals)
           (map (if (instance? Exception maybe-exception)
                  partition-process-fail
                  (comp partition-process-success last)))))))

(defn- safe-consume-partition-fn
  [consumer]
  (fn [records]
    (->> records
         (group-by-topic-partition)
         (cp/pmap
           :builtin
           (fn [[topic-partition records]]
             (let [records-count (count records)
                   maybe-exception
                   (try
                     (notify-tracers tracer/IBeforeConsumePartition
                                     tracer/before-consume-partition
                                     consumer topic-partition records-count)
                     (-> (cleanup-deps consumer)
                         (consumer-protocol/consume-partition topic-partition records))
                     (notify-tracers tracer/IAfterConsumePartition
                                     tracer/after-consume-partition
                                     consumer topic-partition records-count)
                     (catch Exception ex
                       (notify-tracers tracer/IOnConsumePartitionError
                                       tracer/on-consume-partition-error
                                       consumer topic-partition records-count ex)
                       ex))]
               (if (instance? Exception maybe-exception)
                 (partition-process-fail records)
                 (partition-process-success (last records)))))))))

(defn- safe-consume-record-fn
  [consumer]
  (fn [records]
    (->> records
         (group-by-topic-partition)
         (cp/pmap
           :builtin
           (fn [[topic-partition records]]
             (loop [[record & rest-records :as records] records]
               (let [result
                     (try
                       (notify-tracers tracer/IBeforeConsumeRecord
                                       tracer/before-consume-record
                                       consumer record)
                       (consumer-protocol/consume-record (cleanup-deps consumer) record)
                       (notify-tracers tracer/IAfterConsumeRecord
                                       tracer/after-consume-record
                                       consumer record)
                       (catch Exception error
                         (notify-tracers tracer/IOnConsumeRecordError
                                         tracer/on-consume-record-error
                                         consumer record error)
                         error))]
                 (if (instance? Throwable result)
                   (partition-process-fail records)
                   (if (and (seq rest-records) @(::running? consumer))
                     (recur rest-records)
                     (partition-process-success record))))))))))

(defn- manual-consume-partition-fn
  [consumer]
  (fn [records]
    (let [pmap-fn
          (fn [[topic-partition records]]
            (let [records-count (count records)]
              (notify-tracers tracer/IBeforeConsumePartition
                              tracer/before-consume-partition
                              consumer topic-partition records-count)
              (let [result
                    (try
                      (-> (cleanup-deps consumer)
                          (manual-consumer-protocol/consume-partition topic-partition records))
                      (catch Exception catch-to-log
                        (notify-tracers tracer/IAfterConsumePartition
                                        tracer/on-consume-partition-error
                                        consumer topic-partition records-count catch-to-log)
                        (throw catch-to-log)))]
                (notify-tracers tracer/IOnConsumePartitionError
                                tracer/after-consume-partition
                                consumer topic-partition records-count)
                result)))]
      (->> records
           (group-by-topic-partition)
           (cp/pmap :builtin pmap-fn)
           (keep ::commit-record)
           (map partition-process-success)))))

(defn manual-consume-fn
  [consumer]
  (fn [records]
    (let [records-count (count records)]
      (notify-tracers tracer/IBeforeConsume
                      tracer/before-consume
                      consumer records-count)
      (let [result
            (try
              (manual-consumer-protocol/consume (cleanup-deps consumer) records)
              (catch Exception catch-to-log
                (notify-tracers tracer/IOnConsumeError
                                tracer/on-consume-error
                                consumer records-count catch-to-log)
                (throw catch-to-log)))]
        (notify-tracers tracer/IAfterConsume
                        tracer/after-consume
                        consumer records-count)
        (->> result
             ::commit-records
             (map partition-process-success))))))

(defn make-consume-fn
  ;;todo: validate consumer keys
  [consumer]
  (cond
    (satisfies? consumer-protocol/IConsumer consumer)
    (safe-consume-fn consumer)

    (satisfies? consumer-protocol/IPartitionConsumer consumer)
    (safe-consume-partition-fn consumer)

    (satisfies? consumer-protocol/IRecordConsumer consumer)
    (safe-consume-record-fn consumer)

    (satisfies? manual-consumer-protocol/IPartitionConsumer consumer)
    (manual-consume-partition-fn consumer)

    (satisfies? manual-consumer-protocol/IConsumer consumer)
    (manual-consume-fn consumer)))

(defn get-pending-records
  [partition-results]
  (->> partition-results
       (remove :status)
       (mapcat :pending-records)))

(defn get-partitions-to-pause
  [results paused-partitions]
  (set/difference (->> results
                       (remove :status)
                       (map :topic-partition)
                       (set))
                  paused-partitions))

(defn get-partitions-to-resume
  [results paused-partitions]
  (->> results
       (filter :status)
       (map :topic-partition)
       (set)
       (set/intersection paused-partitions)))

(defn get-paused-partitions
  [paused-partitions partitions-to-resume new-paused-partitions]
  (set/union (set/difference paused-partitions partitions-to-resume) new-paused-partitions))

(defn pause-partitions!
  [consumer topic-partitions]
  (when (seq topic-partitions)
    (notify-tracers tracer/IBeforePartitionsPaused
                    tracer/before-partitions-paused
                    consumer topic-partitions)
    (.pause (::kafka-consumer consumer) (map map->TopicPartition topic-partitions))
    (notify-tracers tracer/IAfterPartitionsPaused
                    tracer/after-partitions-paused
                    consumer topic-partitions)))

(defn resume-partitions!
  [consumer topic-partitions]
  (when (seq topic-partitions)
    (notify-tracers tracer/IBeforePartitionsResumed
                    tracer/before-partitions-resumed
                    consumer topic-partitions)
    (.resume (::kafka-consumer consumer) (map map->TopicPartition topic-partitions))
    (notify-tracers tracer/IAfterPartitionsResumed
                    tracer/after-partitions-resumed
                    consumer topic-partitions)))

(defn- trace-poll-loop-iteration-params
  [{:keys [::pending-records ::paused-partitions ::last-commit-timestamp] :as consumer}]
  (let [params {:pending-records-count (count @pending-records)
                :paused-partitions @paused-partitions
                :last-commit-timestamp (or @last-commit-timestamp :never)}]
    (log/tracef "[%s] poll loop iteration params: %s" (::group-id consumer) (pr-str params))))

(defn poll-loop
  [{:keys [::consume-fn] :as consumer}]
  (while true
    (trace-poll-loop-iteration-params consumer)
    (let [paused-partitions @(::paused-partitions consumer)
          records (poll consumer)
          results (consume-fn records)]
      (let [partitions-to-pause (get-partitions-to-pause results paused-partitions)
            partitions-to-resume (get-partitions-to-resume results paused-partitions)]
        (update-consumer-state!
          consumer
          (get-new-offsets results)
          (get-paused-partitions paused-partitions partitions-to-resume partitions-to-pause)
          (get-pending-records results))
        (pause-partitions! consumer partitions-to-pause)
        (resume-partitions! consumer partitions-to-resume)
        (maybe-commit! consumer)))))

(defn get-kafka-consumer-config
  [consumer bootstrap-servers]
  (let [config (-> (config-protocol/kafka-consumer-config consumer)
                   (assoc ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers))]
    (->> (merge config-protocol/KAFKA_CONSUMER_CONFIG_DEFAULTS config)
         (map-vals str))))

(defn- PartitionInfo->TopicPartition
  [^PartitionInfo partition-info]
  (TopicPartition. (.topic partition-info) (.partition partition-info)))

(defn- consumer-fail-loop
  [{:keys [::running?
           ::kafka-consumer
           ::current-offsets
           ::optional-config] :as consumer}]
  (when (:fail-loop? optional-config)
    (try
      (let [sleep-ms (:fail-loop-sleep-ms optional-config)
            failed-at (System/currentTimeMillis)
            topic-partitions (->> (.listTopics kafka-consumer)
                                  (mapcat val)
                                  (map PartitionInfo->TopicPartition))]
        (while @running?
          (let [elapsed-ms (- (System/currentTimeMillis) failed-at)]
            (notify-tracers tracer/IOnConsumerFailLoop
                            tracer/on-consumer-fail-loop
                            consumer elapsed-ms))
          (write-lag-metrics! consumer topic-partitions)
          (Thread/sleep sleep-ms)))
      (catch Throwable ignore-for-shutdown))))

(defn- create-rebalance-listener
  [consumer]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [this topic-partitions]
      (commit-sync! consumer)
      (->> (map TopicPartition->map topic-partitions)
           (notify-tracers tracer/IOnPartitionsRevoked
                           tracer/on-partitions-revoked
                           consumer)))

    (onPartitionsAssigned [this topic-partitions]
      (let [topic-partitions (mapv TopicPartition->map topic-partitions)]
        (init-consumer-state! consumer)
        (reset-assigned-partitions! consumer topic-partitions)
        (notify-tracers tracer/IOnPartitionsAssigned
                        tracer/on-partitions-assigned
                        consumer topic-partitions)))))

(defn start-consumer-thread
  [{:keys [::kafka-consumer] :as consumer}]
  (notify-tracers tracer/IOnConsumerStart tracer/on-consumer-start consumer)
  (let [rebalance-listener (create-rebalance-listener consumer)]
    (.subscribe kafka-consumer (config-protocol/topics consumer) rebalance-listener)
    (try
      (poll-loop consumer)
      (catch WakeupException ignore-for-shutdown
        (log/debugf "[%s] WakeupException is ignored for consumer shutdown" (::group-id consumer)))
      (catch Throwable t
        (notify-tracers tracer/IOnConsumerFail
                        tracer/on-consumer-fail
                        consumer t)
        (consumer-fail-loop consumer))
      (finally
        (try
          (commit-sync! consumer)
          (finally
            (.close kafka-consumer)
            (notify-tracers tracer/IOnConsumerStop
                            tracer/on-consumer-stop
                            consumer)))))))

(defn- trace-create-consumer
  [consumer]
  (log/tracef
    "[%s] create consumer thread with required config %s, topics %s, optional config %s and tracers %s"
    (::group-id consumer)
    (::kafka-consumer-config consumer)
    (config-protocol/topics consumer)
    (::optional-config consumer)
    (->> consumer
         (filter-by-protocol tracer/ITracerName)
         (mapv tracer/tracer-name)
         (pr-str))))

(defn create-consumer
  [bootstrap-servers running? consumer]
  (let [kafka-consumer-config (get-kafka-consumer-config consumer bootstrap-servers)
        kafka-consumer (KafkaConsumer. kafka-consumer-config)
        optional-config (if (satisfies? config-protocol/IOptionalConfig consumer)
                          (merge config-protocol/OPTIONAL_CONFIG_DEFAULTS
                                 (config-protocol/optional-config consumer))
                          config-protocol/OPTIONAL_CONFIG_DEFAULTS)
        start-fn
        (fn []
          (try
            (let [consumer (assoc consumer
                             ::group-id (group-id consumer)
                             ::kafka-consumer kafka-consumer
                             ::kafka-consumer-config kafka-consumer-config
                             ::optional-config optional-config
                             ::running? running?
                             ::current-offsets (atom {})
                             ::pending-records (atom [])
                             ::paused-partitions (atom #{})
                             ::assigned-partitions (atom [])
                             ::last-commit-timestamp (atom nil))
                  ;; make-transform-record-fn needs ::optional-config key in consumer
                  consumer (assoc consumer
                             ::transform-record-fn (make-transform-record-fn consumer))
                  ;; make-consume-fn needs ::transform-fn key in consumer
                  consumer (assoc consumer
                             ::consume-fn (make-consume-fn consumer))]
              (trace-create-consumer consumer)
              (start-consumer-thread consumer))
            (catch Throwable t
              (log/error t "Error in consumer thread")
              (throw t))))]
    {:kafka-consumer kafka-consumer
     :consumer-thread-fn start-fn}))

(defn filter-consumers
  [coll]
  (filter #(and (satisfies? config-protocol/IKafkaConsumerConfig %)
                (satisfies? config-protocol/ITopics %)
                (or (satisfies? consumer-protocol/IConsumer %)
                    (satisfies? consumer-protocol/IRecordConsumer %)
                    (satisfies? consumer-protocol/IPartitionConsumer %)
                    (satisfies? manual-consumer-protocol/IPartitionConsumer %))) coll))

(defn start-consumers
  [bootstrap-servers consumers]
  (let [running? (atom true)
        consumers (map (partial create-consumer bootstrap-servers running?) consumers)
        thread-pool (Executors/newCachedThreadPool)]
    (doseq [^Runnable consumer-thread-fn (map :consumer-thread-fn consumers)]
      (.submit thread-pool consumer-thread-fn))
    (log/info "Started kafka-system")
    {:running? running?
     :kafka-consumers (map :kafka-consumer consumers)
     :consumers-thread-pool thread-pool}))

(defn stop-consumers
  [kafka-system]
  (log/info "Stopping kafka-system")
  (let [{:keys [^ExecutorService consumers-thread-pool
                kafka-consumers running?]} kafka-system]
    (doseq [kafka-consumer kafka-consumers]
      (.wakeup kafka-consumer))
    (reset! running? false)
    (.shutdown consumers-thread-pool)
    (try
      (log/trace "Await consumers thread pool termination")
      (.awaitTermination consumers-thread-pool Integer/MAX_VALUE TimeUnit/SECONDS)
      (catch InterruptedException ie
        (log/error ie "InterruptedException while waiting consumers termination")))
    (log/infof "Stopped kafka-system")))
