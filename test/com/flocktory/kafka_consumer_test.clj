(ns com.flocktory.kafka-consumer-test
  (:require [com.flocktory.kafka-consumer :as kafka-consumer]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [com.flocktory.protocol.manual-consumer :as manual-consumer-protocol]
            [com.flocktory.protocol.consumer :as consumer-protocol])
  (:import (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.clients.consumer OffsetAndMetadata ConsumerRecord)))

(def rand-int* (partial rand-int Integer/MAX_VALUE))

(deftest transform-poll-records-test
  (let [value1 {:action "action1"}
        value2 {:action "action2"}
        value1-str (json/encode value1)
        value2-str (json/encode value2)
        poll-records [(ConsumerRecord. "topic1" 0 0 "key1" value1-str)
                      (ConsumerRecord. "topic2" 0 0 "key2" value2-str)]]
    (let [consumer {::kafka-consumer/optional-config {:value-format :json}}
          transform-record-fn (kafka-consumer/make-transform-record-fn consumer)]
      (is (= [{:topic "topic1"
               :partition 0
               :offset 0
               :key "key1"
               :value value1
               :timestamp -1}
              {:topic "topic2"
               :partition 0
               :offset 0
               :key "key2"
               :value value2
               :timestamp -1}]
             (map (comp transform-record-fn kafka-consumer/ConsumerRecord->map) poll-records))))
    (let [consumer {::kafka-consumer/optional-config {:value-format nil}}
          transform-record-fn (kafka-consumer/make-transform-record-fn consumer)]
      (is (= [{:topic "topic1"
               :partition 0
               :offset 0
               :key "key1"
               :value value1-str
               :timestamp -1}
              {:topic "topic2"
               :partition 0
               :offset 0
               :key "key2"
               :value value2-str
               :timestamp -1}]
             (map (comp transform-record-fn kafka-consumer/ConsumerRecord->map) poll-records))))))

(defn ->record
  ([topic partition offset timestamp key value]
   {:topic topic
    :partition partition
    :offset offset
    :timestamp timestamp
    :key key
    :value value})
  ([topic partition offset timestamp value]
   (->record topic partition offset timestamp (json/encode {:site-id 1} value)))
  ([topic partition offset timestamp]
   (->record topic offset timestamp (json/encode {:action "some-action"}))))

(deftest resume-pause-partitions-test
  (let [FIRST_TOPIC "topic1"
        SECOND_TOPIC "topic2"
        results [(kafka-consumer/partition-process-success {:topic "topic1"
                                                            :partition 0
                                                            :offset 10})
                 (kafka-consumer/partition-process-fail [{:topic "topic2"
                                                          :partition 0
                                                          :offset 11}])]]
    (is (= #{{:topic "topic2"
              :partition 0}} (kafka-consumer/get-partitions-to-pause results #{}))
        "Should pause partition in case of failure")

    (let [already-paused-partitions #{{:topic "topic2"
                                       :partition 0}}]
      (is (= #{} (kafka-consumer/get-partitions-to-pause results already-paused-partitions))
          "Should not pause already paused partitions"))

    (is (= #{} (kafka-consumer/get-partitions-to-resume results #{}))
        "Nothing to resume if no paused partitions")

    (let [already-paused-partitions #{{:topic "topic1"
                                       :partition 0}
                                      {:topic "other-topic"
                                       :partition 1}}]
      (is (= #{{:topic "topic1"
                :partition 0}}
             (kafka-consumer/get-partitions-to-resume results already-paused-partitions))
          "Should resume paused partition in case of recover"))))

(defrecord partition-consumer []
  consumer-protocol/IPartitionConsumer
  (consume-partition
    [this topic-partition records]))

(deftest partition-consumer-test
  (let [record1 {:topic "topic1"
                 :partition 0
                 :offset (rand-int*)}
        record2 {:topic "topic1"
                 :partition 0
                 :offset (rand-int*)}
        record3 {:topic "topic2"
                 :partition 0
                 :offset (rand-int*)}
        partition-consumer
        (assoc (->partition-consumer)
          ::kafka-consumer/current-offsets
          (atom {{:topic "topic1"
                  :partition 0} 0
                 {:topic "topic2"
                  :partition 0} 0
                 {:topic "other-topic"
                  :partition 0} 0})
          ::kafka-consumer/paused-partitions (atom #{{:topic "topic1"
                                                      :partition 0}})
          ::kafka-consumer/pending-records (atom []))
        consume-fn (kafka-consumer/make-consume-fn partition-consumer)
        results (consume-fn [record1 record2 record3])
        paused-partitions @(::kafka-consumer/paused-partitions partition-consumer)]

    (kafka-consumer/update-consumer-state!
      partition-consumer (kafka-consumer/get-new-offsets results)
      (kafka-consumer/get-paused-partitions
        paused-partitions
        (kafka-consumer/get-partitions-to-resume results paused-partitions)
        (kafka-consumer/get-partitions-to-pause results paused-partitions))
      (kafka-consumer/get-pending-records results))

    (is (= {{:topic "topic1"
             :partition 0} (inc (:offset record2))
            {:topic "topic2"
             :partition 0} (inc (:offset record3))
            {:topic "other-topic"
             :partition 0} 0}
           @(::kafka-consumer/current-offsets partition-consumer)))

    (is (= #{} @(::kafka-consumer/paused-partitions partition-consumer)))
    (is (= [] @(::kafka-consumer/pending-records partition-consumer)))))

(defrecord manual-partition-consumer [consume-partition-fn]
  manual-consumer-protocol/IPartitionConsumer
  (consume-partition
    [this topic-partition records]
    (consume-partition-fn this topic-partition records)))

(deftest manual-partition-consumer-test
  (let [record1 {:topic "topic1"
                 :partition 0
                 :offset (rand-int Integer/MAX_VALUE)}
        record2 {:topic "topic1"
                 :partition 0
                 :offset (rand-int Integer/MAX_VALUE)}
        record3 {:topic "topic2"
                 :partition 0
                 :offset (rand-int Integer/MAX_VALUE)}]
    (testing "all partitions return records to commit"
      (let [consumer (-> (fn [this topic-partition records]
                           {::kafka-consumer/commit-record (last records)})
                         (->manual-partition-consumer))
            consume-fn (kafka-consumer/make-consume-fn consumer)
            results (consume-fn [record1 record2 record3])]

        (is (= {{:topic "topic1"
                 :partition 0} (inc (:offset record2))
                {:topic "topic2"
                 :partition 0} (inc (:offset record3))}
               (kafka-consumer/get-new-offsets results)))))
    (testing "if nothing to commit"
      (let [consumer (-> (fn [this topic-partition records]
                           :nothing-to-commit)
                         (->manual-partition-consumer))
            consume-fn (kafka-consumer/make-consume-fn consumer)
            results (consume-fn [record1 record2 record3])
            offsets (kafka-consumer/get-new-offsets results)]
        (is (= {} offsets))))))

(deftest should-commit?-test
  (is (true? (kafka-consumer/should-commit? {::kafka-consumer/last-commit-timestamp (atom nil)}))))

(deftest kafka-offsets-test
  (is (= {(TopicPartition. "topic0" 0) (OffsetAndMetadata. 0)
          (TopicPartition. "topic1" 1) (OffsetAndMetadata. 1)}
         (kafka-consumer/kafka-offsets {{:topic "topic0"
                                         :partition 0} 0
                                        {:topic "topic1"
                                         :partition 1} 1}))))
