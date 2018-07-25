(defproject com.flocktory/kafka-consumer "0.0.8"
  :description "High level kafka consumer in clojure"
  :url "https://github.com/flocktory/kafka-consumer"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.climate/claypoole "1.1.4"]
                 [cheshire "5.8.0"]
                 [org.apache.kafka/kafka_2.11 "1.0.0"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.clojure/tools.logging "0.4.0"]]
  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :sign-releases false}]])
