;; hack for using http nexus repos
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
  "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject com.flocktory/kafka-consumer "0.0.9-DEV4-SNAPSHOT"
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
  :deploy-repositories
  [["snapshots"
    {:sign-releases false,
     :url
                    "http://artifacts.dev.flocktory.com:8081/nexus/content/repositories/snapshots/",
     :username :env/nexus_username,
     :password :env/nexus_password}]
   ["releases"
    {:sign-releases false,
     :url
                    "http://artifacts.dev.flocktory.com:8081/nexus/content/repositories/releases/",
     :username :env/nexus_username,
     :password :env/nexus_password}]]
)
