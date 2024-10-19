(defproject jepsen.bufstream "0.1.0"
  :description "Jepsen tests for the Bufstream distributed log"
  :url "https://github.com/jepsen-io/bufstream"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [io.jepsen/redpanda "0.1.3"
                  :exclusions [org.apache.kafka/kafka-clients
                               cheshire
                               commons-io
                               potemkin]]
                 [jepsen.etcd "0.2.4"
                  :exclusions [jepsen]]
                 [org.apache.kafka/kafka-clients "3.8.0"
                  :exclusions [org.slf4j/slf4j-api]]]
  :repl-options {:init-ns jepsen.bufstream.cli}
  :main jepsen.bufstream.cli
  :jvm-opts ["-server"
             ;"-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             "-Xmx24g"
             ])
