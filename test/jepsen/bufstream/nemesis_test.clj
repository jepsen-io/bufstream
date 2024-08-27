(ns jepsen.bufstream.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.bufstream.nemesis :refer :all]))

(deftest update-role-faults-test
  (is (= {:faults #{:kill, :pause}}
         (update-role-faults :storage
                             {:faults #{:kill ; general, passed through
                                        :pause-storage ; right role
                                        :clock-coordination ; wrong role
                                        :partition}})))) ; n/a for single-node
