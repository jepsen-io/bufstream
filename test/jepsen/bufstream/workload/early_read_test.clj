(ns jepsen.bufstream.workload.early-read-test
  (:require [clojure [test :refer :all]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.bufstream.workload.early-read :as e]))

(deftest checker-test
  (let [h (h/history
            [{:process 1, :type :ok, :f :txn, :value {:sent 1, :polled [2]}}
             {:process 2, :type :ok, :f :txn, :value {:sent 2, :polled [1]}}])
        r (checker/check (e/checker) {} h {})]
    (is (= r {:valid? false
              :count 1
              :errors [[(nth h 0)
                        (nth h 1)]]}))))
