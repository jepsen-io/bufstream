(ns jepsen.bufstream.core
  "Common core functions used by the whole test suite."
  (:require [jepsen [role :as role]]))

(defn roles
  "Given a collection of nodes, returns the roles of each node."
  [[c s & more]]
  {:coordination [c]
   :storage      [s]
   :bufstream    more})
