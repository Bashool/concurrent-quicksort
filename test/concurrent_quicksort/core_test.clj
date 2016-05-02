(ns concurrent-quicksort.core-test
  (:require [clojure.test :refer :all]
            [concurrent-quicksort.core :refer :all]))

(deftest sort-correctly
  (is (= true
         (let [unsorted (read-dataset fname nitems)]))))
