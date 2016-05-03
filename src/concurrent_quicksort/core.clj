(ns concurrent-quicksort.core)
;I was influenced by the following solution on stack overflow
;;;http://stackoverflow.com/questions/36731188/multithreaded-merge-sort-algorithm-in-clojure

;;;Lazy QuickSort implementation in Clojure, from Joy of Clojure chapter 6.
;;;https://gist.github.com/noahlz/1592879
(defn sort-parts [work]
  (lazy-seq
    (loop [[part & parts] work]                             ;; Pull apart work - note: work will be a list of lists.
      (if-let [[pivot & xs] (seq part)]                     ;; This blows up unless work was a list of lists.
        (let [smaller? #(< % pivot)]                        ;; define pivot comparison function.
          (recur (list*
                   (filter smaller? xs)                     ;; work all < pivot
                   pivot                                    ;; work pivot itself
                   (remove smaller? xs)                     ;; work all > pivot
                   parts)))                                 ;; concat parts
        (when-let [[x & parts] parts]                       ;; sort rest if more parts
          (cons x (sort-parts parts)))))))

;;;http://stackoverflow.com/questions/36731188/multithreaded-merge-sort-algorithm-in-clojure
(defn merge-lists [left right]
  (loop [head [] L left R right]
    (if (empty? L) (concat head R)
                   (if (empty? R) (concat head L)
                                  (if (> (first L) (first R))
                                    (recur (conj head (first R)) L (rest R))
                                    (recur (conj head (first L)) (rest L) R))))))

(defn qsort [xs]
  (sort-parts (list xs)))                                   ;; The (list) function ensures that we pass sort-parts a list of lists.


;;;read a file of integers separated by a new line
;INPUT
;fname = path to the file to be read in
;nitems = # of items in each partition
;OUTPUT
;a partitoned list of integers with nitems in each partition
;ASSUMPTIONS
;each item in the dataset is an integer
;the number of items read in from the file is divisible by nitems
(defn read-dataset [fname nitems]
  (partition nitems                                         ;;;Partition the list to have nitems in each partition
             (map read-string                               ;;;Convert each string to long
                  (clojure.string/split-lines               ;;;split string by line
                    (slurp fname)))))                       ;;;read file




;;;https://github.com/TheClimateCorporation/claypoole
(require '[com.climate.claypoole :as cp])


;INPUT
;fname = path to the file to read in
;nitems = # of items in each partition
;nthreads = # of threads in the thread pool
;OUTPUT
;time it takes to sort the list with the given partitions, and size of the thread pool
;ASSUMPTIONS
;the number of items read in from the file is divisible by nitems
(defn concurrent-sort [fname nitems nthreads]
  (let [unsorted (read-dataset fname nitems)]
    (println (count unsorted) "partitions with" nitems "elements in each partition with a thread pool of" nthreads "threads")
    (cp/with-shutdown! [pool (cp/threadpool nthreads)]      ;;;start a thread pool with nthreads
                       (time                                ;;;record time
                         (reduce merge-lists                ;;;merge the partitions together into a single sorted list
                                 (cp/pmap pool qsort unsorted)))))) ;;;Execute quick sort on each partition

;INPUT
;fname = path to the file to read in
;nitems = # of items in each partition
;OUTPUT
;time it takes to sort the list with the given partitions
(defn serial-sort [fname nitems]
  (let [unsorted (read-dataset fname nitems)]
    (println (count unsorted) "partitions with" nitems "elements in each partition")
    (time
      (reduce merge-lists
              (cp/pmap :serial qsort unsorted)))))

;(println (cp/ncpus) "CPUS")

(let [fname "../../resources/numbers.dat"]
  (serial-sort fname 1000000)                               ;1 Partition
  (serial-sort fname 500000)                                ;2 Partitions
  (serial-sort fname 250000)                                ;4 Partitions
  (serial-sort fname 125000)                                ;8 Partitions
  (serial-sort fname 62500)                                 ;16 Partitions
  (serial-sort fname 31250)                                 ;32 Partitions
  (concurrent-sort fname 1000000 1)                         ;1 Thread, 1 Partition
  (concurrent-sort fname 500000 1)                          ;1 Thread, 2 Partitions
  (concurrent-sort fname 250000 1)                          ;1 Thread, 4 Partitions
  (concurrent-sort fname 125000 1)                          ;1 Thread, 8 Partitions
  (concurrent-sort fname 62500 1)                           ;1 Thread, 16 Partitions
  (concurrent-sort fname 31250 1)                           ;1 Thread, 32 Partitions
  (concurrent-sort fname 1000000 2)                         ;2 Threads, 1 Partition
  (concurrent-sort fname 500000 2)                          ;2 Threads, 2 Partitions
  (concurrent-sort fname 250000 2)                          ;2 Threads, 4 Partitions
  (concurrent-sort fname 125000 2)                          ;2 Threads, 8 Partitions
  (concurrent-sort fname 62500 2)                           ;2 Threads, 16 Partitions
  (concurrent-sort fname 31250 2)                           ;2 Threads, 32 Partitions
  (concurrent-sort fname 1000000 4)                         ;4 Threads, 1 Partitions
  (concurrent-sort fname 500000 4)                          ;4 Threads, 2 Partitions
  (concurrent-sort fname 250000 4)                          ;4 Threads, 4 Partitions
  (concurrent-sort fname 125000 4)                          ;4 Threads, 8 Partitions
  (concurrent-sort fname 62500 4)                           ;4 Threads, 16 Partitions
  (concurrent-sort fname 31250 4)                           ;4 Threads, 32 Partitions
  (concurrent-sort fname 1000000 8)                         ;8 Threads, 1 Partitions
  (concurrent-sort fname 500000 8)                          ;8 Threads, 2 Partitions
  (concurrent-sort fname 250000 8)                          ;8 Threads, 4 Partitions
  (concurrent-sort fname 125000 8)                          ;8 Threads, 8 Partitions
  (concurrent-sort fname 62500 8)                           ;8 Threads, 16 Partitions
  (concurrent-sort fname 31250 8)                           ;8 Threads, 32 Partitions
  (concurrent-sort fname 1000000 16)                        ;16 Threads, 1 Partitions
  (concurrent-sort fname 500000 16)                         ;16 Threads, 2 Partitions
  (concurrent-sort fname 250000 16)                         ;16 Threads, 4 Partitions
  (concurrent-sort fname 125000 16)                         ;16 Threads, 8 Partitions
  (concurrent-sort fname 62500 16)                          ;16 Threads, 16 Partitions
  (concurrent-sort fname 31250 16)                          ;16 Threads, 32 Partitions
  (concurrent-sort fname 1000000 32)                        ;32 Threads, 1 Partitions
  (concurrent-sort fname 500000 32)                         ;32 Threads, 2 Partitions
  (concurrent-sort fname 250000 32)                         ;32 Threads, 4 Partitions
  (concurrent-sort fname 125000 32)                         ;32 Threads, 8 Partitions
  (concurrent-sort fname 62500 32)                          ;32 Threads, 16 Partitions
  (concurrent-sort fname 31250 32)                          ;32 Threads, 32 Partitions

  )



(println "finished")
