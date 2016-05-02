(ns concurrent-quicksort.core)
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

(defn qsort [xs]
  (sort-parts (list xs)))                                   ;; The (list) function ensures that we pass sort-parts a list of lists.


;;;read a file of integers separated by a new line
(defn read-dataset [fname nitems]
  (partition nitems
             (map read-string                               ;;;Convert each string to long
                  (clojure.string/split-lines               ;;;split string by line
                    (slurp fname))                          ;;;read file
                  )))


(defn merge-lists [left right]
  (loop [head [] L left R right]
    (if (empty? L) (concat head R)
                   (if (empty? R) (concat head L)
                                  (if (> (first L) (first R))
                                    (recur (conj head (first R)) L (rest R))
                                    (recur (conj head (first L)) (rest L) R))))))

(defn merge-sort [list]
  (if (< (count list) 2) list
                         (apply merge-lists
                                (map merge-sort
                                     (split-at (/ (count list) 2) list)))))



;;;;; First test that the final result is actually sorted
;(def testing ( reduce merge-lists
;                      (pmap qsort (read-dataset "../../resources/numbers.dat" 31250))))
;
;;;; Print "true" if sorted, else print "false"
;(println (apply <= testing))


(require '[com.climate.claypoole :as cp])

;(dotimes [_ 10]
;  (let [unsorted (read-dataset "../../resources/numbers.dat" 31250)]
;    (time
;      (dotimes [_ 1]
;        (reduce merge-lists
;                (pmap qsort unsorted))))))



;(cp/with-shutdown! [pool (cp/threadpool 2)]
;                   (let [unsorted (read-dataset "../../resources/numbers.dat" 31250)]
;                     (dotimes [_ 10]
;                       (time
;                         (dotimes [_ 1]
;                           (reduce merge-lists
;                                   (cp/pmap pool qsort unsorted)))))))

;INPUT
;fname = path to the file to read in
;nitems = # of items in each partition
;niters = # of times to run the sorting algorithm
;OUTPUT
;time it takes to sort the list with the given partitions
;(defn serial-sort [fname nitems niters]
;  (let [unsorted (read-dataset fname nitems)]
;    (println (count unsorted) "partitions with" nitems "elements in each partition")
;    ;(println unsorted)
;    (dotimes [_ niters]
;      (time
;        (dotimes [_ 1]
;          (reduce merge-lists
;                  (map qsort unsorted)))))))

;INPUT
;fname = path to the file to read in
;nitems = # of items in each partition
;niters = # of times to run the sorting algorithm
;nthreads = # of threads in the thread pool
;OUTPUT
;time it takes to sort the list with the given partitions, and size of the thread pool
(defn concurrent-sort [fname nitems niters nthreads]
  (let [unsorted (read-dataset fname nitems)]
    (println (count unsorted) "partitions with" nitems "elements in each partition with a thread pool of" nthreads "threads")
    ;(println unsorted)
    (dotimes [_ niters]
      (cp/with-shutdown! [pool (cp/threadpool nthreads)]
                         (time
                           (dotimes [_ 1]
                             (reduce merge-lists
                                     (cp/pmap pool qsort unsorted))))))))

;INPUT
;fname = path to the file to read in
;nitems = # of items in each partition
;niters = # of times to run the sorting algorithm
;OUTPUT
;time it takes to sort the list with the given partitions
(defn serial-sort [fname nitems niters]
  (let [unsorted (read-dataset fname nitems)]
    (println (count unsorted) "partitions with" nitems "elements in each partition")
    ;(println unsorted)
    (dotimes [_ niters]
      (time
        (dotimes [_ 1]
          (reduce merge-lists
                  (cp/pmap :serial qsort unsorted)))))))

;(println (cp/ncpus) "CPUS")

(let [fname "../../resources/numbers.dat"]
  ;(serial-sort fname 1000000 1)                             ;1 Partition
  ;(serial-sort fname 500000 1)                              ;2 Partitions
  ;(serial-sort fname 250000 1)                              ;4 Partitions
  ;(serial-sort fname 125000 1)                              ;8 Partitions
  ;(serial-sort fname 62500 1)                              ;16 Partitions
  ;(serial-sort fname 31250 1)                              ;32 Partitions
  ;(concurrent-sort fname 1000000 1 1)                       ;1 Thread, 1 Partition
  ;(concurrent-sort fname 500000 1 1)                        ;1 Thread, 2 Partitions
  ;(concurrent-sort fname 250000 1 1)                       ;1 Thread, 4 Partitions
  ;(concurrent-sort fname 125000 1 1)                       ;1 Thread, 8 Partitions
  ;(concurrent-sort fname 62500 1 1)                       ;1 Thread, 16 Partitions
  ;(concurrent-sort fname 31250 1 1)                       ;1 Thread, 32 Partitions
  ;(concurrent-sort fname 1000000 1 2)                       ;2 Threads, 1 Partition
  ;(concurrent-sort fname 500000 1 2)                        ;2 Threads, 2 Partitions
  ;(concurrent-sort fname 250000 1 2)                       ;2 Threads, 4 Partitions
  ;(concurrent-sort fname 125000 1 2)                       ;2 Threads, 8 Partitions
  ;(concurrent-sort fname 62500 1 2)                       ;2 Threads, 16 Partitions
  ;(concurrent-sort fname 31250 1 2)                       ;2 Threads, 32 Partitions
  ;(concurrent-sort fname 1000000 1 4)                       ;4 Threads, 1 Partitions
  ;(concurrent-sort fname 500000 1 4)                        ;4 Threads, 2 Partitions
  (concurrent-sort fname 250000 1 4)                       ;4 Threads, 4 Partitions
  ;(concurrent-sort fname 125000 1 4)                       ;4 Threads, 8 Partitions
  ;(concurrent-sort fname 62500 1 4)                       ;4 Threads, 16 Partitions
  ;(concurrent-sort fname 31250 1 4)                       ;4 Threads, 32 Partitions
  ;(concurrent-sort fname 1000000 1 8)                       ;8 Threads, 1 Partitions
  ;(concurrent-sort fname 500000 1 8)                        ;8 Threads, 2 Partitions
  ;(concurrent-sort fname 250000 1 8)                       ;8 Threads, 4 Partitions
  ;(concurrent-sort fname 125000 1 8)                       ;8 Threads, 8 Partitions
  ;(concurrent-sort fname 62500 1 8)                       ;8 Threads, 16 Partitions
  ;(concurrent-sort fname 31250 1 8)                       ;8 Threads, 32 Partitions
  ;(concurrent-sort fname 1000000 1 16)                       ;16 Threads, 1 Partitions
  ;(concurrent-sort fname 500000 1 16)                        ;16 Threads, 2 Partitions
  ;(concurrent-sort fname 250000 1 16)                       ;16 Threads, 4 Partitions
  ;(concurrent-sort fname 125000 1 16)                       ;16 Threads, 8 Partitions
  ;(concurrent-sort fname 62500 1 16)                       ;16 Threads, 16 Partitions
  ;(concurrent-sort fname 31250 1 16)                       ;16 Threads, 32 Partitions
  ;(concurrent-sort fname 1000000 1 32)                       ;32 Threads, 1 Partitions
  ;(concurrent-sort fname 500000 1 32)                        ;32 Threads, 2 Partitions
  ;(concurrent-sort fname 250000 1 32)                       ;32 Threads, 4 Partitions
  ;(concurrent-sort fname 125000 1 32)                       ;32 Threads, 8 Partitions
  ;(concurrent-sort fname 62500 1 32)                       ;32 Threads, 16 Partitions
  ;(concurrent-sort fname 31250 1 32)                       ;32 Threads, 32 Partitions

  )



(println "finished")
