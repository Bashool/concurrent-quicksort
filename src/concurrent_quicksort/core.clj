(ns concurrent-quicksort.core)
;;;http://stackoverflow.com/questions/12176832/quicksort-in-clojure


(set! *unchecked-math* true)
(set! *warn-on-reflection* true)

(defn swap [^longs a ^long i ^long j]
  (let [t (aget a i)]
    (aset a i (aget a j))
    (aset a j t)))

(defn ^long apartition [^longs a ^long pivot ^long i ^long j]
  (loop [i i j j]
    (if (<= i j)
      (let [v (aget a i)]
        (if (< v pivot)
          (recur (inc i) j)
          (do
            (when (< i j)
              (aset a i (aget a j))
              (aset a j v))
            (recur i (dec j)))))
      i)))

(defn qsort
  ([^longs a]
   (qsort a 0 (long (alength a))))
  ([^longs a ^long lo ^long hi]
   (when
     (< (inc lo) hi)
     (let [pivot (aget a lo)
           split (dec (apartition a pivot (inc lo) (dec hi)))]
       (when (> split lo)
         (swap a lo split))
       (qsort a lo split)
       (qsort a (inc split) hi)
       ;;;(.start (Thread. (fn [] ))) (qsort a lo split)
       ;;;(.start (Thread. (fn [] (qsort a (inc split) hi))))
       ;;;(agent (qsort a lo split))
       ;;;(agent (qsort a (inc split) hi))
       ))
   a))

(defn ^longs rand-long-array []
  (let [rnd (java.util.Random.)]
    (long-array (repeatedly 100000 #(.nextLong rnd)))))

(comment
  (dotimes [_ 10]
    (let [as (rand-long-array)]
      (time
        (dotimes [_ 1]
          (qsort as)))))
  )

;;;read a file of integers separated by a new line
(defn read-dataset [fname]
  (map read-string                                          ;;;Convert each string to long
       (clojure.string/split-lines                          ;;;split string by line
         (slurp fname))                                     ;;;read file
       )
  )

(def unsorted (read-dataset "../../resources/numbers.dat"))

(println (type unsorted))



(def sorted (qsort (long-array unsorted)))

(println (type sorted))

(println (count sorted))

(println (apply <= unsorted))

(println (apply <= sorted))

(dotimes [_ 10]
  (time
    (dotimes [_ 1]
      (qsort (long-array unsorted)))))

(println "finished")

(defn sort-parts
  "Lazy, tail-recursive, incremental quicksort. Works against and
creates partitions based on the pivot, defined as 'work'."
  [work]
  (lazy-seq
    (loop [[part & parts] work]
      (if-let [[pivot & xs] (seq part)]
        (do
            (let [smaller? #(< % pivot)]
              (recur (list*
                       (filter smaller? xs)
                       pivot
                       (remove smaller? xs)
                       parts))))
        (when-let [[x & parts] parts]
          (cons x (sort-parts parts)))))))

(defn qsort2 [xs]
  (sort-parts (list xs)))

(require '[com.climate.claypoole :as cp])

(defn sort-parts-future [work pool]
  (lazy-seq
    (loop [[part & parts] work]                             ;;;Pull apart work
      (if-let [[pivot & xs] (seq part)]
        (let [smaller? #(< % pivot)]                        ;;;Define pivot comparison fn
          (recur (list*
                   (filter smaller? xs)                     ;;;Work all smaller than pivot
                   pivot                                    ;;;Work the pivot itself
                   (remove smaller? xs)                     ;;;Work all greater than pivot
                   parts)))                                 ;;;Concat parts
        (when-let [[x & parts] parts]
          (cons x @(cp/future pool (sort-parts-future parts pool))))))))                  ;;;Sort the rest if more parts

(defn qsort-future [xs]
  ;;;(println (type xs))
  ;;;(println (list xs))
  (def pool (cp/threadpool 50))
  (sort-parts-future (list xs) pool))


(def sorted-future (qsort-future unsorted))

(println (apply <= sorted-future))

(time (qsort-future unsorted))