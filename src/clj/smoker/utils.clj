
(ns smoker.utils
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.io :as io])
  (:import [java.net URI URL]))

(defmacro nil-if-exception [& body]
  `(try (do ~@body) (catch Exception e# nil)))

(defn re-sub-all-but-last [regex replacement string]
  (let [count (count (doall (re-seq regex string)))]
    (loop [str string i (- count 1)]
      (if (<= i 0)
        str
        (recur (su/replace-first str regex replacement) (dec i))))))

