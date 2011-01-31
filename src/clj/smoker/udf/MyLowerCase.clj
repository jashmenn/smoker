
(ns smoker.udf.MyLowerCase
  (:import [org.apache.hadoop.hive.ql.exec UDF])
  (:import [org.apache.hadoop.io Text])
  (:require [clojure.contrib.str-utils2 :as su])
  (:gen-class
   :name smoker.udf.MyLowerCase
   :extends org.apache.hadoop.hive.ql.exec.UDF
   :methods [[evaluate [org.apache.hadoop.io.Text] org.apache.hadoop.io.Text]]
   ))

(defn #^Text evaluate
  "lowercase the Text"
  [#^Text s]
  (if s
    (Text. (su/lower-case (.toString s)))
    s))

(defn #^Text -evaluate 
  "Hook for Java"
  [this #^Text s]
  (evaluate s))
