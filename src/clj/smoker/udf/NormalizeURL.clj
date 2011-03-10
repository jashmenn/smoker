
(ns smoker.udf.NormalizeURL
  (:import 
   [org.apache.hadoop.hive.ql.exec UDF]
   [java.net URI URL]
   [org.apache.hadoop.io Text])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [smoker.utils :as ru]
   [url-normalizer.core :as norm])
  (:gen-class
   :name smoker.udf.NormalizeURL
   :extends org.apache.hadoop.hive.ql.exec.UDF
   :methods [[evaluate [org.apache.hadoop.io.Text] org.apache.hadoop.io.Text]]))

(defn #^Text evaluate
  "generate a normalized url"
  [#^Text s]
  (try 
    (if s
      (Text. (norm/canonicalize-url (ru/as-url (.toString s))))
      s)
    (catch java.net.URISyntaxException e (do (prn e) nil))
    (catch java.net.MalformedURLException e (do (prn e) nil))))

(defn #^Text -evaluate 
  "Hook for Java"
  [this #^Text s]
  (evaluate s))
