
(ns smoker.udf.RationalizeURL
  (:import 
   [org.apache.hadoop.hive.ql.exec UDF]
   [java.net URI URL]
   [org.apache.hadoop.io Text])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [smoker.url-utils :as ru]
   [url-normalizer.core :as norm])
  (:gen-class
   :name smoker.udf.RationalizeURL
   :extends org.apache.hadoop.hive.ql.exec.UDF
   :methods [[evaluate 
              [org.apache.hadoop.io.Text org.apache.hadoop.io.Text] 
               org.apache.hadoop.io.Text]]))

(defn #^Text evaluate
  "generate a full url from a relative one"
  [source href]
  (try 
    (if (and source href)
      (Text. (ru/href-to-url (.toString href) (.toString source)))
      "")
    (catch java.net.URISyntaxException e (do (prn e) nil))
    (catch java.net.MalformedURLException e (do (prn e) nil))))

(defn #^Text -evaluate 
  "Hook for Java"
  [this #^Text source #^Text href]
  (evaluate source href))
