
(ns smoker.udf.URLHostname
  (:import 
   [org.apache.hadoop.hive.ql.exec UDF]
   [java.net URI URL]
   [org.apache.hadoop.io Text])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.logging :as log]
   [smoker.url-utils :as ru]
   [url-normalizer.core :as norm])
  (:gen-class
   :name smoker.udf.URLHostname
   :extends org.apache.hadoop.hive.ql.exec.UDF
   :methods [[evaluate [org.apache.hadoop.io.Text] org.apache.hadoop.io.Text]]))

(defn #^Text evaluate
  "extract the hostname for this url"
  [#^Text s]
  (try 
    (if s
      (let [url (norm/canonicalize-url (ru/as-url (.toString s)))]
       (Text. (str (.getHost url))))
      s)
    (catch java.net.URISyntaxException e (do (log/warn e) nil))
    (catch java.net.MalformedURLException e (do (log/warn e) nil))))

(defn #^Text -evaluate 
  "Hook for Java"
  [this #^Text s]
  (evaluate s))
