
(ns smoker.udf.RobotsURL
  (:import 
   [org.apache.hadoop.hive.ql.exec UDF]
   [java.net URI URL]
   [org.apache.hadoop.io Text])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.logging :as log]
   [raven.utils :as ru]
   [url-normalizer.core :as norm])
  (:gen-class
   :name smoker.udf.RobotsURL
   :extends org.apache.hadoop.hive.ql.exec.UDF
   :methods [[evaluate [org.apache.hadoop.io.Text] org.apache.hadoop.io.Text]]))

(defn robots-url [#^URL url]
  (str (URI. (.getProtocol url)
         (.getUserInfo url)
         (.getHost url)
         (.getPort url)
         "/robots.txt"
         nil 
         nil)))

(defn #^Text evaluate
  "generate a robots url"
  [#^Text s]
  (try 
    (if s
      (let [url (norm/canonicalize-url (ru/as-url (.toString s)))
            robots (robots-url (ru/as-url url))]
       (Text. (str robots)))
      s)
    (catch java.net.URISyntaxException e (do (log/warn e) nil))
    (catch java.net.MalformedURLException e (do (log/warn e) nil))))

(defn #^Text -evaluate 
  "Hook for Java"
  [this #^Text s]
  (evaluate s))
