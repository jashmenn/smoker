
(ns smoker.udf.ExtractTagText
  (:import 
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory])
  (:use 
   [smoker.utils])
  (:require 
   [smoker.udtf.gen :as gen]
   [smoker.url-utils :as url-utils]
   [url-normalizer.core :as norm]
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.seq-utils :as sequ])
  (:import 
   [java.net URL]
   [net.htmlparser.jericho TextExtractor StreamedSource 
    Source StartTag]
   [java.util.regex Pattern])
  (:import 
   [org.apache.hadoop.io Text]
   [java.util Date]))

(gen/gen-udtf)
(gen/gen-wrapper-methods 
  [PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector])

(def max-len 500)
(defn- both? [[a b]] (and a b))

(defn extract-tag-text [tag source]
  (let [tags (.getAllElements source tag)]
    (->> tags
         (map 
          (fn [element] 
            [tag (nil-if-exception (truncate (.toString (.getContent element)) max-len))]))
         (filter both?))))

(defn extract-tags-text [tags body]
  (let [source (Source. body)]
    (reduce 
     (fn [tag] (concat (extract-tag-text tag source)))
     []
     (su/split tags #"\|"))))

(defn -operate [this fields]
  (let [[tag body] (seq fields)]
    (if (and tag body)
      (try 
        (extract-tags-text tag body)
        (catch java.lang.RuntimeException e (prn "bad html"))
        (catch java.lang.StackOverflowError e (prn "bad html"))))))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-tags-text "h1|h2" html-doc)

  )

