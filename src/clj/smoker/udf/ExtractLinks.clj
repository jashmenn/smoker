
(ns smoker.udf.ExtractLinks
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
   [smoker.udf ClojureUDTF]
   [java.util ArrayList List]
   [org.apache.hadoop.hive.ql.exec UDFArgumentException]
   [org.apache.hadoop.hive.ql.metadata HiveException]
   [org.apache.hadoop.hive.ql.udf.generic GenericUDTF]
   [org.apache.hadoop.hive.serde2.objectinspector
    ObjectInspector ObjectInspectorFactory PrimitiveObjectInspector
    StructObjectInspector]
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory]
   [org.apache.hadoop.hive.ql.exec UDF]
   [org.apache.hadoop.io Text]
   [java.util Date]
   ))

(gen/gen-udtf)
(gen/gen-wrapper-methods 
  [PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector])

(defn- both? [[a b]] (and a b))

(defn truncate [s c]
  (if (and s (> (.length s) c))
    (.substring s 0 c)
    s))

(def max-len 500)

(defn extract-links [in-url body]
  (let [source (Source. body)
        atags (.getAllElements source "a")]
    (->> 
     (reduce                                          ;; extract hrefs
      (fn [acc element] 
        (let [href (nil-if-exception (truncate (.getAttributeValue element "href") max-len))
              txt  (nil-if-exception (truncate (.toString (.getContent element)) max-len))]
          (cons [txt href] acc))) [] atags)
     (filter both?)                                ;; remove nils
     (filter (fn [[txt href]] (re-find #"^[^#]" href))) ;; ignore anchors
     (map (fn [[txt href]] 
            [txt (nil-if-exception (url-utils/href-to-url href in-url))]))
     (filter both?)                                ;; remove nils ?
     (map (fn [[txt href]] [txt (nil-if-exception (norm/canonicalize-url href))])) ;; normalize
     (distinct)))) ;; uniq

(defn -operate [this fields]
  (let [[source-url body] (seq fields)]
    (if (and source-url body)
      (try 
        (extract-links source-url body)
        (catch java.lang.RuntimeException e (prn "bad html" source-url))
        (catch java.lang.StackOverflowError e (prn "bad html" source-url))))))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-links "http://a.com" html-doc)

  )

