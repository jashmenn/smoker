
(ns smoker.udf.ExtractLinks
  (:import 
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory])
  (:use [smoker.utils])
  (:require 
   [smoker.url-utils :as url-utils]
   [url-normalizer.core :as norm]
   [clojure.contrib.str-utils2 :as su])
  (:import 
   [java.net URL]
   [net.htmlparser.jericho TextExtractor StreamedSource 
    Source StartTag]
   [java.util.regex Pattern]))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-links "http://a.com" html-doc)

  )

(defn both? [[a b]] (and a b))

(defn extract-links [in-url body]
  (let [source (Source. body)
        atags (.getAllElements source "a")]
    (->> 
     (reduce                                          ;; extract hrefs
      (fn [acc element] 
        (let [href (.getAttributeValue element "href")
              txt (.toString (.getContent element))]
          (cons [txt href] acc))) [] atags)
     (filter both?)                                ;; remove nils
     (filter (fn [[txt href]] (re-find #"^[^#]" href))) ;; ignore anchors
     (map (fn [[txt href]] 
            [txt 
             (nil-if-exception (url-utils/href-to-url href in-url))]))
     (filter both?)                                ;; remove nils ?
     (map (fn [[txt href]] [txt (norm/canonicalize-url href)])) ;; normalize
     (distinct)))) ;; uniq

(defn do-extraction [source-url body]

  )
