
(ns smoker.udf.ExtractLinks
  (:import 
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory])
  (:use 
   [smoker.utils])
  (:require 
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

(gen-class
   :name "smoker.udf.ExtractLinks"
   :extends smoker.udf.ClojureUDTF
   :init "init"
   :constructors {[] []}
   :methods [["operate" [Object] clojure.lang.ISeq]]
   :state "state"
   )

(defn both? [[a b]] (and a b))

(defn truncate [s c]
  (if (> (.length s) c)
    (.substring s 0 c)
    s))

(def max-len 500)

(defn extract-links [in-url body]
  (let [source (Source. body)
        atags (.getAllElements source "a")]
    (->> 
     (reduce                                          ;; extract hrefs
      (fn [acc element] 
        (let [href (truncate (.getAttributeValue element "href") max-len)
              txt  (truncate (.toString (.getContent element)) max-len)]
          (cons [txt href] acc))) [] atags)
     (filter both?)                                ;; remove nils
     (filter (fn [[txt href]] (re-find #"^[^#]" href))) ;; ignore anchors
     (map (fn [[txt href]] 
            [txt (nil-if-exception (url-utils/href-to-url href in-url))]))
     (filter both?)                                ;; remove nils ?
     (map (fn [[txt href]] [txt (norm/canonicalize-url href)])) ;; normalize
     (distinct)))) ;; uniq

(defn -init [] [[] (atom [])])
(defn -close [this])

(def emits
  [PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector])

(defn -initialize [this args]
  (if (not (= (count args) (count emits)))
    (throw 
     (UDFArgumentException. 
      (str *ns* " takes exactly " (count emits) " arguments"))))
  (swap! (.state this) (fn [_] args))
  (let [fieldNames (ArrayList. (map str (range 0 (count emits))))
        fieldIOs (ArrayList. emits)]
    (ObjectInspectorFactory/getStandardStructObjectInspector 
     fieldNames fieldIOs)))

(defn -process [this record]
  (let [primitives 
        (map
         (fn [[i thingy]]
           (.getPrimitiveJavaObject thingy (nth record i)))
         (sequ/indexed @(.state this)))]
    (doall 
     (map 
      (fn [results] 
        (.emit this (into-array Object results))) 
      (.operate this primitives)))))

(defn -operate [this fields]
  (let [[source-url body] (seq fields)]
    (extract-links source-url body)))

(comment "UNTESTED")

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-links "http://a.com" html-doc)

  )

