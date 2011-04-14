
(ns smoker.udf.ExtractLinks
  (:import 
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory])
  (:use 
   [smoker.utils]
        [clojure.contrib.apply-macro])
  (:require 
   [smoker.url-utils :as url-utils]
   [url-normalizer.core :as norm]
   [clojure.contrib.str-utils2 :as su]
   )
  (:import 
   [java.net URL]
   [net.htmlparser.jericho TextExtractor StreamedSource 
    Source StartTag]
   [java.util.regex Pattern]))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-links-streaming "http://a.com" html-doc)

  )

(defn both? [[a b]] (and a b))

(defn with-segments [tag-name in-url body f]
  (map 
   (fn [seg]
     (if (and (= (type seg) StartTag)
              (= tag-name (.getName seg)))
       (f seg))) 
   (iterator-seq (.iterator (StreamedSource. body)))))

(defn get-links [in-url body]
  (with-segments "a" in-url body 
    (fn [seg]
      [
     ;;  (first (seq (.getChildElements seg)))
       (.getElement seg) 
     ;;(.toString (.getTextExtractor seg)) 
       (.getAttributeValue seg "href")]
      )))

(defn extract-links-streaming [in-url body]
  (->> 
   (get-links in-url body)
   (filter both?) ;; remove nils
   (map (fn [[txt href]] 
          [txt (nil-if-exception (url-utils/href-to-url href in-url))])) ;; fix relative
   ;; (filter all-true)                                ;; remove nils ?
   ;; (map (fn [x] (nil-if-exception 
   ;;              (norm/canonicalize-url x))))        ;; normalize
   ;; (filter identity)                                ;; remove nils ?
   ;;(distinct)
   ))


(defn do-extraction [source-url body]

  )
