
(ns smoker.udf.ExtractTagAttributesText
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
   PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector])

(def max-len 500)

(defn extract-attribute-text [element]
  (su/join " "
   (map #(.getValue %)
    (iterator-seq (.iterator (.getAttributes element))))))

(defn extract-tag-attribute-text [tag wanted source]
  (let [tags (.getAllElements source tag)]
    (->> tags
         (map 
          (fn [element] 
            [tag 
             (nil-if-exception 
              (truncate (if (not (empty? wanted))
                          (.getAttributeValue element wanted) "") max-len))
             (nil-if-exception
              (truncate (extract-attribute-text element) max-len))])))))

(defn extract-tags-attribute-text [tags wanted body]
  (let [source (Source. body)]
    (reduce 
     (fn [acc tag] (concat acc (extract-tag-attribute-text tag wanted source)))
     []
     (su/split tags #"\|"))))

(defn -operate [this fields]
  (let [[tag wanted body] (seq fields)]
    (if (and tag wanted body)
      (try 
        (extract-tags-attribute-text tag wanted body)
        (catch java.lang.RuntimeException e (prn "bad html"))
        (catch java.lang.StackOverflowError e (prn "bad html"))))))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-tags-attribute-text "h1|h2" "" html-doc)
  (extract-tags-attribute-text "img" "src" html-doc)

  (import [java.util.regex Pattern Matcher])

  (let [p (Pattern/compile "(?i).*\\b(us|our|me)\\b.*")
        m (.matcher p "follow US on twitter!")]
    (.find m 0))

  )

