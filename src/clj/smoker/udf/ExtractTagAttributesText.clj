
(ns smoker.udf.ExtractTagAttributesText
  (:import 
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
    PrimitiveObjectInspectorFactory])
  (:use 
   [smoker.utils])
  (:require 
   [smoker.udtf.gen :as gen]
   [clojure.contrib.string :as string]
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
   PrimitiveObjectInspectorFactory/javaIntObjectInspector
   ])

(def max-len 500)

(defn extract-attribute-text [attributes]
  (su/join " "
   (map #(.getValue %)
    (iterator-seq (.iterator attributes)))))

(defn extract-tag-attribute-text-streaming [tag wanted body]
  (->> (map (fn [element i] [element i]) 
            (iterator-seq (.iterator (StreamedSource. body)))
            (iterate inc 0))
       (filter (fn [[seg i]]
                 (if (and (= (type seg) StartTag)
                          (= (string/lower-case tag) 
                             (string/lower-case (.getName seg))))
                   true)))
       (map 
          (fn [[seg i]] 
            (let [attributes (.parseAttributes seg)] 
              [tag 
              (if (not (empty? wanted)) ;; attribute-wanted
                (nil-if-exception
                  (truncate 
                    (.getValue attributes wanted) max-len))
                "")
              (nil-if-exception ;; attribute text 
               (truncate 
                (extract-attribute-text attributes) max-len))
               i])))))

(defn extract-tags-attribute-text [tags wanted body]
  (let [source (Source. body)]
    (reduce 
     (fn [acc tag] 
       (concat acc 
               (extract-tag-attribute-text-streaming tag wanted body)))
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

