
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
   PrimitiveObjectInspectorFactory/javaStringObjectInspector
   PrimitiveObjectInspectorFactory/javaStringObjectInspector])

(def max-len 500)
(defn- both? [[a b c]] (and a b c))

(defn extract-tag-text [source-url tag source]
  (let [tags (.getAllElements source tag)]
    (->> tags
         (map 
          (fn [element] 
            [source-url
             tag 
             (nil-if-exception (truncate (.toString (.getContent element)) max-len))]))
         (filter both?))))

(defn extract-tags-text [source-url tags body]
  (let [source (Source. body)]
    (reduce 
     (fn [acc tag] (concat acc (extract-tag-text source-url tag source)))
     []
     (su/split tags #"\|"))))

(defn -operate [this fields]
  (let [[source-url tag body] (seq fields)]
    (if (and source-url tag body)
      (try 
        (extract-tags-text source-url tag body)
        (catch java.lang.RuntimeException e (prn "bad html"))
        (catch java.lang.StackOverflowError e (prn "bad html"))))))

(comment

  (def html-doc (slurp "test-resources/toy-pages/fake1.html"))
  (extract-tags-text "h1|h2" html-doc)


  (import [java.util.regex Pattern Matcher])

  (let [p (Pattern/compile "(?i).*\\b(us|our|me)\\b.*")
        m (.matcher p "follow US on twitter!")]
    (.find m 0))

  (let [p (Pattern/compile "(?i).*est(?:ablished)?[\\.:]?\\s*(\\d\\d\\d\\d).*")
        x (Pattern/compile "(?i).*est(?:ablished)?[\\.:]?\\s*(\\d\\d\\d\\d).*")
        m (.matcher x "we were est. 1987")
        found (.find m)
        mr (.toMatchResult m)]
    ;;(prn (.find m 0))
    (when found
      (.group mr 1)))


    ;; }
    ;; if (!regex.equals(lastRegex) || p == null) {
    ;;   lastRegex.set(regex);
    ;;   p = Pattern.compile(regex.toString());
    ;; }
    ;; Matcher m = p.matcher(s.toString());
    ;; result.set(m.find(0));
    ;; return result;
 

  )

