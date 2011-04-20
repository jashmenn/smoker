
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
     (fn [acc tag] (concat acc (extract-tag-text tag source)))
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


  (import [java.util.regex Pattern Matcher])

  (let [p (Pattern/compile "(?i).*\b(us|our|me)\b.*")
        m (.matcher p "follow US on twitter!")]
    (.find m 0))

    ;; }
    ;; if (!regex.equals(lastRegex) || p == null) {
    ;;   lastRegex.set(regex);
    ;;   p = Pattern.compile(regex.toString());
    ;; }
    ;; Matcher m = p.matcher(s.toString());
    ;; result.set(m.find(0));
    ;; return result;
 

  )

