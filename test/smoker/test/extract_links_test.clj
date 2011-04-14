
(ns smoker.test.extract_links_test
  (:import 
   [org.apache.hadoop.io Text])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [smoker.udf.ExtractLinks :as extract])
  (:use [clojure.test]))

(def html-doc (slurp "test-resources/toy-pages/fake1.html"))

(deftest test-extracting-outlinks
  (let [links (set (extract/extract-links "http://a.com/" html-doc))]
    (is (contains? links ["Please don't crawl this" "http://a.com/secret/dont-crawl.html"]))
    (is (contains? links ["Crawl this instead" "http://a.com/public/do-crawl.html"]))))

;; (deftest test-extracting-outlinks
;;   (let [links (set (axy/evaluate (Text. "http://a.com/") (Text. html-doc)))]
;;     (is (include? links ["Please don't crawl this" "http://a.com/secret/dont-crawl.html"]))
;;     (is (include? links ["Crawl this instead" "http://a.com/public/do-crawl.html"]))))
