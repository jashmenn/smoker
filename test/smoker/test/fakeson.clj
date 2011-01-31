
(ns smoker.test.fakeson
  (:import [smoker FakesonParser])
  (:require [clojure.contrib.str-utils2 :as su])
  (:use [clojure.test]))

(def test-file "test-resources/test-url-fields.txt")

(deftest test-that-you-can-parse-fakeson 
  (is (= 26 (reduce 
         (fn [sum line]
           (+ sum (count (seq (FakesonParser/urlStringsFromWhateverThisCrapIs line))))
           ) 0 (su/split-lines (slurp test-file))))))

(comment

  (test-that-you-can-parse-fakeson)

  ()
  )
