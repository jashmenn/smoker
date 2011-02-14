
(ns smoker.test.urls
  (:import [smoker.udf RobotsURL]
           [smoker.udf NormalizeURL]
           [org.apache.hadoop.io Text])
  (:require [clojure.contrib.str-utils2 :as su]
            [smoker.udf.RobotsURL :as robo]
            [smoker.udf.NormalizeURL :as norm])
  (:use [clojure.test]))

(def robots-tests 
     [["http://a.com" "http://a.com/robots.txt"]
      ["http://b.com/foo/bar/baz.html" "http://b.com/robots.txt"]])

(deftest test-robots
  (doseq [[question answer] robots-tests]
    (is (= (Text. answer) (robo/evaluate (Text. question)))))
  (is (nil? (robo/evaluate (Text. "asdf")))))


(def normal-tests 
     [["http://natemurray.com"      "http://natemurray.com/"]
      ["http://natemurray.com:80"   "http://natemurray.com/"]
      ["http://natemurray.com/#bla" "http://natemurray.com/"]])

(deftest test-normalize
  (doseq [[question answer] normal-tests]
    (is (= (Text. answer) (norm/evaluate (Text. question)))))
  (is (nil? (norm/evaluate (Text. "asdf")))))


(comment

 (test-robots)
 (test-normalize)

  )
