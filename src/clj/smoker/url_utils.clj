;;
;;; todo move these into the url normalization library or somewhere else
;;
(ns smoker.url-utils
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [url-normalizer.core :as norm]
   [clojure.contrib.io :as io])
  (:use smoker.utils)
  (:import [java.net URI URL]))

(defn as-url
  "takes urlish and attempts to make it a valid URL"
  [urlish]
  (let [nospaces (su/replace (str urlish) #"\s" "%20")
        url (io/as-url nospaces)]
    url))

(defn convert-url-to-uri [url]
  (norm/to-uri (as-url url)))

(defn ensure-ends-with 
  "ensure s ends in char"
  [s char]
  (if (re-find (java.util.regex.Pattern/compile (str char "$")) s) 
               s (str s char)))

(defn url-encode-href [str]
  (su/replace str #" " "%20"))
(defn fix-invalid-fragment [str]
  (re-sub-all-but-last #"#" "%23" str))
(defn preprocess-href [href]
  (-> href
   (fix-invalid-fragment)
   (url-encode-href)))


(defn #^URI resolve-href
"Given URL and HREF resolves the HREF, possibly relative to that URL.
Returns a URI

Examples:
source-url   href     -> url
http://a.com foo.html -> http://a.com/foo.html
http://a.com http://www.google.com -> http://www.google.com

Note: this is needed becauyse the default URI .resolve is unintuitive see:

;; (.resolve (URI. \"http://a.com\") \"foo.html\")
;;  -> #<URI http://a.comfoo.html>
"
  [source-url href]
  (let [surl (ensure-ends-with source-url "/")]
     (.resolve (convert-url-to-uri surl)
               (preprocess-href href))))

(defn href-to-url [href url]
  (let [href (su/trim href)
        uri #^URI (resolve-href url href)]
    (if (.getHost uri)
      (norm/canonicalize-url uri)
      nil)))


