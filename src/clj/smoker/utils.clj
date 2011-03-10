
(ns smoker.utils
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.io :as io]))

(defn as-url
  "takes urlish and attempts to make it a valid URL"
  [urlish]
  (let [nospaces (su/replace (str urlish) #"\s" "%20")
        url (io/as-url nospaces)]
    url))


