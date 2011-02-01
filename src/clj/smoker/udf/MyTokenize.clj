
(ns smoker.udf.MyTokenize
  (:import [smoker.udf ClojureUDTF])
  (:import [java.util ArrayList List])
  (:import [org.apache.hadoop.hive.ql.exec UDFArgumentException])
  (:import [org.apache.hadoop.hive.ql.metadata HiveException])
  (:import [org.apache.hadoop.hive.ql.udf.generic GenericUDTF])
  (:import [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspector ObjectInspectorFactory PrimitiveObjectInspector
            StructObjectInspector])
  (:import [org.apache.hadoop.hive.serde2.objectinspector.primitive 
            PrimitiveObjectInspectorFactory])
  (:import [org.apache.hadoop.hive.ql.exec UDF])
  (:import [org.apache.hadoop.io Text])
  (:import [java.util Date])
  (:require [clojure.contrib.str-utils2 :as su]))

(gen-class
 :name smoker.udf.MyTokenize
 :extends smoker.udf.ClojureUDTF
 :init init
 :constructors {[] []}
 :state state
 )

(defn -init []
  [[] (ref {:stringIO nil})])

(defn -initialize [this args]
  (if (not (= (count args) 1))
   (throw 
      (UDFArgumentException. "tokenize() takes exactly one argument")))
  (dosync (alter (.state this) assoc :stringIO (nth args 0)))
  (let [fieldNames (ArrayList. ["word" "count"])
        fieldIOs (ArrayList. 
          [PrimitiveObjectInspectorFactory/javaStringObjectInspector
          PrimitiveObjectInspectorFactory/javaIntObjectInspector])]
    (ObjectInspectorFactory/getStandardStructObjectInspector 
       fieldNames fieldIOs)))

(defn tokenize [line]
  (map 
   (fn [token] [token (Integer/valueOf 1)]) 
   (su/split line #"\s+")))

(defn -process [this record]
  (if-let [document (.getPrimitiveJavaObject 
                     (@(.state this) :stringIO) (nth record 0))]
    (doall (map (fn [results] 
                  (.emit this (into-array Object results))) 
                (tokenize document)))))

(defn -close [this])

