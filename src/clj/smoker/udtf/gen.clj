
(ns smoker.udtf.gen
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

(defmacro gen-udtf
  "Creates a UDTF that takes exactly one argument and can return 0 or more tuples.
You also need to call gen-wrapper-methods."
  []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    `(do 
       (gen-class
        :name ~the-name
        :extends smoker.udf.ClojureUDTF
        :init "init"
        :constructors {[] []}
        :methods [["operate" [Object] clojure.lang.ISeq]]
        :state "state"
        ))))

(defn -init []
  [[] (ref {:stringIO nil})])

(defn build-initialize [emits]
  (fn [this args]
    (if (not (= (count args) 1))
      (throw 
       (UDFArgumentException. (str *ns* " takes exactly one argument"))))
    (dosync (alter (.state this) assoc :stringIO (nth args 0)))
    (let [fieldNames (ArrayList. (map str (range 0 (count emits))))
          fieldIOs (ArrayList. emits)]
      (ObjectInspectorFactory/getStandardStructObjectInspector 
       fieldNames fieldIOs))))

(defn -close [this])

(defn -process [this record]
  (if-let [document (.getPrimitiveJavaObject 
                     (@(.state this) :stringIO) (nth record 0))]
    (doall (map (fn [results] 
                  (.emit this (into-array Object results))) 
                (.operate this document)))))

(defn gen-wrapper-methods
  "Generates the methods needed to use your UDTF. 

For now, use the PrimitiveObjectInspectorFactory to specify the types you'd plan on returning (syntax sugar to come eventually). Example:

(gen/gen-wrapper-methods 
 [PrimitiveObjectInspectorFactory/javaStringObjectInspector
  PrimitiveObjectInspectorFactory/javaIntObjectInspector])

Will allow you to return a tuple of (String, int)

Now you need to write an -operate method that accepts [this line] and returns a seq of tuples that match your types. In the above case, we could return:

    [[\"hi\" 1] [\"bye\" 2]]

"
  [emits]
  (intern *ns* '-init -init)
  (intern *ns* '-initialize (build-initialize emits))
  (intern *ns* '-close -close)
  (intern *ns* '-process -process))

