
(ns smoker.udf.MyTokenize
  (:import [java.util ArrayList List])
  (:import [org.apache.hadoop.hive.ql.exec UDFArgumentException])
  (:import [org.apache.hadoop.hive.ql.metadata.HiveException])
  (:import [org.apache.hadoop.hive.ql.udf.generic.GenericUDTF])
  (:import [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspector ObjectInspectorFactory PrimitiveObjectInspector
            StructObjectInspector])
  (:import [org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory])
  (:import [org.apache.hadoop.hive.ql.exec UDF])
  (:import [org.apache.hadoop.io Text])
  (:require [clojure.contrib.str-utils2 :as su]))

(defn array-form-of 
  "given Class klass returns the corresponding java primitive array Class.
e.g. the Class `java.lang.String` becomes the Class `[Ljava.lang.String;`"
  [klass]
  (let [as-str (pr-str (class (into-array klass [])))]
    (Class/forName as-str)))

(gen-class
   :name smoker.udf.MyTokenize
   :extends GenericUDTF
   :init init
   :constructors {[] []}
   :state state
   :methods [[initialize [PrimitiveObjectInspector] org.apache.hadoop.io.Text]]
   )

(defn -init []
  [[] (ref {:stringIO nil})])

