
(ns smoker.udtf.gen
  (:import 
   [smoker.udf ClojureUDTF]
   [java.util ArrayList List]
   [org.apache.hadoop.hive.ql.exec UDFArgumentException]
   [org.apache.hadoop.hive.ql.metadata HiveException]
   [org.apache.hadoop.hive.ql.udf.generic GenericUDTF]
   [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspector ObjectInspectorFactory PrimitiveObjectInspector
            StructObjectInspector]
   [org.apache.hadoop.hive.serde2.objectinspector.primitive 
              PrimitiveObjectInspectorFactory]
   [org.apache.hadoop.hive.ql.exec UDF]
   [org.apache.hadoop.io Text]
   [java.util Date])
  (:require 
   [clojure.contrib.str-utils2 :as su]
   [clojure.contrib.seq-utils :as sequ]))


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
  [[] (atom [])])

(defn build-initialize [emits]
  (fn [this args]
    ;; (if (not (= (count args) (count emits)))
    ;;   (throw 
    ;;    (UDFArgumentException. 
    ;;    (str *ns* " takes exactly " (count emits) " arguments"))))
    (swap! (.state this) (fn [_] args))
    (let [fieldNames (ArrayList. (map str (range 0 (count emits))))
          fieldIOs (ArrayList. emits)]
      (ObjectInspectorFactory/getStandardStructObjectInspector 
       fieldNames fieldIOs))))

(defn -close [this])

(defn -process [this record]
  (let [primitives 
        (map 
         (fn [[i thingy]]
           (.getPrimitiveJavaObject thingy (nth record i)))
         (sequ/indexed @(.state this)))]
    (doall 
     (map 
      (fn [results] 
        (.emit this (into-array Object results))) 
      (.operate this primitives)))))


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

