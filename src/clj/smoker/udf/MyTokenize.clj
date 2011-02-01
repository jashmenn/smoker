
(ns smoker.udf.MyTokenize
  (:import [java.util ArrayList List])
  (:import [org.apache.hadoop.hive.ql.exec UDFArgumentException])
  (:import [org.apache.hadoop.hive.ql.metadata.HiveException])
  (:import [org.apache.hadoop.hive.ql.udf.generic.GenericUDTF])
  (:import [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspector ObjectInspectorFactory PrimitiveObjectInspector
            StructObjectInspector])
  (:import [org.apache.hadoop.hive.serde2.objectinspector.primitive 
            PrimitiveObjectInspectorFactory])
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
   :methods [[initialize [(array-form-of PrimitiveObjectInspector)] 
                         StructObjectInspector]
             [process [(array-form-of Object)]]
             [close []]]
   )

(defn -init []
  [[] (ref {:stringIO nil})])

(defn -initialize [this args]
  (if (not (= (count args) 1))
    (throw (UDFArgumentException. "tokenize() takes exactly one argument")))
  (dosync (alter (.state this) assoc :stringIO (nth args 0)))
  (let [fieldNames (ArrayList. ["word" "count"])
        fieldIOs (ArrayList. [PrimitiveObjectInspectorFactory/javaStringObjectInspector
                              PrimitiveObjectInspectorFactory/javaIntObjectInspector])]
    (ObjectInspectorFactory/getStandardStructObjectInspector fieldNames fieldIOs)))

(defn -process [this record]
  (if-let [document (.getPrimitiveJavaObject @(.state this) (nth record 0))]
   ; to be continued 
    ))

(defn -close [this])



;;   private PrimitiveObjectInspector stringOI = null;

;;   @Override
;;   public StructObjectInspector initialize(ObjectInspector[] args)
;;   throws UDFArgumentException {
;;     if (args.length != 1) {
;;       throw new UDFArgumentException("tokenize() takes exactly one argument");
;;     }

;;     if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
;;         && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
;;       throw new UDFArgumentException("tokenize() takes a string as a parameter");
;;     }

;;     stringOI = (PrimitiveObjectInspector) args[0];

;;     List<String> fieldNames = new ArrayList<String>(2);
;;     List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
;;     fieldNames.add("word");
;;     fieldNames.add("cnt");
;;     fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
;;     fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
;;     return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
;;   }

;;   @Override
;;   public void process(Object[] record) throws HiveException {
;;     final String document = (String) stringOI.getPrimitiveJavaObject(record[0]);

;;     if (document == null) {
;;       return;
;;     }
;;     String[] tokens = document.split("\\s+");
;;     for (String token : tokens) {
;;       forward(new Object[] { token, Integer.valueOf(1) });
;;     }
;;   }

;;   @Override
;;   public void close() throws HiveException {
;;     // do nothing
;;   }
;; }
