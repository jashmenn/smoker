(defproject com.atti.wdm/smoker "1.0.1-SNAPSHOT"
  :description "When you visit your Hive, bring your smoker and you'll get stung less"
  :dependencies 
  [[org.clojure/clojure "1.2.0"]
   [org.clojure/clojure-contrib "1.2.0"]
   [url-normalizer "0.0.4"]
   [net.htmlparser.jericho/jericho-html "3.1"]
   [clj-html-parser "0.1.6"]
   [commons-logging "1.0.4"]]
  :dev-dependencies
  [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
   [lein-run "1.0.0"]
   [lein-javac "1.2.1-SNAPSHOT"]
   [swank-clojure "1.3.0-SNAPSHOT"]
   [robert/hooke "1.0.2"]
   [lein-daemon "0.2.1"]

   ;; start hive deps - include these deps in your project if you want
   ;; to use the hive functionality. they wont be in the uberjar
   [hive/hive-common "0.5.0"]
   [hive/hive-cli "0.5.0"]
   [hive/hive-exec "0.5.0"]
   [hive/hive-hwi "0.5.0"]
   [hive/hive-metastore "0.5.0"]
   [hive/hive-service "0.5.0"]
   [hive/hive-shims "0.5.0"]
   [hive/hive-serde "0.5.0"]
   [hive/hive-jdbc "0.5.0"]
   [commons-lang/commons-lang "2.5"]
   [jline "0.9.94"]
   [org.antlr/antlr "3.0.1"]
   [fb/libfb303 "3.0.3"]
   [org.datanucleus/datanucleus-core "1.1.2"]
   [org.datanucleus/datanucleus-rdbms "1.1.2"]
   [org.datanucleus/datanucleus-connectionpool "1.0.2"]
   [commons-pool "1.2"]
   [commons-dbcp "1.2.2"]
   [commons-collections "3.2.1"]
   [javax.jdo/jdo2-api "2.3-ea"]
   [javax.jdo/jdo2-api "2.3-eb"]
   [mysql/mysql-connector-java "5.0.2"]
   ;; end hive deps
   ]
  :repositories
    {"clojars" "http://clojars.org/repo"
     "yp"      "http://maven.corp.atti.com:9999/nexus/content/groups/public"
     "YP-3p" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/thirdparty"
     "YP-releases" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/releases"
     "YP-snap" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/snapshots"
     }
  :compile-path "build/classes"
  :target-dir "build"
  :java-source-path "src/java"
  :source-path "src/clj"
  :aot :all
  )


;; tmp
;; export HADOOP_HOME=~/projects/atti-configs/qbert-hadoop; export HIVE_HOME=~/projects/atti-configs/qbert-hive; export HIVE_CONF_DIR=$HIVE_HOME/conf
;; lein clean && lein javac && lein jar && java -cp build/smoker-1.0.1-SNAPSHOT.jar:`cat classpath`:$HIVE_CONF_DIR smoker.QueryRunner

