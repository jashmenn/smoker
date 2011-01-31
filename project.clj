(defproject smoker "1.0.0-SNAPSHOT"
  :description "When you visit your Hive, bring your smoker and you'll get stung less"
  :dependencies 
  [[org.clojure/clojure "1.2.0"]
   [org.clojure/clojure-contrib "1.2.0"]
   
   [commons-lang "2.5"]
   [commons-logging "1.0.4"]
   [commons-io "2.0"]
   [log4j "1.2.15" :exclusions
    [javax.mail/mail
     javax.jms/jms
     com.sun.jdmk/jmxtools
     com.sun.jmx/jmxri]]]
  :dev-dependencies
  [[org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions
    [log4j
     commons-httpclient
     commons-logging
     javax.servlet/servlet-api
     org.apache.ant/ant
     org.apache.ant/ant-launcher
     org.eclipse.jdt/core
     org.mortbay.jetty/jetty
     org.mortbay.jetty/jsp-2.1
     org.mortbay.jetty/jsp-api-2.1
     org.mortbay.jetty/servlet-api-2.5
     org.slf4j/slf4j-api
     org.slf4j/slf4j-log4j12]]
   [hive/hive-common "0.5.0"]
   [hive/hive-cli "0.5.0"]
   [hive/hive-exec "0.5.0"]
   [hive/hive-hwi "0.5.0"]
   [hive/hive-metastore "0.5.0"]
   [hive/hive-service "0.5.0"]
   [hive/hive-shims "0.5.0"]
   [hive/hive-serde "0.5.0"]
   [lein-run "1.0.0"]
   [lein-javac "1.2.1-SNAPSHOT"]
   [swank-clojure "1.3.0-SNAPSHOT"]
   [robert/hooke "1.0.2"]
   [lein-daemon "0.2.1"]]
  :repositories
    {"clojars" "http://clojars.org/repo"
     "yp"      "http://maven.corp.atti.com:9999/nexus/content/groups/public"
     "YP-3p" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/thirdparty"
     "YP-releases" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/releases"
     "YP-snap" "http://svn2.wc1.yellowpages.com:9999/nexus/content/repositories/snapshots"
     }
  :compile-path "build/classes"
  :target-dir "build"
  :java-source-path "src"
  )
