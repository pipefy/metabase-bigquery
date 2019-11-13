(defproject metabase/bigquery-alt-driver "0.0.1-SNAPSHOT-1.27.0"
  :min-lein-version "2.5.0"

  :aliases
  {"install-for-building-drivers" ["with-profile" "+install-for-building-drivers" "install"]}

  :dependencies
  [[com.google.apis/google-api-services-bigquery "v2-rev20181202-1.27.0"]]

  :profiles
  {:provided
   {:dependencies
    [[metabase-core "1.0.0-SNAPSHOT"]
     [metabase/google-driver "1.0.0-SNAPSHOT-1.27.0"]]}

   :install-for-building-drivers
   {:auto-clean true
    :aot        :all}


   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "bigquery-alt.metabase-driver.jar"}})
