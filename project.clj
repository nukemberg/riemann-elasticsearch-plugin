(defproject riemann-elasticsearch-output "0.1.0-SNAPSHOT"
  :description "ElasticSearch output plugin for Riemann"
  :url "https://github.com/avishai-ish-shalom/riemann-elasticsearch-output"
  :license {:name "Apache v2"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clojurewerkz/elastisch "2.1.0"]]
  :plugins [[codox "0.6.1"]
            [lein-midje "3.1.3"]]
  :profiles {:dev {:dependencies [[clj-time "0.9.0"]
                                  [riemann "0.2.8"]
                                  [midje "1.6.3"]]}}
  :codox {:src-linenum-anchor-prefix "L"
          :src-dir-uri "https://github.com/avishai-ish-shalom/riemann-elasticsearch-plugin/blob/master"})
