(defproject riemann-elasticsearch-output "0.1.1-SNAPSHOT"
  :description "ElasticSearch output plugin for Riemann"
  :url "https://github.com/avishai-ish-shalom/riemann-elasticsearch-output"
  :license {:name "Apache v2"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[clojurewerkz/elastisch "2.2.0-beta2" :exclusions [clj-http org.antlr/antlr-runtime potemkin cheshire]]]
  :plugins [[codox "0.6.1"]
            [lein-midje "3.1.3"]]
  :profiles {:dev {:dependencies [[riemann "0.2.10"]
                                  [midje "1.6.3"]]}}
  :codox {:src-linenum-anchor-prefix "L"
          :src-dir-uri "https://github.com/avishai-ish-shalom/riemann-elasticsearch-plugin/blob/master"})
