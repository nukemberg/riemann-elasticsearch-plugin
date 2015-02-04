(ns riemann-elasticsearch-output.core-test
  (:require [midje.sweet :refer :all]
            [riemann-elasticsearch-output.core :refer :all]
            [clj-time.core :as t]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esrb]
            ))

(fact "about `logstash-time-index"
      (let [event {:service ...service... :host ...host... :time 1423033893}]
        (logstash-time-index event) => "logstash-2015.02.04"
        (logstash-time-index "PREFIX_" event) => "PREFIX_2015.02.04"))

(fact "about `logstash-v1-format`"
      (let [attributes {:k :v}
            event {:service :service :host :host :time 1423033893 :attributes attributes}]
        (logstash-v1-format event) => {"@version" "1" "@timestamp" "2015-02-04T07:11:33.000Z" :k :v :host :host :service :service}))

(fact "about `elasticsearch-sync"
      ((elasticsearch-sync {:es-opts ..es-opts.. :url ..url.. :type-fn --type-- :index-fn --idx-- :format-fn --fmt--}) [..e..]) => nil
      (provided
       (#'riemann-elasticsearch-output.core/bulk-msg --idx-- --type-- --fmt-- ..e..) => ..doc..
       (esrb/bulk ..es.. anything) => nil
       (esrb/bulk-index [..doc..]) => nil
       (esr/connect ..url.. ..es-opts..) => ..es..))
