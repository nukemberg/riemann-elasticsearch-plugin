(ns riemann-elasticsearch-output.core-test
  (:require [midje.sweet :refer :all]
            [midje.util :refer [expose-testables]]
            [riemann-elasticsearch-output.core :refer :all]
            [clj-time.core :as t]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esrb]
            ))

(expose-testables riemann-elasticsearch-output.core)

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

(facts "about `index-bulk!`"
       (fact "return :ok when bulk index is ok"
             (index-bulk! ..es.. [..doc..]) => :ok
             (provided
              (esrb/bulk ..es.. anything) => nil
              (esrb/bulk-index [..doc..]) => nil))
       (fact "return :retry on retryable error"
             (index-bulk! ..es.. [..doc..]) => :retry
             (provided
              (esrb/bulk-index [..doc..]) =throws=> (java.net.ConnectException. )
              ))
       (fact "return :error on non-retryable errors"
             (index-bulk! ..es.. [..doc..]) => :error
             (provided
              (esrb/bulk-index [..doc..]) =throws=> (Exception.))))

(facts "about `retry`"
       (fact "doesn't sleep when fn returns ok"
             (retry 3 ..sleep.. --fn--) => :ok
             (provided
              (--fn--) =streams=> [:ok :retry :retry] :times 1))
       (fact "retry n times on retryable error"
             (retry 2 ..sleep.. --fn--) => :retry
             (provided
              (#'riemann-elasticsearch-output.core/sleep ..sleep..) => nil :times 2
              (--fn--) =streams=> [:retry :retry :retry :retry] :times 3))
       (fact "stop retrying if unretryable error"
             (retry 5 ..sleep.. --fn--) => :error
             (provided
              (#'riemann-elasticsearch-output.core/sleep ..sleep..) => nil :times 2
              (--fn--) =streams=> [:retry :retry :error :retry :retry] :times 3))
       (fact "when last attempt succeeds return :ok"
             (retry 5 ..sleep.. --fn--) => :ok
             (provided
              (#'riemann-elasticsearch-output.core/sleep ..sleep..) => nil :times 2
              (--fn--) =streams=> [:retry :retry :ok] :times 3)))
