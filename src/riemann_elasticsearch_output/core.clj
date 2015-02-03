(ns riemann-elasticsearch-output.core
	(:require [clojurewerkz.elastisch.rest :as esr]
		[clojurewerkz.elastisch.rest.bulk :as esrb]
		[clj-time.core :as t]
		[clj-time.coerce :as tc]
		[clj-time.format :as tf]
		[riemann.config :refer :all]
		[riemann.streams :refer :all]
                [clojure.tools.logging :refer [warn error info infof debug debugf spy]]))

(def logstash-index-time-format (tf/formatter "yyyy.MM.dd"))

(defn logstash-time-index
	([event] (logstash-time-index "logstash-" event))
	([prefix event]
           (let [time (tc/from-long (long (* (:time event) 1000)))
                 time-str (tf/unparse logstash-index-time-format time)]
             (str prefix time-str))))

(def iso8601 (tf/formatters :date-time))

(defn logstash-v1-format [event]
  (merge (dissoc event :time :attributes)
         (:attributes event)
         {"@timestamp" (tf/unparse iso8601 (tc/from-long (long (* (:time event) 1000))))
          "@version" "1"
          }))

(defn- bulk-msg [idx-fn type-fn fmt-fn doc]
  (merge (fmt-fn doc) {:_index (idx-fn doc) :_type (type-fn doc)}))

(defn elasticsearch-sync [{:keys [url opts index-fn type-fn format-fn es-opts]
                           :or {type-fn #(str (:host %) "-" (:service %)) index-fn logstash-time-index format-fn logstash-v1-format}}]
  {:pre [(not (nil? url))
         (every? ifn? '(index-fn format-fn type-fn))]}
  (let [es (esr/connect url es-opts)
        doc-fn (partial bulk-msg index-fn type-fn format-fn)
        ]
    (info "Created Elasticsearch connection object")
    (fn [events]
      (spy events)
      (let [docs (map doc-fn events)]
        (debugf "Sending bulk index request to Elasticsearch with %d docs" (count docs))
        (try
          (esrb/bulk es
                     (esrb/bulk-index docs))
          (catch Exception e
            (warn "Bulk index to Elasticsearch failed" e)
            (spy docs)))))))
