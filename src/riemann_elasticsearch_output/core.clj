(ns riemann-elasticsearch-output.core
	(:require [clojurewerkz.elastisch.rest :as esr]
                  [clojurewerkz.elastisch.rest.bulk :as esrb]
                  [riemann.common :refer [unix-to-iso8601]]
		[clj-time.core :as t]
		[clj-time.coerce :as tc]
		[clj-time.format :as tf]
		[riemann.config :refer :all]
		[riemann.streams :refer :all]
                [clojure.tools.logging :refer [warn error info infof debug debugf spy]]))

(def logstash-index-time-format (tf/formatter "yyyy.MM.dd"))

(defn logstash-time-index
  "Return a time based index name with optional prefix. Default prefix is 'logstash-'. E.g.:

(logstash-time-index {:time 1423006857023 :host \"localhost\" :service \"app-health\" :metric 23}) ==> \"logstash-2015.02.03\"
(logstash-time-index \"riemann_\" {:time 1423006857023 :host \"localhost\" :service \"app-health\" :metric 23}) ==> \"riemann_2015.02.03\"
"
  ([event] (logstash-time-index "logstash-" event))
  ([prefix event]
     (let [time (tc/from-long (long (* (:time event) 1000)))
           time-str (tf/unparse logstash-index-time-format time)]
       (str prefix time-str))))

(defn logstash-v1-format
  "Convert an event to a Logstash V1 format compatible document"
  [event]
  (merge (dissoc event :time :attributes)
         (:attributes event)
         {"@timestamp" (unix-to-iso8601 (:time event))
          "@version" "1"
          }))

(defn- bulk-msg [idx-fn type-fn fmt-fn doc]
  (merge (fmt-fn doc) {:_index (idx-fn doc) :_type (type-fn doc)}))

(defn elasticsearch-sync
  "Create a new syncronouos Elasticsearch output stream.

:type-fn - a function which extracts document type from event
:index-fn - a function which returns index name from event
:format-fn - a function which converts an event to document to be indexed. A document is map object.
:url - The ElasticSearch REST API URL
:es-opts - ElasticSearch connection options. Refer to http://reference.clojureelasticsearch.info/clojurewerkz.elastisch.rest.html#var-connect for more info
"
  [{:keys [url index-fn type-fn format-fn es-opts]
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
            (warn e "Bulk index to Elasticsearch failed")
            (spy docs)))))))
