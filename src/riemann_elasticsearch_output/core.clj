(ns riemann-elasticsearch-output.core
	(:require [clojurewerkz.elastisch.rest :as esr]
                  [clojurewerkz.elastisch.rest.bulk :as esrb]
                  [riemann.common :refer [unix-to-iso8601]]
		[clj-time.core :as t]
		[clj-time.coerce :as tc]
		[clj-time.format :as tf]
		[riemann.config :refer :all]
		[riemann.streams :refer :all]
                [clojure.tools.logging :refer [warn warnf error info infof debug debugf spy]]))

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

(defn- valid-bulk-msg? [msg]
  (every? (comp not nil?) (vals (select-keys msg [:_type :_index]))))

(defn- ^{:testable true} index-bulk! [es docs]
  (try
    (esrb/bulk es (esrb/bulk-index docs))
    :ok
    (catch java.net.ConnectException e
      (warn "Couldn't connect to ElasticSearch, retrying in 1 sec")
      (Thread/sleep 1000)
      :retry)
    (catch clojure.lang.ExceptionInfo e
      (case (get-in (ex-data e) [:object :status])
        400 (warn e "Bulk index failed, perhaps a document is malformed?"))
      :error)
    (catch Exception e
      (warn e "Bulk index to Elasticsearch failed")
      (spy docs)
      :error)))

; this function is needed so we can rebind it during testing, since Thread/sleep can't be easily mocked
(defn- sleep [n] (Thread/sleep n))

(defn- retry-forever [retry-interval func]
  (loop []
    (let [res (func)]
      (if (= :retry res)
        (do
          (sleep retry-interval)
          (recur))
        res))))

(defn- ^{:testable true} retry [retries retry-interval func]
  (if (<= retries 0)
    (retry-forever retry-interval func)
    (loop [retries retries]
      (let [res (func)]
        (if (and (= :retry res) (> retries 0))
          (do
            (sleep retry-interval)
            (recur (dec retries)))
          res)))))

(defn elasticsearch-sync
  "Create a new syncronouos Elasticsearch output stream.

:type-fn - a function which extracts document type from event
:index-fn - a function which returns index name from event
:format-fn - a function which converts an event to document to be indexed. A document is map object.
:url - The ElasticSearch REST API URL
:es-opts - ElasticSearch connection options. Refer to http://reference.clojureelasticsearch.info/clojurewerkz.elastisch.rest.html#var-connect for more info
:retries - the number of retries on retryable errors
:retry-interval - the number of milliseconds to wait before retrying
"
  [{:keys [url index-fn type-fn format-fn es-opts retries retry-interval]
    :or {type-fn #(str (:host %) "-" (:service %))
         index-fn logstash-time-index
         format-fn logstash-v1-format
         retries 3
         retry-interval 1000}
    }]
    {:pre [(not (nil? url))
           (every? ifn? '(index-fn format-fn type-fn))
           (integer? retries)
           (integer? retry-interval)
           (> retry-interval 0)]}

    (let [es (esr/connect url es-opts)
        doc-fn (partial bulk-msg index-fn type-fn format-fn)
        ]
    (info "Created Elasticsearch connection object")
    (fn [events]
      (spy events)
      (let [docs (map doc-fn events)
            [docs bad-docs] (split-with valid-bulk-msg? docs)]

        ; cowardly refuse to index malformed docs
        (when-not (empty? bad-docs)
          (warnf "%d malformed messages recieved, skipping" (count bad-docs))
          (spy bad-docs))

        (when-not (empty? docs)
          (debugf "Sending bulk index request to Elasticsearch with %d docs" (count docs))
          (case (retry retries retry-interval (partial index-bulk! es docs))
            :ok nil
            :error (warn "Indexing failed due to unretryable error")
            nil (warn "Indexing failed after %d retries" retries)))))))
