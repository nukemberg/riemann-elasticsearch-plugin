# riemann-elasticsearch-output


[![Build Status](https://travis-ci.org/avishai-ish-shalom/riemann-elasticsearch-plugin.svg?branch=master)](https://travis-ci.org/avishai-ish-shalom/riemann-elasticsearch-plugin)

A riemann Elasticsearch output plugin. This plugin is usefull if you want to index events in ElasticSearch.

## Usage

In your riemann.config

```clojure

(load-plugins) ; will load plugins from the classpath
; or
(load-plugin "elasticsearch")

(let [elasticsearch (batch 500 2
                      (async-queue! :elasticsearch {:queue-size 1e3 :core-pool-size 4 :max-pool-size 4}
                        (elasticsearch/elasticsearch-sync {:url "http://localhost:9200" :type-fn :type})))
      ]
  (streams      
    (with {:type "riemann-event"}
      elasticsearch)))
```

## Installing

You will need to build this module for now and push it on riemann's classpath, for this
you will need a working JDK, JRE and [leiningen](http://leiningen.org).

First build the project:

```
lein uberjar
```

The resulting artifact will be in `target/riemann-elasticsearch-output-standalone-0.0.1.jar`.
You will need to push that jar on the machine(s) where riemann runs, for instance, in
`/usr/lib/riemann/riemann-elasticsearch-output.jar`.

If you have installed riemann from a stock package you will only need to tweak
`/etc/default/riemann` or `/etc/sysconfig/riemann` and change
the line `EXTRA_CLASSPATH` to read:

```
EXTRA_CLASSPATH=/usr/lib/riemann/riemann-elasticsearch-output.jar
```

You can then use exposed functions, provided you have loaded the plugin in your configuration.

## License

Copyright © 2015 Avishai Ish-Shalom

Distributed under the Apache V2 License
