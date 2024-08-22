(ns db-migrator.config
  (:gen-class)
  (:require
   [yaml.core :as yaml]
   [taoensso.timbre :as log]))

(def config-file
  (yaml/from-file "resources/config.yml"))

(def loglevel
  (if (nil? (:log-level config-file))
    :info
    (keyword (:log-level config-file))))

(log/report "Log level set to" (name (:min-level (log/set-level! loglevel))))

(def max-parameters-value Short/MAX_VALUE)

