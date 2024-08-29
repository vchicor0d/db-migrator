(ns db-migrator.database
  (:gen-class)
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [honey.sql :as sql]
   [honey.sql.helpers :as sqlh]
   [taoensso.timbre :as log]
   [db-migrator.config :as config]))

(def queryoptions
  {:builder-fn rs/as-unqualified-lower-maps})

(def source
  {:dbtype (:dbtype (:source config/config-file))
   :dbname (:dbname (:source config/config-file))
   :user (:user (:source config/config-file))
   :password (:password (:source config/config-file))
   :host (:host (:source config/config-file))
   :port (:port (:source config/config-file))
   :ssl (:ssl (:source config/config-file))
   :sslmode (:sslmode (:source config/config-file))})

(def destination
  {:dbtype (:dbtype (:destination config/config-file))
   :dbname (:dbname (:destination config/config-file))
   :user (:user (:destination config/config-file))
   :password (:password (:destination config/config-file))
   :host (:host (:destination config/config-file))
   :port (:port (:destination config/config-file))
   :ssl (:ssl (:destination config/config-file))
   :sslmode (:sslmode (:destination config/config-file))})

(def limit (let [config-limit (:limit (:db config/config-file))]
             (if (nil? config-limit) 10000 config-limit)))

(def source-datasource
  (jdbc/get-datasource source))

(def destination-datasource
  (jdbc/get-datasource destination))

(defn- replace-*-with-key [query key]
  (str/replace query #"\*" key))

(defn where-in-clause
  "Returns a in where clause within a list or a subquery (replacing * with key)"
  ([field list]
   (str field " in (" (str/join "," list) ")"))
  ([field subquery key]
   (str field " in (" (replace-*-with-key subquery key) ")")))

(defn select-query
  "Obtains a select query"
  [table conditions]
  (let [query-without-conditions (-> (sqlh/select :*)
                                     (sqlh/from table)
                                     (sql/format))
        query (if (seq conditions)
                [(str (first query-without-conditions) " where " conditions)]
                query-without-conditions)]
    query))

(defn- insert-query
  "Obtains an insert query"
  [table rows]
  (log/debug "Creating query to insert" (count rows) "into" (name table))
  (let [query (-> (sqlh/insert-into table)
                  (sqlh/values (seq rows))
                  (sqlh/upsert (-> (sqlh/on-conflict)
                                   sqlh/do-nothing))
                  (sql/format))]
    (log/debug query)
    query))

(defn- execute-query!
  "Executes a jdbc query"
  [datasource query]
  (jdbc/execute! datasource
                 query
                 queryoptions))

(defn- split-select-query
  "Splits a select query into a list of queries"
  [query]
  (log/info "Trying to split select query in 2 parts and extract them separately")
  (let [matcher (re-matcher #"(.+ (?:FROM|from) itemsize where communicationkey in )\(([0-9, ]+)\)" query)
        found (re-find matcher)
        partial-query (get found 1)
        commkeys (str/split (get found 2) #",")
        end-parens (get found 3)
        first-half-commkeys (take (int (/ (count commkeys) 2)) commkeys)
        second-half-commkeys (take (int (/ (count commkeys) 2)) commkeys)
        first-query (str partial-query "(" (str/join "," first-half-commkeys) ")" end-parens)
        second-query (str partial-query "(" (str/join "," second-half-commkeys) ")" end-parens)]
    [first-query second-query]))

(defn- extract-resultset! [queries]
  (map #(try
          (execute-query! source-datasource [%])
          (catch Exception e
            (log/error "Error extracting resultset from query")
            (when (= :trace (:min-level log/*config*))
              (.printStackTrace e))
            (extract-resultset! (split-select-query (first %)))))
       queries))

(defn execute-select-query!
  "Executes a select query to the origin database"
  [query]
  (log/trace "Executing query:" query)
  (log/info "Extracting resultset from" (last (re-find (re-matcher #"(?:FROM|from) (\S+)" (first query)))))
  (let [result (apply concat (extract-resultset! query))]
    (log/debug "Found" (count result) "items")
    result))

(defn- split-parameters
  "Splits a list into pieces with the maximum value of elements specified"
  ([data]
   (split-parameters data config/max-parameters-value))
  ([data max-value]
   (log/debug "Splitting resultset into chunks of" (int max-value) "rows")
   (let [rest (set data)]
     (loop [splitted-data []
            rest rest]
       (if (<= (count rest) max-value)
         (conj splitted-data rest)
         (recur (conj splitted-data (take max-value rest))
                (drop max-value rest)))))))

(defn- max-parameters-by-field-number
  "Determines the max parameters value for a given number of fields"
  [field-number]
  (if (> field-number 0)
    (Math/floor (/ config/max-parameters-value field-number))
    config/max-parameters-value))

(defn- execute-insert-query-rowblock!
  "Executes a insert query to the destination database"
  [table resultset]
  (log/info "Inserting resultset into" (name table) "table in chunks of" (count (first resultset)) "rows.")
  (doseq [rowblock resultset]
    (log/debug "Inserting chunk of" (count rowblock) "rows.")
    (try
      (execute-query! destination-datasource (insert-query table rowblock))
      (catch Exception e
        (log/error "Exception inserting block of " (count rowblock) "rows.")
        (when (= :trace (:min-level log/*config*))
          (.printStackTrace e))
        (when (> (count rowblock) 1)
          (log/info "Trying to split the block into two parts and insert them separately.")
          (doseq [splitted-rowblock [[(take (int (/ (count rowblock) 2)) rowblock)
                                      (drop (int (/ (count rowblock) 2)) rowblock)]]]
            (execute-insert-query-rowblock! table splitted-rowblock)))))))

(defn execute-insert-query!
  "Executes a insert query to the destination database"
  [table resultset]
  (log/info "Splitting resultset of" (count resultset) "rows to insert into" (name table) "table.")
  (execute-insert-query-rowblock! table (split-parameters resultset (max-parameters-by-field-number (count (first resultset))))))

(defn migrate-in-batches
  "Executes a migration from source to destination in batches"
  ([source-query destination-table]
   (migrate-in-batches source-query destination-table limit))
  ([source-query destination-table limit]
   (loop [offset 0
          resultset (execute-select-query! [(str (first source-query) " limit " limit "offset" offset)])]
     (when (seq resultset)
       (execute-insert-query! destination-table resultset)
       (recur (+ limit offset) (execute-select-query! [(str (first source-query) " limit " limit "offset" offset)]))))))

(defn migrate-in-batches-applying-function
		"Executes a migration from source to destination in batches applying a function to the retrieved resultsets"
		([source-query destination-table fun]
   (migrate-in-batches-applying-function source-query destination-table fun limit))
  ([source-query destination-table fun limit]
   (loop [offset 0
          resultset (execute-select-query! [(str (first source-query) " limit " limit "offset" offset)])]
     (when (seq resultset)
       (fun (execute-insert-query! destination-table resultset))
       (recur (+ limit offset) (execute-select-query! [(str (first source-query) " limit " limit "offset" offset)]))))))
