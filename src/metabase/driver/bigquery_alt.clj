(ns metabase.driver.bigquery_alt
  (:require [clojure
             [set :as set]
             [string :as str]]
             [clojure.tools.logging :as log]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.bigquery_alt
             [common :as bigquery_alt.common]
             [query-processor :as bigquery_alt.qp]]
             [metabase.driver.google :as google]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor
             [store :as qp.store]
             [error-type :as error-type]
             [timezone :as qp.timezone]
             [util :as qputil]]
             [metabase.util.schema :as su]
            [schema.core :as s])
  (:import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
           com.google.api.client.http.HttpRequestInitializer
           [com.google.api.services.bigquery Bigquery Bigquery$Builder BigqueryScopes]
            [com.google.api.services.bigquery.model QueryRequest QueryResponse Table TableCell TableFieldSchema TableList TableList$Tables TableReference TableRow TableSchema]
            DatasetList DatasetList$Datasets DatasetReference
            TableList$Tables TableReference TableRow TableSchema]
            java.util.Collections))

(driver/register! :bigquery_alt, :parent #{:google :sql})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Client                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- ^Bigquery credential->client [^GoogleCredential credential]
  (.build (doto (Bigquery$Builder.
                 google/http-transport
                 google/json-factory
                 (reify HttpRequestInitializer
                   (initialize [this httpRequest]
                     (.initialize credential httpRequest)
                     (.setConnectTimeout httpRequest 0)
                     (.setReadTimeout httpRequest 0))))
            (.setApplicationName google/application-name))))

(def ^:private ^{:arglists '([database])} ^GoogleCredential database->credential
  (partial google/database->credential (Collections/singleton BigqueryScopes/BIGQUERY)))

(def ^:private ^{:arglists '([database])} ^Bigquery database->client
  (comp credential->client database->credential))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                      Sync                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- ^TableList list-tables
  "Fetch a page of Tables. By default, fetches the first page; page size is 50. For cases when more than 50 Tables are
  present, you may fetch subsequent pages by specifying the `page-token`; the token for the next page is returned with a
  page when one exists."
  ([database]
   (list-tables database nil))

  ([{{:keys [project-id]} :details, :as database}, ^String dataset-id]
   (list-tables (database->client database) project-id dataset-id nil))

  ([{{:keys [project-id]} :details, :as database}, ^String dataset-id, ^String page-token-or-nil]
   (list-tables (database->client database) project-id dataset-id page-token-or-nil))

  ([^Bigquery client, ^String project-id, ^String dataset-id, ^String page-token-or-nil]
   {:pre [client (seq project-id) (seq dataset-id)]}
   (google/execute (u/prog1 (.list (.tables client) project-id dataset-id)
                     (.setPageToken <> page-token-or-nil)))))


(defn- ^DatasetList list-datasets
  ([{{:keys [project-id]} :details, :as database}]
   {:pre [database (seq project-id)]}
   (google/execute (u/prog1 (.list (.datasets (database->client database)) project-id)))))


(defmethod driver/describe-database :bigquery_alt
  [_ database]
  ;; first page through all the 50-table pages until we stop getting "next page tokens"

    (let [datasets (loop [datasets [], ^DatasetList dataset-list (list-datasets database)]
                 (let [datasets (concat datasets (.getDatasets dataset-list))]
                   (for [^DatasetList$Datasets dataset datasets
                        :let [^DatasetReference datasetref (.getDatasetReference dataset)]]
                        (.getDatasetId datasetref))))]

    (let [tables  (loop [tables [], dataset_id (first datasets), dataset_list (next datasets)]
                    (let [tables (concat tables (loop [dataset_tables [], ^TableList table-list (list-tables database dataset_id)]
                      (let [dataset_tables (concat dataset_tables (.getTables table-list))]
                        (if-let [next-page-token (.getNextPageToken table-list)]
                          (recur dataset_tables (list-tables database dataset_id next-page-token))
                          dataset_tables))))]
                    (if (and (seq dataset_list))
                      (recur tables (first dataset_list) (next dataset_list))
                      tables))
                )]
    ;; after that convert the results to MB format
    {:tables (set (for [^TableList$Tables table tables
                        :let [^TableReference tableref (.getTableReference table)]]
                    {:schema (.getDatasetId tableref), :name (.getTableId tableref)}))})))


(defmethod driver/can-connect? :bigquery_alt
  [_ details-map]
  ;; check whether we can connect by just fetching the first page of tables for the database. If that succeeds we're
  ;; g2g
  (boolean (list-datasets {:details details-map})))

(s/defn get-table :- Table
  ([{{:keys [project-id]} :details, :as database} dataset-id table-id]
   (get-table (database->client database) project-id dataset-id table-id))

  ([client :- Bigquery, project-id :- su/NonBlankString, dataset-id :- su/NonBlankString, table-id :- su/NonBlankString]
   (google/execute (.get (.tables client) project-id dataset-id table-id))))

(defn- bigquery-type->base-type [field-type]
  (case field-type
    "BOOLEAN"   :type/Boolean
    "FLOAT"     :type/Float
    "INTEGER"   :type/Integer
    "RECORD"    :type/Dictionary ; RECORD -> field has a nested schema
    "STRING"    :type/Text
    "DATE"      :type/Date
    "DATETIME"  :type/DateTime
    "TIMESTAMP" :type/DateTimeWithLocalTZ
    "TIME"      :type/Time
    "NUMERIC"   :type/Decimal
    :type/*))

(s/defn ^:private table-schema->metabase-field-info
  [schema :- TableSchema]
  (for [^TableFieldSchema field (.getFields schema)]
    {:name          (.getName field)
     :database-type (.getType field)
     :base-type     (bigquery-type->base-type (.getType field))}))

(defmethod driver/describe-table :bigquery_alt
  [_ database {dataset-id :schema, table-name :name}]
    {:schema dataset-id
   :name   table-name
   :fields (set (table-schema->metabase-field-info (.getSchema (get-table database dataset-id table-name))))})


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                Running Queries                                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(def ^:private ^:const ^Integer query-timeout-seconds 60)

(defn do-with-finished-response
  "Impl for `with-finished-response`."
  {:style/indent 1}
  [^QueryResponse response, f]
  ;; 99% of the time by the time this is called `.getJobComplete` will return `true`. On the off chance it doesn't,
  ;; wait a few seconds for the job to finish.
  (loop [remaining-timeout (double query-timeout-seconds)]
    (cond
      (.getJobComplete response)
      (f response)

      (pos? remaining-timeout)
      (do
        (Thread/sleep 250)
        (recur (- remaining-timeout 0.25)))

      :else
      (throw (ex-info "Query timed out." (into {} response))))))

(defmacro with-finished-response
  "Exeecute `body` with after waiting for `response` to complete. Throws exception if response does not complete before
  `query-timeout-seconds`.

    (with-finished-response [response (execute-bigquery ...)]
      ...)"
  [[response-binding response] & body]
  `(do-with-finished-response
    ~response
    (fn [~(vary-meta response-binding assoc :tag 'com.google.api.services.bigquery.model.QueryResponse)]
      ~@body)))

(defn- post-process-native
  "Parse results of a BigQuery query."
  [^QueryResponse resp]
  (with-finished-response [response resp]
    (let [^TableSchema schema
          (.getSchema response)

          parsers
          (doall
           (for [^TableFieldSchema field (.getFields schema)
                 :let                    [column-type (.getType field)
                                          method (get-method bigquery_alt.qp/parse-result-of-type column-type)]]
             (partial method column-type bigquery_alt.common/*bigquery-timezone-id*)))

          columns
          (for [column (table-schema->metabase-field-info schema)]
            (-> column
                (set/rename-keys {:base-type :base_type})
                (dissoc :database-type)))]
      {:columns (map (comp u/qualified-name :name) columns)
       :cols    columns
       :rows    (for [^TableRow row (.getRows response)]
                  (for [[^TableCell cell, parser] (partition 2 (interleave (.getF row) parsers))]
                    (when-let [v (.getV cell)]
                      ;; There is a weird error where everything that *should* be NULL comes back as an Object.
                      ;; See https://jira.talendforge.org/browse/TBD-1592
                      ;; Everything else comes back as a String luckily so we can proceed normally.
                      (when-not (= (class v) Object)
                        (parser v)))))})))

(defn- ^QueryResponse execute-bigquery
  ([{{:keys [project-id]} :details, :as database} query-string]
   (execute-bigquery (database->client database) project-id query-string))

  ([^Bigquery client, ^String project-id, ^String query-string]
   {:pre [client (seq project-id) (seq query-string)]}
   (let [request (doto (QueryRequest.)
                   (.setTimeoutMs (* query-timeout-seconds 1000))
                   ;; if the query contains a `#legacySQL` directive then use legacy SQL instead of standard SQL
                   (.setUseLegacySql (str/includes? (str/lower-case query-string) "#legacysql"))
                   (.setQuery query-string))]
     (google/execute (.query (.jobs client) project-id request)))))

(defn- process-native* [database query-string]
  {:pre [(map? database) (map? (:details database))]}
  ;; automatically retry the query if it times out or otherwise fails. This is on top of the auto-retry added by
  ;; `execute`
  (letfn [(thunk []
            (post-process-native (execute-bigquery database query-string)))]
    (try
      (thunk)
      (catch Throwable e
        (when-not (error-type/client-error? (:type (u/all-ex-data e)))
          (thunk))))))

(defn- effective-query-timezone-id [database]
  (if (get-in database [:details :use-jvm-timezone])
    (qp.timezone/system-timezone-id)
    "UTC"))


(defmethod driver/execute-query :bigquery_alt
  [driver {{sql :query, params :params, :keys [table-name mbql?]} :native, :as outer-query}]
  (let [database (qp.store/database)]
    (binding [bigquery_alt.common/*bigquery-timezone-id* (effective-query-timezone-id database)]
      (log/tracef "Running BigQuery query in %s timezone" bigquery_alt.common/*bigquery-timezone-id*)
      (let [sql (str "-- " (qputil/query->remark outer-query) "\n" (if (seq params)
                                                                     (unprepare/unprepare driver (cons sql params))
                                                                     sql))]
        (process-native* database sql)))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           Other Driver Method Impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/supports? [:bigquery_alt :expressions] [_ _] false)

(defmethod driver/supports? [:bigquery_alt :foreign-keys] [_ _] true)

;; BigQuery is always in UTC
(defmethod driver/db-default-timezone :bigquery_alt [_ _]
  "UTC")
