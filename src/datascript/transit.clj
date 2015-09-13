(ns datascript.transit
  (:require
    [datascript :as d]
    [datascript.core :as dc]
    [cognitect.transit :as t])
  (:import
    [datascript.core DB Datom]
    [datascript.btset BTSet Iter]
    [java.io ByteArrayInputStream ByteArrayOutputStream]))


(def read-handlers
  { "datascript/DB"    (t/read-handler dc/db-from-reader)
    "datascript/Datom" (t/read-handler dc/datom-from-reader) })


(def write-handlers
  (let [list-handler (get t/default-write-handlers java.util.List)]
    { DB    (t/write-handler "datascript/DB"
              (fn [db]
                { :schema (:schema db)
                  :datoms (:eavt db) }))
      Datom (t/write-handler "datascript/Datom"
              (fn [^Datom d]
                [(.-e d) (.-a d) (.-v d) (.-tx d)]))
      BTSet list-handler
      Iter list-handler }))


(defn read-transit [is]
  (t/read (t/reader is :json { :handlers read-handlers })))


(defn read-transit-str [^String s]
  (read-transit (ByteArrayInputStream. (.getBytes s "UTF-8"))))


(defn write-transit [o os]
  (t/write (t/writer os :json { :handlers write-handlers }) o))


(defn write-transit-bytes ^bytes [o]
  (let [os (ByteArrayOutputStream.)]
    (write-transit o os)
    (.toByteArray os)))


(defn write-transit-str [o]
  (String. (write-transit-bytes o) "UTF-8"))
