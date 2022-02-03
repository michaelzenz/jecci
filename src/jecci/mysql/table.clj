(ns jecci.mysql.table
  (:require [jecci.utils.dbms.table :as t]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.mysql.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def TableClientTranslation
  {
   :create-table (fn [value]
                   [(str "create table if not exists t"
                      value
                      " (id int not null primary key, val int)")])
   })

(defrecord my-TableClient [dbclient]
  client/Client
  (open! [this test node]
    (my-TableClient. (client/open! dbclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn dbclient)))
      (client/setup! dbclient test)) 
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (or (db/isleader? (:node# (:conn dbclient)))
         (= (:f op) :read))
      (client/invoke! dbclient test op) 
     (throw (Exception. "not writing to backup")))
    )
  (teardown! [this test]
    (client/teardown! dbclient test))
  (close! [this test]
    (client/close! dbclient test))
  )

(defn gen-TableClient [conn last-created-table]
  (my-TableClient. (t/gen-TableClient conn last-created-table TableClientTranslation)))
