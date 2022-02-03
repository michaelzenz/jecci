(ns jecci.mysql.db
  "Tests for mysql"
  (:require [clojure.tools.logging :refer :all]
            [clojure.stacktrace :as cst]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [dom-top.core :refer [with-retry]]
            [fipp.edn :refer [pprint]]
            [jepsen [core :as jepsen]
             [control :as c]
             [db :as db]
             [faketime :as faketime]
             [util :as util]]
            [jepsen.control.util :as cu]
            [clojure.java.jdbc :as j]
            [jecci.utils.handy :as juh]
            [jecci.utils.db :as jud])
  (:use [slingshot.slingshot :only [throw+ try+]]))

(def home "/home/jecci")
(def my-dir "/home/jecci/mysql")
(def my-install-dir "/home/jecci/pginstall")
(def my-bin-dir (str my-install-dir "/bin"))
(def my-postgres-file (str my-bin-dir "/postgres"))
(def my-initdb-file (str my-bin-dir "/initdb"))
(def my-my_ctl-file (str my-bin-dir "/my_ctl"))
(def my-psql-file (str my-bin-dir "/psql"))
(def my-my_basebackup-file (str my-bin-dir "/my_basebackup"))
(def my-my_isready-file (str my-bin-dir "/my_isready"))

(def my-data "/home/jecci/pgdata")
(def my-config-file (str my-data "/postgresql.conf"))
(def my-hba-file (str my-data "/my_hba.conf"))
(def my-log-file (str my-data "/pg.log"))
(def my_ctl-stdout (str my-data "/my_ctl.stdout"))

(def repeatable-read "shared_buffers=4GB\nlisten_addresses = '*'\nwal_keep_size=16GB\n
default_transaction_isolation = 'repeatable read'")
(def serializable "shared_buffers=4GB\nlisten_addresses = '*'\nwal_keep_size=16GB\n
default_transaction_isolation = 'serializable'")

; leader of pg cluster, should contain only one node
(def leaders ["n1"])
(def backups ["n2" "n3" "n4" "n5"])

(def hba-appends
  (str/join "\n"
            (for [db ["replication" "all"]
                  :let [line ((fn [db] (str "host    "
                                            db
                                            "         all         "
                                            "0.0.0.0/0"
                                            "    trust"))
                              db)]]
              line)))

(defn my-initdb
  "Inits postgres"
  []
  (c/exec "bash" :-c (str my-initdb-file " -n -D " my-data " -E utf-8 ")))

(defn get-leader-node
  "Get leader, should return one node"
  []
  (first leaders))

(defn get-followers-node
  "Get followers, should return a list"
  []
  backups)

(defn my-start!
  "Starts the pg daemons on the cluster"
  [_ _]
  (try+
   (c/su (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f "postgres" :|| :echo "no process to kill"))
   (juh/exec->info (c/exec my-my_ctl-file "-D" my-data "-l" my-log-file "start"))
   (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
     nil)))

(defn my-stop!
  "Stops postgres"
  [test node]
  (try+
   (juh/exec->info (c/exec my-my_ctl-file "-D" my-data "stop"))
   (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
     nil)))

(defn start-wait-pg!
  "Starts pg, waiting for my_isready."
  [test node]
  (jud/restart-loop :postmaster (my-start! test node)
                    (try+
                     (juh/exec->info (c/exec my-my_isready-file))
                     :ready
                     (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                       (when (str/includes? (str err) "no response") :crashed)
                       (when (str/includes? (str err) "rejecting connections") :starting)))))



(defn tarball-url
  "Return the URL for a tarball"
  [test]
  (or (:tarball-url test)
      (str "https://downloads.mysql.com/archives/get/p/23/file/mysql-server_8.0.27-1debian11_amd64.deb-bundle.tar")))


(defn isleader?
  [node]
  (contains? (into #{} leaders) node))

(defn setup-faketime!
  "Configures the faketime wrapper for this node, so that the given binary runs
  at the given rate."
  [bin rate]
  (info "Configuring" bin "to run at" (str rate "x realtime"))
  (faketime/wrap! my-postgres-file 0 rate))

(defn install!
  "Downloads archive and extracts it to our local my-dir, if it doesn't exist
  already. If test contains a :force-reinstall key, we always install a fresh
  copy.

  Calls `sync`"
  [test node]
  (c/su
   (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f "postgres" :|| :echo "no process to kill")
   (c/exec :rm :-rf my-data)
   (c/exec :rm :-rf my-dir))
  (when (or (:force-reinstall test)
            (not (cu/exists? my-install-dir)))
    (info node "installing postgresql")
    (cu/install-archive! (tarball-url test) my-dir {:user? "jecci", :pw? "123456"})
    
    (c/exec "mkdir" "-p" my-data)
    (c/cd my-dir
          (c/exec (str my-dir "/configure") (str "--prefix=" my-install-dir) "--enable-depend" "--enable-cassert"
                  "--enable-debug"  "--without-readline" "--without-zlib")
          (juh/exec->info  (c/exec "make" "-j8" "-s"))
          (juh/exec->info  (c/exec "make" "install" "-j8" "-s")))))

(defn db
  "postgres"
  []
  (reify db/DB
    (setup! [db test node]
      (info "setting up postgres")
      (c/su (juh/exec->info (c/exec "apt" "install" "-qy" "bison" "flex")))

      (c/cd home
            (install! test node)

            (when (isleader? node)
              (info node "initing leader postgres")

              (juh/exec->info (my-initdb))
              (c/exec "echo" :-e (if (= (:workload test) :append) serializable repeatable-read)
                      :>> my-config-file)
              (c/exec "echo" :-e hba-appends :>> my-hba-file)

              (c/sudo "jecci"
                      (if-let [ratio (:faketime test)]
                   ; Add faketime wrappers
                        (setup-faketime! my-postgres-file (faketime/rand-factor ratio))
                        (c/cd my-dir
                   ; Destroy faketime wrappers, if applicable.
                              (faketime/unwrap! my-postgres-file))))

              ; To use faketime wrapper, we must start pg by running postgres executable
              ; directly, because if we use wrapper, the parent pid of postgres will not be the
              ; one we run, which is not allowed by my_ctl
              ; 
              ; Will change to use this kind of start for pg in the future. Will also change
              ; to use start-daemon! as well.
                   ;(juh/exec->info (c/exec "bash" :-c (str "nohup \"" my-postgres-file
                         ;             "\" " "-D " my-data " < \"" "/dev/null"
                           ;           "\" >> \"" my-log-file "\" 2>&1 &")))
              (juh/exec->info (c/exec my-my_ctl-file :-D my-data :-l my-log-file "start"))))

      (info node "finish installing" (str (when (isleader? node) " and init for leaders")))
      (jepsen/synchronize test 180)

      (when (contains? (into #{} backups) node)
        (info node "starting backup")
        (c/cd home
              (c/su (c/exec :rm :-rf my-data))
              (c/exec "mkdir" :-p my-data)
              (juh/change-permit my-data)
              (locking test (juh/exec->info (c/exec my-my_basebackup-file
                                                    :-R :-D my-data :-h (first leaders) :-p "5432")))
              (c/exec "echo" :-e hba-appends :>> my-hba-file)
              (c/exec "echo" :-e "shared_buffers=128MB\nlisten_addresses = '*'" :>> my-config-file)

              (c/sudo "jecci"
                      (if-let [ratio (:faketime test)]
              ; Add faketime wrappers
                        (setup-faketime! my-postgres-file (faketime/rand-factor ratio))
                        (c/cd my-dir
                ; Destroy faketime wrappers, if applicable.
                              (faketime/unwrap! my-postgres-file))))
              ; same as leader, will change the way we start pg in the future
              (juh/exec->info (c/exec my-my_ctl-file :-D my-data :-l my-log-file "start"))))

      (info node "finish starting up")
      (jepsen/synchronize test 180))

    (teardown! [db test node]
      (try+
       (my-stop! test node)
       (c/exec :rm :-rf (str my-data "/*"))
       (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
          ; No such dir
         (warn (str err))))
      (info node "finish tearing down"))

    ; Following files will be downloaded from nodes
    ; When test fail or finish
    db/LogFiles
    (log-files [_ test node]
      [my-log-file])))
