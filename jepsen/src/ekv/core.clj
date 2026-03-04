(ns ekv.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [jepsen.checker :as checker]
            [knossos.model :as model]))

(def default-history-path "results/history.edn")
(def default-workers 4)
(def default-ops 200)
(def default-cluster-nodes 3)
(def default-mode "none")
(def default-profile "register")
(def default-seed 1)

(defn usage []
  (println "Usage: lein run [history_path workers ops cluster_nodes mode profile seed]")
  (println "Defaults:")
  (println (str "  history_path: " default-history-path))
  (println (str "  workers:      " default-workers))
  (println (str "  ops:          " default-ops))
  (println (str "  cluster_nodes:" default-cluster-nodes))
  (println (str "  mode:         " default-mode " (none|partition_flap|restart_one|partition_restart)"))
  (println (str "  profile:      " default-profile " (register|lock)"))
  (println (str "  seed:         " default-seed)))

(defn parse-int [s default]
  (if (nil? s) default (Integer/parseInt s)))

(defn ensure-parent! [path]
  (some-> path io/file .getParentFile .mkdirs))

(defn run-generator! [history-path workers ops cluster-nodes mode profile seed]
  (ensure-parent! history-path)
  (println "Generating EKV history via mix script...")
  (let [{:keys [exit out err]}
        (sh/sh "mix" "run" "jepsen/generate_history.exs"
               history-path
               (str workers)
               (str ops)
               (str cluster-nodes)
               mode
               profile
               (str seed)
               :dir "..")]
    (when-not (empty? out) (print out))
    (when-not (empty? err) (binding [*out* *err*] (print err)))
    (when-not (zero? exit)
      (throw (ex-info "History generation failed" {:exit exit})))))

(defn load-history [history-path]
  (with-open [r (java.io.PushbackReader. (io/reader history-path))]
    (edn/read r)))

(defn check-linearizable [history cluster-nodes mode profile]
  (let [lin  (checker/linearizable {:model (model/cas-register)})
        test {:name (str "ekv-local-" cluster-nodes "n-" mode "-" profile)
              :start-time 0}]
    (checker/check lin test history {})))

(defn -main [& args]
  (if (some #{"-h" "--help"} args)
    (usage)
    (let [history-path (-> (or (nth args 0 nil) default-history-path) io/file .getAbsolutePath)
          workers      (parse-int (nth args 1 nil) default-workers)
          ops          (parse-int (nth args 2 nil) default-ops)
          cluster-nodes (parse-int (nth args 3 nil) default-cluster-nodes)
          mode         (or (nth args 4 nil) default-mode)
          profile      (or (nth args 5 nil) default-profile)
          seed         (parse-int (nth args 6 nil) default-seed)]
      (run-generator! history-path workers ops cluster-nodes mode profile seed)
      (let [history (load-history history-path)
            result  (check-linearizable history cluster-nodes mode profile)
            valid?  (:valid? result)]
        (println)
        (println "Jepsen linearizability check result:")
        (println (str "  history path: " history-path))
        (println (str "  cluster nodes:" cluster-nodes))
        (println (str "  mode:         " mode))
        (println (str "  profile:      " profile))
        (println (str "  seed:         " seed))
        (println (str "  operations:   " (count history)))
        (println (str "  valid?:       " valid?))
        (when-not valid?
          (println "  details:")
          (prn result))
        (System/exit (if valid? 0 1))))))
