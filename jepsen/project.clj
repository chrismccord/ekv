(defproject ekv-jepsen "0.1.0"
  :description "Local Jepsen/Knossos linearizability harness for EKV"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.10"]]
  :main ekv.core)
