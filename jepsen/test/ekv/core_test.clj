(ns ekv.core-test
  (:require [clojure.test :refer :all]
            [ekv.core :as core]))

(deftest only-explicit-true-passes
  (doseq [[valid? expected-name expected-exit]
          [[true "true" 0]
           [false "false" 1]
           [:unknown "unknown" 1]
           [nil "error" 1]
           ["true" "error" 1]]]
    (with-redefs [core/run-generator! (fn [& _])
                  core/load-history (fn [& _] [])
                  core/check-linearizable (fn [& _] {:valid? valid?})]
      (let [exit (atom nil)
            output (with-out-str (reset! exit (core/run-cli [])))]
        (is (= expected-exit @exit))
        (is (.contains output (str "EKV_JEPSEN_RESULT=" expected-name "\n")))
        (when-not (true? valid?)
          (is (.contains output "details:")))))))

(deftest generator-errors-cannot-produce-a-positive-verdict
  (with-redefs [core/run-generator! (fn [& _] (throw (ex-info "failed" {})))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"failed" (core/run-cli [])))))
