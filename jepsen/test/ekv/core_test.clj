(ns ekv.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [ekv.core :as core]))

(deftest only-definitive-validity-passes
  (is (= 0 (core/result-exit-code {:valid? true})))
  (doseq [result [{:valid? false} {:valid? :unknown} {:valid? nil} {}]]
    (testing (str result)
      (is (= 1 (core/result-exit-code result))))))
