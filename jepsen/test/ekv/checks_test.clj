(ns ekv.checks-test
  (:require [clojure.test :refer :all]
            [ekv.checks :as checks]
            [ekv.core :as core]
            [knossos.wgl :as wgl]))

(def a {:owner "a" :token "acquire-a"})
(def a2 {:owner "a" :token "renew-a"})
(def b {:owner "b" :token "acquire-b"})

(defn pair [p f value type & [extra]]
  [(merge {:process p :type :invoke :f f :value (if (= :read f) nil value)} extra)
   (merge {:process p :type type :f f :value value} extra)])

(defn valid? [history]
  (:valid? (wgl/analysis (checks/->StrictRegister nil) (vec history))))

(deftest register-oracle-rejects-missing-and-stale-values
  (is (true? (valid? (pair 0 :read nil :ok))))
  (is (true? (valid? (concat (pair 0 :write 7 :ok) (pair 1 :read 7 :ok)))))
  (is (false? (valid? (concat (pair 0 :write 7 :ok) (pair 1 :read nil :ok)))))
  (is (false? (valid? (concat (pair 0 :write 7 :ok) (pair 1 :read 6 :ok)))))
  (is (false? (valid? (concat (pair 0 :write 7 :fail) (pair 1 :read 7 :ok)))))
  (is (true? (valid? (concat (pair 0 :write 7 :info) (pair 1 :read 7 :ok))))))

(deftest lock-oracle-checks-tokens-not-just-owner
  (let [acquired (pair 0 :cas [nil a] :ok)
        renewed (concat acquired (pair 1 :cas [a a2] :ok))]
    (is (false? (valid? (concat acquired (pair 2 :read nil :ok)))))
    (is (false? (valid? (concat acquired (pair 2 :cas [nil b] :ok)))))
    (is (false? (valid? (concat renewed (pair 2 :read a :ok)))))
    (is (false? (valid? (concat renewed (pair 2 :cas [a nil] :ok)))))
    (is (true? (valid? (concat renewed (pair 2 :cas [a2 nil] :ok) (pair 3 :read nil :ok)))))))

(deftest version-bindings-must-justify-cas
  (let [acquired (pair 0 :cas [nil a] :ok {:vsn [1 "a"] :expected_vsn nil})
        renewed (pair 1 :cas [a a2] :ok {:vsn [2 "a"] :expected_vsn [1 "a"]})
        lookup {:process :observer :type :info :f :lookup :value a :vsn [1 "a"]}]
    (is (true? (:valid? (checks/versions (concat acquired renewed)))))
    (is (false? (:valid? (checks/versions (concat acquired (map #(assoc % :vsn [1 "a"]) renewed))))))
    (is (false? (:valid? (checks/versions (concat acquired [(assoc lookup :vsn [9 "a"])])))))
    (is (false? (:valid? (checks/versions (concat acquired (map #(assoc % :expected_vsn [9 "a"]) renewed))))))
    (is (true? (:valid? (checks/versions (concat (pair 0 :cas [nil a] :info)
                                                 [lookup] renewed)))))
    (is (= [] (checks/client-history [lookup])))))

(defn covered-register []
  (vec
    (concat
      [{:process :nemesis :f :config
        :value {:format 2 :nodes ["a" "b" "c"] :mode :none :profile :register}}]
      (pair 0 :write 1 :ok {:phase :workload :node "a"})
      (pair 1 :read 1 :ok {:phase :workload :node "a"})
      (pair 2 :write 2 :ok {:phase :recovery :node "a"})
      (mapcat (fn [p n] (pair p :read 2 :ok {:phase :recovery :node n})) [3 4 5] ["a" "b" "c"])
      [{:process :nemesis :f :recovery_complete}])))

(deftest coverage-cannot-be-vacuously-true
  (let [h (covered-register)
        check #(checks/coverage % 3 "none" "register")]
    (is (true? (:valid? (check h))))
    (is (false? (:valid? (check []))))
    (is (false? (:valid? (check (map #(if (= :ok (:type %)) (assoc % :type :fail) %) h)))))
    (is (false? (:valid? (check (filter #(not= "c" (:node %)) h)))))
    (is (false? (:valid? (check (filter #(not= :recovery_complete (:f %)) h)))))
    (is (false? (:valid? (checks/coverage h 3 "partition_flap" "register"))))))

(deftest aggregate-checker-rejects-a-lost-value-with-otherwise-valid-coverage
  (let [h (mapv #(assoc %2 :index %1) (range) (covered-register))
        lost (mapv #(if (and (= 5 (:process %)) (= :ok (:type %)))
                      (assoc % :value nil) %) h)
        good-result (core/check-linearizable h 3 "none" "register")
        lost-result (core/check-linearizable lost 3 "none" "register")]
    (is (true? (:valid? good-result)))
    (is (true? (get-in lost-result [:coverage :valid?])))
    (is (false? (get-in lost-result [:linearizable :valid?])))
    (is (false? (:valid? lost-result)))))

(deftest fault-cycles-require-successful-overlap-and-healing
  (let [h (covered-register)
        h (assoc-in h [0 :value :mode] :partition_flap)
        fault (fn [f cycle] {:process :nemesis :f f :cycle cycle :value :partition_flap})
        complete (map-indexed
                   #(assoc %2 :index %1)
                   (concat (take 5 h)
                           [(fault :fault_start 0)]
                           (pair 6 :read 1 :ok {:phase :workload :node "a"})
                           [(fault :fault_healing 0) (fault :fault_end 0)
                            (fault :fault_start 1)]
                           (pair 7 :read 1 :ok {:phase :workload :node "a"})
                           [(fault :fault_healing 1) (fault :fault_end 1)]
                           (drop 5 h)))
        check #(checks/coverage % 3 "partition_flap" "register")]
    (is (true? (:valid? (check complete))))
    (is (false? (:valid? (check (remove #(= :fault_end (:f %)) complete)))))
    (is (false? (:valid? (check (map #(if (= :fault_start (:f %)) (assoc % :index 100) %) complete)))))))
