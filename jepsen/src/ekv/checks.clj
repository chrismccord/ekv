(ns ekv.checks
  (:require [knossos.model :as model]))

;; Knossos' stock registers treat nil reads as "unknown". For EKV, a successful
;; nil read is an assertion of absence and must compare equal to model state.
(defrecord StrictRegister [value]
  knossos.model.Model
  (step [this op]
    (case (:f op)
      :write (->StrictRegister (:value op))
      :read (if (= value (:value op))
              this
              (model/inconsistent (str "read " (pr-str (:value op)) " from " (pr-str value))))
      :cas (let [[expected next] (:value op)]
             (if (= expected value)
               (->StrictRegister next)
               (model/inconsistent (str "CAS expected " (pr-str expected) " but held " (pr-str value)))))
      (model/inconsistent (str "unexpected operation " (:f op))))))

(defn client-history [history]
  (filterv #(integer? (:process %)) history))

(defn versions
  "The lock model compares full, uniquely-tokened values. Verify the concrete
  VSN/value bindings which justify those comparisons. Eventual lookups supply
  bindings only: they are deliberately not fed to the linearizability model."
  [history]
  (let [bindings (keep (fn [op]
                         (cond
                           (and (= :ok (:type op)) (= :cas (:f op)))
                           [(:vsn op) (second (:value op))]
                           (= :lookup (:f op)) [(:vsn op) (:value op)]))
                       history)
        by-vsn (group-by first bindings)
        by-value (group-by second (filter (comp some? second) bindings))
        vsn? #(and (vector? %) (= 2 (count %)) (integer? (first %)) (some? (second %)))
        errors (concat
                 (for [[vsn pairs] by-vsn
                       :when (or (not (vsn? vsn)) (> (count (set (map second pairs))) 1))]
                   {:error :invalid-or-reused-vsn :vsn vsn :bindings pairs})
                 (for [[value pairs] by-value
                       :when (> (count (set (map first pairs))) 1)]
                   {:error :token-changed-version :value value :bindings pairs})
                 (for [op history
                       :when (and (= :invoke (:type op)) (= :cas (:f op)))
                       :let [expected (first (:value op))
                             vsn (:expected_vsn op)]
                       :when (not (if (nil? expected)
                                    (nil? vsn)
                                    (some #{[vsn expected]} (get by-vsn vsn))))]
                   {:error :unjustified-version-comparison :op op}))]
    {:valid? (empty? errors) :errors (vec errors)}))

(defn coverage [history node-count mode profile]
  (let [clients (client-history history)
        successes (filter #(= :ok (:type %)) clients)
        counts (frequencies (map (juxt :phase #(or (:action %) (:f %))) successes))
        required (if (= profile "lock") [:read :acquire :renew :release] [:read :write])
        configs (filter #(= :config (:f %)) history)
        config (:value (first configs))
        nodes (set (:nodes config))
        starts (filter #(= :fault_start (:f %)) history)
        healing (filter #(= :fault_healing (:f %)) history)
        ends (filter #(= :fault_end (:f %)) history)
        invokes (into {} (map (juxt :process identity) (filter #(= :invoke (:type %)) clients)))
        recovery-nodes (set (map :node (filter #(and (= :recovery (:phase %))
                                                    (= :read (:f %))) successes)))
        errors (concat
                 (when-not (and (= 1 (count configs)) (= 2 (:format config))
                                (= node-count (count nodes)) (= mode (name (:mode config)))
                                (= profile (name (:profile config))))
                   [:invalid-config])
                 (for [phase [:workload :recovery], action required
                       :when (zero? (get counts [phase action] 0))]
                   [:missing-success phase action])
                 (when-not (= nodes recovery-nodes) [:missing-member-recovery-read])
                 (when-not (some #(= :recovery_complete (:f %)) history) [:incomplete-recovery])
                 (when-not (= (count starts) (count healing) (count ends)) [:unhealed-fault])
                 (when-not (if (= mode "none")
                             (empty? starts)
                             (and (= 2 (count starts)) (= #{0 1} (set (map :cycle starts)))))
                   [:incorrect-fault-cycles])
                 (for [start starts
                       :let [end (some #(when (= (:cycle start) (:cycle %)) %) ends)
                             heal (some #(when (= (:cycle start) (:cycle %)) %) healing)]
                       :when (or (nil? end) (nil? heal)
                                 (not= (keyword mode) (:value start) (:value heal) (:value end))
                                 (not (< (:index start) (:index heal) (:index end)))
                                 (not (some #(and (< (:index start)
                                                    (:index (get invokes (:process %)))
                                                    (:index %) (:index heal))
                                                  (= :workload (:phase %)))
                                            successes)))]
                   [:fault-without-successful-work (:cycle start)]))]
    {:valid? (empty? errors) :counts counts :errors (vec errors)}))
