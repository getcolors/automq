(ns io.github.getcolors.automq.workflow-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.workflow :as workflow]))

(defn- chain [event]
  (loop [step :automq/start seen []]
    (let [[_ next-step] (workflow/wire-fn step {:green/event event})]
      (if next-step
        (recur next-step (conj seen next-step))
        seen))))

(deftest create-resolves-addresses-before-it-needs-them
  ;; DNS needs the compute output; the brokers advertise names that must
  ;; already resolve, and the certificate is issued for those names during the
  ;; play. The order is the dependency, not a preference.
  (is (= [:automq/infrastructure :automq/ssh-config :automq/dns
          :automq/ansible :automq/acceptance]
         (chain :create))))

(deftest delete-unwinds-in-the-order-that-keeps-access
  ;; The ssh_config block goes before the destroy and the keypair after it: a
  ;; stale block is harmless, a key that predeceases its host locks the
  ;; operator out of machines that still exist. DNS goes before the destroy so
  ;; no record survives pointing at an address Vultr can hand to someone else.
  (is (= [:automq/ansible :automq/ssh-config :automq/dns
          :automq/infrastructure :automq/ssh-cleanup]
         (chain :delete))))

(deftest the-destroy-guard-is-desired-state-not-a-flag
  (let [errs (:green/err (workflow/start-step
                          {:green/event :delete :green/real? true
                           :compute-prevent-destroy true}
                          {}))]
    (is (some? errs))))

(deftest defaults-cover-every-key-an-operator-should-not-have-to-write
  (testing "the shape of the cluster and its ports have sensible defaults"
    (is (= 3 (:automq-node-count workflow/defaults)))
    (is (= 9092 (:automq-kafka-port workflow/defaults)))
    (is (= "vultr" (:provider-compute workflow/defaults))))
  (testing "but the guard defaults to protecting the deployment"
    (is (true? (:compute-prevent-destroy workflow/defaults)))))

(deftest each-tofu-stage-keys-its-own-state
  (is (not= tools/infrastructure-tool tools/dns-tool))
  (is (every? #(re-find #"^automq-" %)
              [tools/infrastructure-tool tools/dns-tool tools/ansible-tool
               tools/ansible-local-tool tools/acceptance-tool])))
