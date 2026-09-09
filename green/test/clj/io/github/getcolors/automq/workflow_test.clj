(ns io.github.getcolors.automq.workflow-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [green.workflow :as wf]
            [io.github.getcolors.compute-orchestration :as compute]
            [io.github.getcolors.compute-inspection :as inspection]
            [io.github.getcolors.automq.workflow :as workflow]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.ssh-config :as config]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.automq.validate-test :refer [base]]
            [io.github.getcolors.automq.cluster-test :refer [params]]))
(deftest create-and-delete-order-retain-access
  (is (= :automq/infrastructure (second (workflow/wire-fn :automq/start {:green/event :create}))))
  (is (= :automq/ssh-config (second (workflow/wire-fn :automq/infrastructure {:green/event :create}))))
  (is (= :automq/infrastructure (second (workflow/wire-fn :automq/dns {:green/event :delete}))))
  (is (= 1 (count (workflow/wire-fn :automq/infrastructure {:green/event :delete}))))
  (is (= 1 (count (workflow/wire-fn :automq/start {:green/event :validate})))))
(deftest compute-adapter-forwards-policy-and-adopts-only-success
  (let [seen (atom nil)]
    (with-redefs [compute/orchestrate (fn [_ topology requirements] (reset! seen [topology requirements]) {:status "ready" :cluster params :key {:private_key_path "/tmp/owned"}})]
      (let [result (tools/infrastructure-step (assoc base :green/event :create))]
        (is (= params (:colors-compute/cluster result)))
        (is (= "/tmp/owned" (:ssh-private-key-path result)))
        (is (= [{:role nil :count 3}] (first @seen)))
        (is (= #{22 9092 9093 9094} (set (map :from_port (get-in @seen [1 :security :ingress])))))
        (is (= ["automq-vultr/automq-infrastructure.tfstate"] (get-in @seen [1 :legacy_state_keys])))))
    (with-redefs [compute/orchestrate (fn [& _] {:status "error"})]
      (is (= 1 (:green/exit (tools/infrastructure-step (assoc base :green/event :create))))))))
(deftest delete-inspection-fails-closed-and-protection-precedes-it
  (with-redefs [validate/runtime-errors (constantly []) validate/secret-errors (constantly [])
                inspection/read-deployment (fn [& _] {:status "present" :cluster params :key {:private_key_path "/tmp/owned"}})]
    (is (= params (:colors-compute/cluster (workflow/start-step (assoc base :green/event :delete :compute-prevent-destroy false) {}))))
    (with-redefs [inspection/read-deployment (fn [& _] (throw (AssertionError. "protected deletion")))]
      (is (not= 0 (:green/exit (workflow/start-step (assoc base :green/event :delete) {})))))))
(deftest native-sdk-passes-compute-join-to-application-stages
  (let [visited (atom []) stage (fn [name] (fn [opts] (swap! visited conj name) (is (= "10.40.0.5" (:vpc-ip (last (tools/nodes opts))))) (assoc opts :green/exit 0)))]
    (with-redefs [validate/runtime-errors (constantly []) validate/secret-errors (constantly []) config/preflight! #(assoc % :green/exit 0)
                  compute/orchestrate (fn [& _] {:status "ready" :cluster params :key {:private_key_path "/tmp/owned"}})
                  tools/ansible-local-step (stage :ssh) tools/dns-step (stage :dns) tools/ansible-step (stage :ansible) tools/acceptance-step (stage :acceptance)]
      (is (= 0 (:green/exit (wf/run (wf/workflow {:start :automq/start :wire-fn workflow/wire-fn}) (assoc base :green/event :create)))))
      (is (= [:ssh :dns :ansible :acceptance] @visited)))))

(deftest credential-free-native-build-renders-library-and-application-artifacts
  (let [directory (.toFile (java.nio.file.Files/createTempDirectory "automq-green-build-" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (try
      (with-redefs [compute/orchestrate (fn [& _] (throw (AssertionError. "build must not run compute")))
                    inspection/read-deployment (fn [& _] (throw (AssertionError. "build must not read state")))]
        (let [result (wf/run workflow/workflow (assoc base :green/event :build :workdir (.getPath directory)))
              names (set (map #(.getName %) (file-seq directory)))]
          (is (= 0 (:green/exit result)) (:green/err result))
          (is (contains? names "node.tf.json"))
          (is (contains? names "shared.tf.json"))
          (is (contains? names "inventory.json"))
          (is (contains? names "compose.yml"))
          (is (= "/home/build-placeholder/.ssh/automq-vultr" (:ssh-private-key-path result)))
          (is (not (.exists (io/file (tools/tool-dir (assoc base :workdir (.getPath directory)) tools/infrastructure-tool) "main.tf"))))))
      (finally (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))

(deftest library-controls-provider-capabilities-and-real-inventory
  (let [opts (assoc base :provider-compute "digitalocean" :digitalocean-region "ams3" :digitalocean-size "s-2vcpu-4gb"
                         :digitalocean-image "ubuntu-24-04-x64" :automq-ssh-sources ["0.0.0.0/0"] :automq-kafka-sources [])]
    (is (empty? (validate/state-errors opts))))
  (is (seq (validate/state-errors (assoc base :provider-backend "local"))))
  (is (thrown? Exception (cluster/nodes (assoc base :green/event :create))))
  (is (= 3 (count (cluster/nodes (assoc base :green/event :delete :automq-node-count 1) params)))))
