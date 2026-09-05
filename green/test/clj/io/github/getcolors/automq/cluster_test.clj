(ns io.github.getcolors.automq.cluster-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.once.compute-cluster :as once-cluster]))

(def opts
  {:profile "automq-vultr"
   :provider-compute "vultr"
   :vultr-vpc-subnet "10.40.0.0/24"
   :automq-node-count 3
   :automq-host "automq.example.com"
   :automq-broker-name-prefix "b"
   :automq-kafka-port 9092
   :automq-internal-port 9094
   :automq-controller-port 9093})

;; The compute stage's recorded `params`, as ONCE reads it: snake_case node
;; keys, every field present.
(def params
  {:provider "vultr" :ssh_key_id "7692e92a"
   :nodes [{:index 0 :ip "203.0.113.10" :vpc_ip "10.40.0.3" :user "root" :sudoer "root" :name "automq-vultr-0"}
           {:index 1 :ip "203.0.113.11" :vpc_ip "10.40.0.4" :user "root" :sudoer "root" :name "automq-vultr-1"}
           {:index 2 :ip "203.0.113.12" :vpc_ip "10.40.0.5" :user "root" :sudoer "root" :name "automq-vultr-2"}]})

(deftest the-spec-describes-one-homogeneous-vultr-cluster
  ;; The Compute Cluster Standard's spec-content test: the shape ONCE is handed
  ;; is data, and this is what that data must say.
  (is (= [] (once-cluster/spec-errors cluster/spec)))
  (is (= [{:role nil :count-key :automq-node-count :count 3}] (:roles cluster/spec)))
  (is (= {:role nil :index 0} (once-cluster/entry-id cluster/spec))
      "the bare profile alias reaches node 0")
  (is (= {:non-empty ["ssh-sources"] :may-be-empty ["kafka-sources"]} (:sources cluster/spec)))
  (is (= "vultr" (:default cluster/spec)))
  (is (= ["vultr"] (keys (:registry cluster/spec))))
  (is (= {:mode :created :key :vultr-vpc-subnet}
         (get-in cluster/spec [:registry "vultr" :network]))
      "the quorum crosses a VPC this package creates from vultr-vpc-subnet")
  (is (not (contains? cluster/spec :fallback-subnet))
      "a created network cuts its fallbacks from the CIDR key, not a stand-in")
  (is (= [:vultr-api-key] (get-in cluster/spec [:registry "vultr" :secrets]))))

(deftest names-derive-from-one-index
  (testing "the machine label, the node id and the broker ordinal are one number"
    (is (= ["automq-vultr-0" "automq-vultr-1" "automq-vultr-2"]
           (cluster/machine-names opts)))
    (is (= ["b0.automq.example.com" "b1.automq.example.com" "b2.automq.example.com"]
           (cluster/broker-names opts)))))

(deftest compute-name-prefers-the-profile
  (is (= "automq-vultr" (cluster/compute-name opts)))
  (testing "an override is used when desired state supplies one"
    (is (= "legacy" (cluster/compute-name (assoc opts :vultr-name "legacy")))))
  (testing "a blank override is not an override"
    (is (= "automq-vultr" (cluster/compute-name (assoc opts :vultr-name "  "))))))

(deftest certificate-covers-the-bootstrap-name-and-every-broker
  ;; A client's first connection is to the bootstrap name and every later one
  ;; is to a broker name, so a SAN list missing either half fails for exactly
  ;; the client that happens to be routed there.
  (is (= ["automq.example.com"
          "b0.automq.example.com" "b1.automq.example.com" "b2.automq.example.com"]
         (cluster/certificate-names opts))))

(deftest quorum-is-built-from-private-addresses
  (is (= "0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093"
         (cluster/quorum-voters opts (cluster/nodes opts params))))
  (testing "no public address reaches the quorum string"
    (is (not (re-find #"203\.0\.113" (cluster/quorum-voters opts (cluster/nodes opts params)))))))

(deftest listeners-bind-privately-and-advertise-publicly
  (let [n (first (cluster/nodes opts params))]
    (is (= "CONTROLLER://10.40.0.3:9093,INTERNAL://10.40.0.3:9094,EXTERNAL://0.0.0.0:9092"
           (cluster/listeners opts n)))
    (testing "the controller listener is never advertised — Kafka rejects it"
      (is (not (re-find #"CONTROLLER" (cluster/advertised-listeners opts n)))))
    (is (= "INTERNAL://10.40.0.3:9094,EXTERNAL://b0.automq.example.com:9092"
           (cluster/advertised-listeners opts n)))))

(deftest a-build-renders-fixed-addresses
  (testing "ONCE's fallbacks: TEST-NET-1 publicly, the VPC subnet privately, offset 10"
    (let [ns* (cluster/nodes opts nil)]
      (is (= 3 (count ns*)))
      (is (= ["192.0.2.10" "192.0.2.11" "192.0.2.12"] (mapv :ip ns*)))
      (is (= ["10.40.0.10" "10.40.0.11" "10.40.0.12"] (mapv :vpc-ip ns*)))
      (is (= ["automq-vultr-0" "automq-vultr-1" "automq-vultr-2"] (mapv :name ns*)))
      (is (= ["b0.automq.example.com" "b1.automq.example.com" "b2.automq.example.com"]
             (mapv :broker-name ns*))))))

(deftest nodes-on-a-real-run-come-from-state-in-the-renderers-spelling
  ;; ONCE hands back every node as recorded, `:vpc_ip` and all; this package's
  ;; templates were written against `:vpc-ip`, so the wrapper respells it and
  ;; adds the broker name. Nothing else is touched: the name is the label the
  ;; template gave the instance, never recomputed, and extension fields ride
  ;; through.
  (let [recorded (assoc-in params [:nodes 1 :name] "renamed-in-console")
        ns* (cluster/nodes opts (update-in recorded [:nodes 0] assoc :extra "kept"))]
    (is (= ["203.0.113.10" "203.0.113.11" "203.0.113.12"] (mapv :ip ns*)))
    (is (= ["10.40.0.3" "10.40.0.4" "10.40.0.5"] (mapv :vpc-ip ns*)))
    (is (not-any? #(contains? % :vpc_ip) ns*))
    (is (= "renamed-in-console" (:name (second ns*))))
    (is (= "kept" (:extra (first ns*))))
    (is (= "b1.automq.example.com" (:broker-name (second ns*))))))

(deftest principals-are-distinct-and-the-client-is-not-a-superuser
  (is (= ["automq-admin" "automq-broker" "automq"] (cluster/scram-principals opts)))
  (testing "the controller principal is absent from the SCRAM set on purpose"
    (is (not (some #{"automq-controller"} (cluster/scram-principals opts)))))
  (let [supers (cluster/super-users opts)]
    (is (re-find #"User:automq-admin" supers))
    (is (re-find #"User:automq-controller" supers))
    (testing "the public client principal is never a superuser"
      (is (not (re-find #"User:automq;|User:automq$" supers))))))

(deftest client-acls-grant-no-administration
  (let [acls (cluster/client-acls opts)
        ops (set (mapcat :operations acls))]
    (is (= #{"topic" "group"} (set (map :resource-type acls))))
    (is (every? #(= "prefixed" (:pattern-type %)) acls))
    (is (= #{"Describe" "Read" "Write"} ops))
    (testing "nothing that could administer the cluster or bypass the prefix"
      (is (not (contains? ops "Create")))
      (is (not (contains? ops "Alter")))
      (is (not (contains? ops "ClusterAction"))))))
