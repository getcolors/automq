(ns io.github.getcolors.automq.cluster-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster :as cluster]))

(def opts
  {:profile "automq-vultr"
   :automq-node-count 3
   :automq-host "automq.example.com"
   :automq-broker-name-prefix "b"
   :automq-kafka-port 9092
   :automq-internal-port 9094
   :automq-controller-port 9093})

(def params
  [{:index 0 :ip "203.0.113.10" :vpc-ip "10.40.0.3" :user "root"}
   {:index 1 :ip "203.0.113.11" :vpc-ip "10.40.0.4" :user "root"}
   {:index 2 :ip "203.0.113.12" :vpc-ip "10.40.0.5" :user "root"}])

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
  (testing "documentation-range addresses, so goldens mean the same everywhere"
    (let [ns* (cluster/nodes opts nil)]
      (is (= 3 (count ns*)))
      (is (= ["192.0.2.10" "192.0.2.11" "192.0.2.12"] (mapv :ip ns*)))
      (is (every? #(re-matches #"10\.40\.0\.\d+" (:vpc-ip %)) ns*)))))

(deftest a-partial-compute-output-is-an-error-not-a-smaller-cluster
  ;; Rendering a two-voter quorum for a three-node cluster produces something
  ;; that starts and then cannot elect, which is far worse than refusing.
  (is (nil? (cluster/missing-node-error opts params)))
  (is (re-find #"node 2" (cluster/missing-node-error opts (butlast params))))
  (is (re-find #"quorum string"
               (cluster/missing-node-error opts [{:index 0 :ip "1.2.3.4" :vpc-ip ""}
                                                 {:index 1 :ip "1.2.3.5" :vpc-ip "10.0.0.2"}
                                                 {:index 2 :ip "1.2.3.6" :vpc-ip "10.0.0.3"}])))
  (testing "no output at all is a build, not a broken cluster"
    (is (nil? (cluster/missing-node-error opts nil)))))

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
