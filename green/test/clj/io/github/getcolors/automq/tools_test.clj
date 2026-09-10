(ns io.github.getcolors.automq.tools-test
  (:require [cheshire.core :as json]
            [green.tofu :as tofu]
            [green.process :as process]
            [green.scaffold :as scaffold]
            [io.github.getcolors.automq.validate :as validate]
            [clojure.string]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.validate-test :as validation]
            [io.github.getcolors.automq.cluster-test :refer [params]]
            [io.github.getcolors.automq.tools :as tools]))

(def opts
  (merge validation/base {:profile "automq-vultr" :workdir ".colors"
   :provider-compute "vultr" :provider-dns "cloudflare" :provider-backend "r2"
   :automq-node-count 3
   :automq-host "automq.example.com" :automq-broker-name-prefix "b"
   :automq-kafka-port 9092 :automq-internal-port 9094 :automq-controller-port 9093
   :automq-sasl-user "automq" :automq-client-topic-prefix "colors-"
   :vultr-vpc-subnet "10.40.0.0/24"
   :vultr-ssh-sources ["0.0.0.0/0"]
   :vultr-kafka-sources ["203.0.113.0/24"]}))

(def applied (assoc opts :colors-compute/cluster params))

(deftest the-adopted-cluster-reaches-the-renderers-respelled
  ;; ONCE records `vpc_ip` and `ssh_key_id` with underscores — the latter is
  ;; the SSH Keypair Standard's contract with ONCE's create preflight and must
  ;; stay verbatim on the params map. The renderers read `:vpc-ip`, so the
  ;; node wrapper respells that one key and nothing else.
  (let [[n] (tools/nodes applied)]
    (is (= "7692e92a" (:ssh_key_id (:colors-compute/cluster applied))))
    (is (= "10.40.0.3" (:vpc-ip n)))
    (is (nil? (:vpc_ip n)))
    (is (= "automq-vultr-0" (:name n)))))

(deftest the-zone-is-the-registrable-domain
  (is (= "example.com" (tools/zone opts))))

(deftest dns-records-are-never-proxied
  ;; Cloudflare's proxy terminates HTTP. Kafka is raw TCP, so a proxied record
  ;; publishes an address that speaks the wrong protocol entirely.
  (let [records (-> (tools/dns-json opts (cluster/nodes opts params))
                    (json/parse-string true)
                    :resource :cloudflare_dns_record)]
    (is (= 6 (count records)) "three bootstrap records and one per broker")
    (is (every? #(false? (:proxied %)) (vals records)))
    (testing "the bootstrap name carries every node's address"
      (is (= #{"203.0.113.10" "203.0.113.11" "203.0.113.12"}
             (set (map :content (vals (select-keys records [:bootstrap_0 :bootstrap_1 :bootstrap_2])))))))
    (testing "each broker name points at its own node"
      (is (= "203.0.113.12" (:content (:broker_2 records))))
      (is (= "b2.automq.example.com" (:name (:broker_2 records)))))))

(deftest the-inventory-carries-per-node-facts-only
  (let [inv (json/parse-string (tools/inventory opts (cluster/nodes opts params)) true)
        hosts (get-in inv [:all :children :automq :hosts])]
    (is (= 3 (count hosts)))
    (testing "exactly one node issues certificates, so only one holds the DNS token"
      (is (= 1 (count (filter :automq_cert_issuer (vals hosts)))))
      (is (true? (:automq_cert_issuer (:automq-vultr-0 hosts)))))
    (testing "the quorum string is not per-node: three nodes must not disagree"
      (is (not-any? :automq_quorum_voters (vals hosts))))))

(deftest ssh-config-hosts-point-the-bare-alias-at-node-zero
  (let [hosts (tools/ssh-config-hosts opts (cluster/nodes opts params))]
    (is (= "automq-vultr" (:name (first hosts))))
    (is (= "203.0.113.10" (:ip (first hosts))))
    (is (= ["automq-vultr" "automq-vultr-0" "automq-vultr-1" "automq-vultr-2"]
           (mapv :name hosts)))
    (is (= ["203.0.113.10" "203.0.113.10" "203.0.113.11" "203.0.113.12"]
           (mapv :ip hosts)))))

(deftest the-ansible-data-carries-no-credential
  ;; Secrets reach the host as lookup('env', …) expressions written literally
  ;; into the playbook. Anything in this map would land in .colors/ and in a
  ;; committed golden.
  (let [data (tools/ansible-data (assoc opts :green/event :build))]
    (is (not-any? (fn [[k v]]
                    (and (string? v)
                         (re-find #"(?i)secret|password|token|access.key" (name k))))
                  data))
    (is (= "0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093"
           (:quorum-voters (tools/ansible-data applied))))))


(deftest dns-receives-separate-r2-backend-credentials
  (let [values (assoc applied :green/event :create :r2-access-key-id "synthetic-id" :r2-secret-access-key "synthetic-secret" :cloudflare-api-token "synthetic-dns")
        captured (atom nil) environment (into {} (System/getenv))]
    (with-redefs [tofu/tofu-with-spec (fn [opts _specs options] (reset! captured (:env options)) (assoc opts :green/exit 0))]
      (is (= 0 (:green/exit (tools/dns-step values)))))
    (is (= {"AWS_ACCESS_KEY_ID" "synthetic-id" "AWS_SECRET_ACCESS_KEY" "synthetic-secret" "CLOUDFLARE_API_TOKEN" "synthetic-dns"} @captured))
    (is (= environment (into {} (System/getenv))))
    (is (= {} (validate/tofu-env values :provider-compute)))))

(deftest acceptance-emits-public-gate-measurements
  (let [result (atom nil)]
    (with-redefs [scaffold/scaffold (fn [opts _] opts)
                  process/run-with-timeout (fn [& _] {:exit 0 :out "acceptance: 18 passed, 0 failed\n" :err ""})]
      (is (= "acceptance: 18 passed, 0 failed\n"
             (with-out-str (reset! result (tools/acceptance-step (assoc applied :green/event :create))))))
      (is (= 0 (:green/exit @result))))))
