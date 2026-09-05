(ns io.github.getcolors.automq.tools-test
  (:require [cheshire.core :as json]
            [clojure.string]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.cluster-test :refer [params]]
            [io.github.getcolors.automq.tools :as tools]))

(def opts
  {:profile "automq-vultr" :workdir ".colors"
   :provider-compute "vultr" :provider-dns "cloudflare" :provider-backend "r2"
   :automq-node-count 3
   :automq-host "automq.example.com" :automq-broker-name-prefix "b"
   :automq-kafka-port 9092 :automq-internal-port 9094 :automq-controller-port 9093
   :automq-sasl-user "automq" :automq-client-topic-prefix "colors-"
   :vultr-vpc-subnet "10.40.0.0/24"
   :vultr-ssh-sources ["0.0.0.0/0" "::/0"]
   :vultr-kafka-sources ["203.0.113.0/24"]})

(def applied (assoc opts :once/cluster params))

(deftest the-adopted-cluster-reaches-the-renderers-respelled
  ;; ONCE records `vpc_ip` and `ssh_key_id` with underscores — the latter is
  ;; the SSH Keypair Standard's contract with ONCE's create preflight and must
  ;; stay verbatim on the params map. The renderers read `:vpc-ip`, so the
  ;; node wrapper respells that one key and nothing else.
  (let [[n] (tools/nodes applied)]
    (is (= "7692e92a" (:ssh_key_id (:once/cluster applied))))
    (is (= "10.40.0.3" (:vpc-ip n)))
    (is (nil? (:vpc_ip n)))
    (is (= "automq-vultr-0" (:name n)))))

(deftest the-compute-stage-refuses-anything-but-the-whole-cluster
  ;; The real create's infrastructure step hands its tofu outputs here. No
  ;; `params` output at all, or a node set that is partial or incomplete, is
  ;; exit 1 with ONCE's message rather than a quorum string against
  ;; 192.0.2.10; the whole cluster lands under `:once/cluster`.
  (let [result (fn [p] {:green/exit 0 :tofu/outputs (when p {:params p})})]
    (testing "no params output"
      (let [r (tools/resolved-cluster opts (result nil))]
        (is (= 1 (:green/exit r)))
        (is (= "compute produced no params output; refusing to converge against the documentation addresses"
               (:green/err r)))))
    (testing "a partial cluster"
      (let [r (tools/resolved-cluster opts (result (update params :nodes pop)))]
        (is (= 1 (:green/exit r)))
        (is (= "the compute stage did not report nodes this package declares: 2" (:green/err r)))))
    (testing "an incomplete node"
      (let [r (tools/resolved-cluster opts (result (assoc-in params [:nodes 2 :ip] nil)))]
        (is (= 1 (:green/exit r)))
        (is (clojure.string/includes? (:green/err r) "did not report a complete node"))))
    (testing "the whole cluster, string-keyed as tofu delivers it"
      (let [raw {"provider" "vultr" "ssh_key_id" "7692e92a"
                 "nodes" (mapv #(into {} (map (fn [[k v]] [(name k) v])) %) (:nodes params))}
            r (tools/resolved-cluster opts (result raw))]
        (is (= 0 (:green/exit r)))
        (is (= params (:once/cluster r)))))))

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
  (let [data (tools/ansible-data opts)]
    (is (not-any? (fn [[k v]]
                    (and (string? v)
                         (re-find #"(?i)secret|password|token|access.key" (name k))))
                  data))
    (is (= "0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093"
           (:quorum-voters (tools/ansible-data applied))))))

(deftest the-compute-stage-renders-every-value-its-template-names
  ;; A Selmer key that is absent renders as empty rather than failing, so the
  ;; firewall rule shipped `port = ""` and only the provider rejected it.
  (let [data (tools/infrastructure-data opts)]
    (is (= 9092 (:kafka-port data)))
    (is (= 3 (:node-count data)))
    (is (= "automq-vultr" (:compute-name data)))
    (is (every? #(not (clojure.string/blank? (str (get data %))))
                [:kafka-port :node-count :compute-name :ssh-sources-hcl
                 :kafka-sources-hcl :controller-port :internal-port]))
    (testing "the quorum ports reach the firewall template"
      ;; Without a rule for these, a Vultr firewall group silently drops TCP
      ;; on the private interface while still passing ICMP, and the cluster
      ;; never elects a controller.
      (is (= 9093 (:controller-port data)))
      (is (= 9094 (:internal-port data))))))

(deftest cidr-lists-survive-both-yaml-and-string-forms
  (is (= ["0.0.0.0/0" "::/0"] (tools/cidrs opts :vultr-ssh-sources)))
  (is (= ["1.2.3.0/24"] (tools/cidrs {:x "1.2.3.0/24"} :x))))
