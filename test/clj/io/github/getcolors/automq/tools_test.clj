(ns io.github.getcolors.automq.tools-test
  (:require [cheshire.core :as json]
            [clojure.string]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.tools :as tools]))

(def opts
  {:profile "automq-vultr" :workdir ".colors"
   :provider-compute "vultr" :provider-dns "cloudflare" :provider-backend "r2"
   :automq-node-count 3
   :automq-host "automq.example.com" :automq-broker-name-prefix "b"
   :automq-kafka-port 9092 :automq-internal-port 9094 :automq-controller-port 9093
   :automq-sasl-user "automq" :automq-client-topic-prefix "colors-"
   :vultr-ssh-sources ["0.0.0.0/0" "::/0"]
   :vultr-kafka-sources ["203.0.113.0/24"]})

(def params
  [{:index 0 :ip "203.0.113.10" :vpc-ip "10.40.0.3" :user "root"}
   {:index 1 :ip "203.0.113.11" :vpc-ip "10.40.0.4" :user "root"}
   {:index 2 :ip "203.0.113.12" :vpc-ip "10.40.0.5" :user "root"}])

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
           (mapv :name hosts)))))

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
           (:quorum-voters (tools/ansible-data (assoc opts :automq/params params)))))))

(deftest the-compute-stage-renders-every-value-its-template-names
  ;; A Selmer key that is absent renders as empty rather than failing, so the
  ;; firewall rule shipped `port = ""` and only the provider rejected it.
  (let [data (tools/infrastructure-data opts)]
    (is (= 9092 (:kafka-port data)))
    (is (= 3 (:node-count data)))
    (is (= "automq-vultr" (:compute-name data)))
    (is (every? #(not (clojure.string/blank? (str (get data %))))
                [:kafka-port :node-count :compute-name :ssh-sources-hcl
                 :kafka-sources-hcl]))))

(deftest cidr-lists-survive-both-yaml-and-string-forms
  (is (= ["0.0.0.0/0" "::/0"] (tools/cidrs opts :vultr-ssh-sources)))
  (is (= ["1.2.3.0/24"] (tools/cidrs {:x "1.2.3.0/24"} :x))))
