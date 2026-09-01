(ns io.github.getcolors.automq.validate-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.validate :as validate]))

(def base
  {:profile "automq-vultr" :workdir ".colors"
   :provider-compute "vultr" :provider-dns "cloudflare" :provider-backend "r2"
   :compute-prevent-destroy true
   :automq-image "automqinc/automq:1.7.4@sha256:68bf5df674ab9755da51f5200c152df391b0968aeeaf9ec4d12619517cd1234f"
   :automq-node-count 3
   :automq-cluster-id "VrUQI4OSR0y5vnTrGiKsxQ"
   :automq-host "automq.example.com"
   :automq-broker-name-prefix "b"
   :automq-letsencrypt-email "ops@example.com"
   :automq-lego-version "5.4.0"
   :automq-kafka-port 9092 :automq-internal-port 9094 :automq-controller-port 9093
   :automq-sasl-user "automq" :automq-sasl-mechanism "SCRAM-SHA-512"
   :automq-heap-opts "-Xms2g -Xmx2g"
   :automq-data-r2-bucket "automq-data" :automq-ops-r2-bucket "automq-ops"
   :automq-r2-endpoint "https://account.eu.r2.cloudflarestorage.com"
   :automq-r2-region "auto"
   :automq-wal-batch-interval-ms 250 :automq-wal-max-bytes-in-batch 8388608
   :vultr-region "ams" :vultr-plan "vc2-4c-8gb" :vultr-os-id 2284
   :vultr-vpc-subnet "10.40.0.0/24"
   :vultr-ssh-sources ["0.0.0.0/0"] :vultr-kafka-sources ["0.0.0.0/0"]
   :r2-bucket "tofu-state" :r2-endpoint "https://account.eu.r2.cloudflarestorage.com"})

(defn errors [opts] (validate/state-errors opts))
(defn error-matching [opts re] (some #(re-find re %) (errors opts)))

(deftest a-complete-desired-state-is-accepted
  (is (empty? (errors base))))

(deftest every-missing-key-is-reported-at-once
  ;; Exit code 2 means "here is everything that is wrong", not "here is the
  ;; first thing": an operator should need one run to fix a file, not six.
  (let [reported (errors (dissoc base :automq-host :vultr-region :automq-cluster-id))]
    (is (= 3 (count reported)))
    (is (every? #(str/ends-with? % " is required") reported))))

(deftest the-image-must-be-pinned-by-digest
  (testing "a tag alone lets a silent retag change behaviour at run time"
    (is (error-matching (assoc base :automq-image "automqinc/automq:1.7.4")
                        #"pinned by digest")))
  (is (nil? (error-matching base #"pinned by digest"))))

(deftest an-even-quorum-is-refused
  ;; Four voters tolerate exactly one failure, the same as three, while adding
  ;; a node that can fail. That is strictly worse, so it is not offered.
  (is (error-matching (assoc base :automq-node-count 4) #"must be odd"))
  (is (nil? (error-matching (assoc base :automq-node-count 5) #"must be odd")))
  (testing "one node is a legitimate development shape"
    (is (nil? (error-matching (assoc base :automq-node-count 1) #"must be odd"))))
  (is (error-matching (assoc base :automq-node-count 0) #"from 1 to 9"))
  (is (error-matching (assoc base :automq-node-count "three") #"must be an integer")))

(deftest the-cluster-id-must-be-a-real-kafka-uuid
  (is (error-matching (assoc base :automq-cluster-id "not-a-uuid") #"base64 UUID"))
  (is (error-matching (assoc base :automq-cluster-id "VrUQI4OSR0y5vnTrGiKsx") #"base64 UUID")))

(deftest storage-must-not-be-shared
  (testing "the two roles write different key layouts and cannot share a bucket"
    (is (error-matching (assoc base :automq-ops-r2-bucket "automq-data")
                        #"must be different buckets")))
  (testing "and neither may be the state bucket, since AutoMQ writes at the root"
    (is (error-matching (assoc base :automq-data-r2-bucket "tofu-state")
                        #"must not be the OpenTofu state bucket"))))

(deftest listener-ports-must-differ
  (is (error-matching (assoc base :automq-internal-port 9092) #"must differ"))
  (is (error-matching (assoc base :automq-kafka-port 70000) #"from 1 to 65535")))

(deftest principals-must-be-distinct
  ;; Four principals share one namespace in the metadata log, and three of them
  ;; are superusers: a collision is a privilege escalation, not a typo.
  (is (error-matching (assoc base :automq-admin-user "automq") #"must all differ"))
  (is (nil? (error-matching base #"must all differ"))))

(deftest the-destroy-guard-must-stay-in-desired-state
  (is (error-matching (assoc base :compute-prevent-destroy false) #"must remain true")))

(deftest sources-must-be-cidr-lists
  (is (error-matching (assoc base :vultr-kafka-sources "0.0.0.0/0") #"YAML list"))
  (is (error-matching (assoc base :vultr-ssh-sources ["1.2.3.4"]) #"CIDR block"))
  (is (error-matching (assoc base :vultr-ssh-sources []) #"at least one source")))

(deftest the-profile-overlay-is-refused
  (is (seq (validate/env-errors {"COLORS_PAR_PROFILE" "somewhere-else"})))
  (is (empty? (validate/env-errors {}))))

(deftest secrets-are-asked-for-only-when-they-are-needed
  (let [none (dissoc base :vultr-api-key :cloudflare-api-token
                     :automq-r2-access-key-id :automq-r2-secret-access-key)]
    (testing "a create needs the storage keys as well as the provider keys"
      (is (some #(re-find #"AUTOMQ_R2_ACCESS_KEY_ID" %) (validate/secret-errors none :create))))
    (testing "a delete converges nothing, so demanding storage keys would only lock the exit"
      (is (not-any? #(re-find #"AUTOMQ_R2" %) (validate/secret-errors none :delete)))
      (is (some #(re-find #"VULTR_API_KEY" %) (validate/secret-errors none :delete))))))

(deftest the-api-probe-distinguishes-outage-from-credential
  ;; The whole point: a single "check your token" message for every non-2xx
  ;; sends an operator to rotate a key during a provider outage.
  (is (nil? (validate/api-error {:exit 0 :out "200"})))
  (is (re-find #"rejected" (validate/api-error {:exit 0 :out "401"})))
  (is (re-find #"rejected" (validate/api-error {:exit 0 :out "403"})))
  (is (re-find #"rate-limited" (validate/api-error {:exit 0 :out "429"})))
  (let [outage (validate/api-error {:exit 0 :out "503"})]
    (is (re-find #"failure on Vultr's side" outage))
    (is (re-find #"do not\s+rotate" outage)))
  (let [offline (validate/api-error {:exit 6 :out "000"})]
    (is (re-find #"not a credential problem" offline))))

(deftest tools-are-checked-without-touching-the-network
  (let [runner (fn [args _]
                 (if (= "curl" (last args)) {:exit 1 :out ""} {:exit 0 :out ""}))
        errs (validate/runtime-errors (dissoc base :vultr-api-key) runner)]
    (is (some #(re-find #"curl" %) errs))))
