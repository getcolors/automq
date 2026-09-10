(ns io.github.getcolors.automq.storage-test
  (:require [clojure.test :refer [deftest is testing]]
            [green.process :as process]
            [green.scaffold :as scaffold]
            [green.tofu :as tofu]
            [io.github.getcolors.automq.storage :as storage]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.workflow :as workflow]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.automq.validate-test :refer [base]]))
(def managed (assoc base :automq-storage-managed true :automq-storage-provider "s3"
                        :automq-r2-region "us-east-1" :automq-r2-endpoint "https://s3.us-east-1.amazonaws.com"
                        :provider-backend "s3" :s3-bucket "automq-test-state" :s3-region "us-east-1"
                        :provider-dns "none" :automq-tls-mode "private-ca"))
(deftest managed-storage-validation-and-order
  (is (empty? (validate/state-errors (dissoc managed :r2-bucket :r2-endpoint))))
  (is (empty? (validate/secret-errors managed :create)))
  (is (seq (validate/state-errors (assoc managed :automq-storage-provider "r2"))))
  (is (seq (validate/state-errors (assoc managed :s3-bucket (:automq-data-r2-bucket managed)))))
  (is (= :automq/storage (second (workflow/wire-fn :automq/infrastructure (assoc managed :green/event :create)))))
  (is (= :automq/storage (second (workflow/wire-fn :automq/dns (assoc managed :green/event :delete))))))

(deftest ownership-refuses-foreign-and-inaccessible-buckets
  (doseq [probe [{:exit 0 :out ""} {:exit 254 :err "(403) Forbidden"} {:exit 1 :err "network timeout"}]]
    (with-redefs [process/run (fn [args _] (if (= "aws" (first args)) probe {:exit 0 :out ""}))]
      (is (thrown? Exception (storage/ownership-preflight! managed)))))
  (let [probes (atom 0)]
    (with-redefs [process/run (fn [args _] (if (= "aws" (first args)) (do (swap! probes inc) {:exit 254 :err "(404) Not Found"}) {:exit 0 :out ""}))]
      (storage/ownership-preflight! managed)
      (is (= 2 @probes))))
  (with-redefs [process/run (fn [args _]
                            (is (not= "aws" (first args)))
                            (if (= ["tofu" "show" "-json"] args)
                              {:exit 0 :out (cheshire.core/generate-string {:values {:root_module {:resources [{:address "aws_s3_bucket.application[\"data\"]" :values {:bucket (:automq-data-r2-bucket managed)}} {:address "aws_s3_bucket.application[\"ops\"]" :values {:bucket (:automq-ops-r2-bucket managed)}}]}}})}
                              {:exit 0 :out "aws_s3_bucket.application[\"data\"]\naws_s3_bucket.application[\"ops\"]\n"}))]
    (storage/ownership-preflight! managed)))

(deftest managed-keys-are-transient-and-reach-only-ansible-environment
  (let [opts (assoc managed :green/event :create :green/dry-run true
                           :automq/storage-credentials {:access_key_id "SCOPED-ID" :secret_access_key "SCOPED-SECRET"})
        seen (atom nil)]
    (is (nil? (:automq/storage-credentials (tools/ansible-data opts))))
    (with-redefs [scaffold/scaffold (fn [opts _] opts)
                  process/run-with-timeout (fn [args options timeout]
                                             (reset! seen [args options timeout])
                                             {:exit 0 :out ""})]
      (is (= 0 (:green/exit (tools/ansible-step opts)))))
    (is (= "SCOPED-SECRET" (get-in @seen [1 :extra-env "COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY"])))
    (is (not-any? #(re-find #"SCOPED" %) (first @seen)))
    (is (thrown? Exception (storage/credential-env managed)))))

(deftest adopted-storage-does-not-call-aws
  (with-redefs [process/run (fn [& _] (throw (AssertionError. "adopted storage must not provision")))
                tofu/tofu-with-spec (fn [& _] (throw (AssertionError. "adopted storage must not provision")))]
    (is (= 0 (:green/exit (storage/step base))))))

(deftest managed-delete-retries-go-straight-to-guarded-finalization
  (doseq [status ["destroyed" "absent" "error"]]
    (let [finalized (atom 0)]
      (with-redefs [validate/runtime-errors (constantly [])
                    validate/secret-errors (constantly [])
                    io.github.getcolors.compute-inspection/read-deployment (fn [& _] {:status status})
                    workflow/backend-finalize-step (fn [opts] (swap! finalized inc) (assoc opts :green/exit 0))
                    tools/ansible-step (fn [& _] (throw (AssertionError. "retired compute must not be contacted")))
                    tools/infrastructure-step (fn [& _] (throw (AssertionError. "finalize retry must not recreate compute")))]
        (is (= 0 (:green/exit (green.workflow/run workflow/workflow
                              (assoc managed :green/event :delete :compute-prevent-destroy false :s3-bucket-mode "managed"))))))
        (is (= 1 @finalized)))))

(deftest first-create-allows-absent-state-but-never-a-renamed-existing-bucket
  (with-redefs [process/run (fn [args _]
                             (cond
                               (= args ["tofu" "state" "list"]) {:exit 1 :err "No state file was found!"}
                               (= "aws" (first args)) {:exit 254 :err "(404) Not Found"}
                               :else {:exit 0 :out ""}))]
    (storage/ownership-preflight! managed))
  (with-redefs [process/run (fn [args _]
                             (cond
                               (= args ["tofu" "state" "list"]) {:exit 0 :out "aws_s3_bucket.application[\"data\"]\n"}
                               (= args ["tofu" "show" "-json"]) {:exit 0 :out "{\"values\":{\"root_module\":{\"resources\":[{\"address\":\"aws_s3_bucket.application[\\\"data\\\"]\",\"values\":{\"bucket\":\"old-bucket\"}}]}}}"}
                               (= "aws" (first args)) {:exit 0 :out ""}
                               :else {:exit 0 :out ""}))]
    (is (thrown? Exception (storage/ownership-preflight! managed)))))
