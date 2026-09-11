(ns io.github.getcolors.automq.storage-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
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
  (doseq [status ["destroyed" "absent"]]
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

(deftest gcs-storage-ownership-and-native-backend
  (let [opts (assoc managed :automq-storage-provider "gcs" :google-project "colors-508307"
                   :provider-backend "gcs" :gcs-bucket "automq-test-state" :gcs-region "us-central1"
                   :gcs-bucket-mode "managed")
        calls (atom [])]
    (with-redefs [process/run (fn [args _]
                              (swap! calls conj args)
                              (if (= "gcloud" (first args)) {:exit 1 :err "gs://owned-data not found: 404."}
                                  {:exit 0 :out ""}))]
      (storage/ownership-preflight! opts)
      (is (= 2 (count (filter #(= "gcloud" (first %)) @calls)))))
    (with-redefs [process/run (fn [args _]
                              (if (= "gcloud" (first args)) {:exit 1 :err "HTTPError 403: Forbidden"}
                                  {:exit 0 :out ""}))]
      (is (thrown? Exception (storage/ownership-preflight! opts))))
    (is (= :automq/backend-finalize
           (second (workflow/wire-fn :automq/infrastructure (assoc opts :green/event :delete)))))))

(deftest oci-cleanup-renders-before-purge-and-keeps-backend-credentials-out-of-arguments
  (let [opts (assoc managed :automq-storage-provider "oci" :provider-backend "oci" :green/event :delete
                   :oci-access-key-id "BACKEND-ID" :oci-secret-access-key "BACKEND-SECRET")
        calls (atom [])]
    (with-redefs [scaffold/scaffold (fn [render-opts _] (is (= :create (:green/event render-opts))) render-opts)
                  process/run (fn [args options]
                                (swap! calls conj args)
                                (is (= ["python3" "oci-storage.py" "cleanup"] (subvec args 0 3)))
                                (is (= "BACKEND-SECRET" (get-in options [:extra-env "AWS_SECRET_ACCESS_KEY"])))
                                (is (not (str/includes? (last args) "BACKEND-SECRET")))
                                {:exit 0 :out ""})
                  tofu/tofu-with-spec (fn [run-opts _ _] (is (= 1 (count @calls))) (assoc run-opts :green/exit 0))]
      (is (= 0 (:green/exit (storage/step opts)))))))

(deftest oci-service-user-requires-an-explicit-email
  (let [opts (assoc managed :automq-storage-provider "oci"
                   :oci-tenancy-id "tenancy" :oci-compartment-id "compartment"
                   :oci-namespace "example" :oci-config-file-profile "DEFAULT"
                   :automq-r2-region "eu-frankfurt-1"
                   :automq-r2-endpoint "https://example.compat.objectstorage.eu-frankfurt-1.oraclecloud.com"
                   :automq-oci-user-email "operator+automq@example.com")]
    (is (empty? (validate/state-errors opts)))
    (is (some #(str/includes? % "unique user email") (validate/state-errors (dissoc opts :automq-oci-user-email))))
    (is (some #(str/includes? % ":automq-oci-user-email") (validate/state-errors (assoc opts :automq-oci-user-email "not-an-email"))))))

(deftest partial-delete-cleans-application-before-compute-and-errors-refuse
  (doseq [status ["partial" "error"]]
    (let [calls (atom []) step (fn [name] (fn [opts] (swap! calls conj name) (assoc opts :green/exit 0)))]
      (with-redefs [validate/runtime-errors (constantly []) validate/secret-errors (constantly [])
                    io.github.getcolors.compute-inspection/read-deployment (fn [& _] {:status status})
                    tools/ansible-step (step :ansible) tools/ansible-local-step (step :ssh-config)
                    tools/dns-step (step :dns) storage/step (step :storage)
                    tools/infrastructure-step (step :infrastructure) workflow/backend-finalize-step (step :finalize)
                    workflow/backend-advice (fn [& _] identity)]
        (let [result (green.workflow/run workflow/workflow (assoc managed :green/event :delete :compute-prevent-destroy false :s3-bucket-mode "managed"))]
          (is (= (if (= status "partial") 0 1) (:green/exit result)))
          (is (= (if (= status "partial") [:ansible :ssh-config :dns :storage :infrastructure :finalize] []) @calls)))))))
