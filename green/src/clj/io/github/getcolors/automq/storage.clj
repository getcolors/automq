(ns io.github.getcolors.automq.storage
  "Deployment-owned data/ops buckets and bucket-scoped credentials."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [green.cli :as cli]
            [green.process :as process]
            [green.scaffold :as scaffold]
            [green.tofu :as tofu]))

(def tool "automq-storage")
(defn managed? [opts] (true? (:automq-storage-managed opts)))
(defn directory [opts] (cli/stage-dir opts tool {:default-profile "automq"}))
(defn aws-env [opts]
  (into {} (keep (fn [[key variable]] (when-let [value (not-empty (str (get opts key)))] [variable value])))
        (if (= "oci" (:provider-backend opts))
          {:oci-access-key-id "AWS_ACCESS_KEY_ID" :oci-secret-access-key "AWS_SECRET_ACCESS_KEY"}
          {:aws-access-key-id "AWS_ACCESS_KEY_ID" :aws-secret-access-key "AWS_SECRET_ACCESS_KEY" :aws-session-token "AWS_SESSION_TOKEN"})))
(defn specs [opts]
  (cond-> [{:template :io.github.getcolors.automq.tools.storage/main.tf
    :target (str (directory opts) "/main.tf") :data (assoc opts :automq-storage-gcs (= "gcs" (:automq-storage-provider opts)) :automq-storage-oci (= "oci" (:automq-storage-provider opts)) :oci-auth (or (:oci-auth opts) "APIKey") :oci-home-region (or (:oci-home-region opts) (:automq-r2-region opts))) :opts scaffold/preserve-jinja-delimiters}]
    (= "oci" (:automq-storage-provider opts))
    (conj {:template :io.github.getcolors.automq.tools.storage/oci-storage.py :target (str (directory opts) "/oci-storage.py") :data opts :opts scaffold/preserve-jinja-delimiters})))
(defn- checked [args options]
  (let [result (process/run args options)]
    (when-not (zero? (:exit result))
      (throw (ex-info "managed storage state operation failed" {})))
    (:out result)))
(defn ownership-preflight!
  "Refuse existing buckets unless this stage already owns their Terraform address."
  [opts]
  (if (= "oci" (:automq-storage-provider opts))
    (checked ["python3" "oci-storage.py" "preflight" (json/generate-string (select-keys opts [:profile :oci-auth :oci-config-file-profile :oci-namespace :oci-compartment-id :automq-r2-region :automq-data-r2-bucket :automq-ops-r2-bucket :compute-prevent-destroy]))] {:dir (directory opts) :extra-env (aws-env opts)})
    (let [options {:dir (directory opts) :extra-env (aws-env opts)}]
    (checked ["tofu" "init" "-input=false" "-no-color"] options)
    (let [state (process/run ["tofu" "state" "list"] options)
          empty-state? (and (= 1 (:exit state)) (str/includes? (str (:err state)) "No state file was found!"))
          _ (when-not (or (zero? (:exit state)) empty-state?)
              (throw (ex-info "managed storage state unavailable" {})))
          addresses (set (str/split-lines (if empty-state? "" (:out state))))
          recorded (if (empty? addresses) {}
                       (into {} (map (juxt :address #(or (get-in % [:values :bucket]) (get-in % [:values :name]))))
                             (get-in (json/parse-string (checked ["tofu" "show" "-json"] options) true) [:values :root_module :resources])))]
      (doseq [[role bucket] [["data" (:automq-data-r2-bucket opts)] ["ops" (:automq-ops-r2-bucket opts)]]]
        (when-not (= bucket (get recorded (str (if (= "gcs" (:automq-storage-provider opts)) "google_storage_bucket" "aws_s3_bucket") ".application[\"" role "\"]")))
          (let [result (process/run (if (= "gcs" (:automq-storage-provider opts)) ["gcloud" "storage" "buckets" "describe" (str "gs://" bucket) "--project" (:google-project opts) "--format=json"] ["aws" "s3api" "head-bucket" "--bucket" bucket "--region" (:automq-r2-region opts)]) options)]
            ;; 403, network failures, and a successful probe all fail closed.
            (when-not (and (pos? (:exit result)) (re-find #"\(404\)|Not Found|NoSuchBucket|HTTPError 404|not found: 404" (str (:err result))))
              (throw (ex-info "managed storage refuses to adopt an existing or inaccessible bucket" {}))))))))))
(defn step [opts]
  (if-not (managed? opts) (assoc opts :green/exit 0)
    (try
      (let [documents (specs opts)
            event (:green/event opts)]
        (when (and (= :delete event) (= "oci" (:automq-storage-provider opts)))
          (scaffold/scaffold (assoc opts :green/event :create) documents)
          (checked ["python3" "oci-storage.py" "cleanup" (json/generate-string (select-keys opts [:profile :oci-auth :oci-config-file-profile :oci-namespace :oci-compartment-id :automq-r2-region :automq-data-r2-bucket :automq-ops-r2-bucket :compute-prevent-destroy]))] {:dir (directory opts) :extra-env (aws-env opts)}))
        (when (= :create event)
          (scaffold/scaffold opts documents)
          (ownership-preflight! opts))
        (let [result (tofu/tofu-with-spec opts documents
                       {:dir (directory opts) :env (aws-env opts) :output-key :automq/storage-credentials})]
          ;; Scoped credentials remain in memory and encrypted backend state.
          ;; Never copy them into template values or print the output object.
          result))
      (catch Exception _ (assoc opts :green/exit 1 :green/err "managed storage failed; inspect bucket ownership, state access, and provider permissions")))))
(defn credential-env [opts]
  (let [{:keys [access_key_id secret_access_key oci_signing_key_b64 oci_signing_key_id]} (:automq/storage-credentials opts)]
    (when (or (str/blank? access_key_id) (str/blank? secret_access_key)
              (and (= "oci" (:automq-storage-provider opts)) (or (str/blank? oci_signing_key_b64) (str/blank? oci_signing_key_id))))
      (throw (ex-info "managed storage credentials unavailable" {})))
    {"COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID" access_key_id
     "COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY" secret_access_key
     "COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_B64" (or oci_signing_key_b64 "")
     "COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_ID" (or oci_signing_key_id "")
     "ANSIBLE_HOST_KEY_CHECKING" "False"}))
