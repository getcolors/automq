(ns io.github.getcolors.automq.storage
  "Opt-in deployment-owned S3 or GCS data/ops buckets and bucket-scoped credentials."
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
        {:aws-access-key-id "AWS_ACCESS_KEY_ID" :aws-secret-access-key "AWS_SECRET_ACCESS_KEY"
         :aws-session-token "AWS_SESSION_TOKEN"}))
(defn specs [opts]
  [{:template :io.github.getcolors.automq.tools.storage/main.tf
    :target (str (directory opts) "/main.tf") :data (assoc opts :automq-storage-gcs (= "gcs" (:automq-storage-provider opts))) :opts scaffold/preserve-jinja-delimiters}])
(defn- checked [args options]
  (let [result (process/run args options)]
    (when-not (zero? (:exit result))
      (throw (ex-info "managed storage state operation failed" {})))
    (:out result)))
(defn ownership-preflight!
  "Refuse existing buckets unless this stage already owns their Terraform address."
  [opts]
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
              (throw (ex-info "managed storage refuses to adopt an existing or inaccessible bucket" {})))))))))
(defn step [opts]
  (if-not (managed? opts) (assoc opts :green/exit 0)
    (try
      (let [documents (specs opts)
            event (:green/event opts)]
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
  (let [{:keys [access_key_id secret_access_key]} (:automq/storage-credentials opts)]
    (when (or (str/blank? access_key_id) (str/blank? secret_access_key))
      (throw (ex-info "managed storage credentials unavailable" {})))
    {"COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID" access_key_id
     "COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY" secret_access_key
     "ANSIBLE_HOST_KEY_CHECKING" "False"}))
