(ns io.github.getcolors.automq.validate
  "Desired-state, credential, tool, and Vultr validation."
  (:require [clojure.string :as str]
            [green.cli :as green-cli]
            [green.process :as process]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.compute :as compute]
            [io.github.getcolors.compute-planning :as planning]
            [io.github.getcolors.compute-ssh :as compute-ssh]
            [io.github.getcolors.once.validate :as once-validate]))

(def profile-par (green-cli/par-name :profile))

(def compute-providers (:compute compute/registry))
(def default-compute-provider cluster/default-compute-provider)

(def required
  "Every key desired state must carry whichever provider is selected. The
  provider-scoped keys come from `compute-providers`.

  `vultr-ssh-keys` is deliberately absent: per the SSH Keypair Standard its
  *absence* selects keygen mode, and requiring it would make a conforming
  deployment invalid. `vultr-name` is absent for the same shape of reason —
  the Compute Name Standard makes the profile the default and the key only an
  override (§2, §5)."
  [:profile :workdir :provider-compute :provider-dns :provider-backend
   :compute-prevent-destroy
   :automq-image :automq-node-count :automq-cluster-id
   :automq-host :automq-broker-name-prefix
   :automq-letsencrypt-email :automq-lego-version
   :automq-kafka-port :automq-internal-port :automq-controller-port
   :automq-sasl-user :automq-sasl-mechanism :automq-heap-opts
   :automq-data-r2-bucket :automq-ops-r2-bucket
   :automq-r2-endpoint :automq-r2-region
   :automq-wal-batch-interval-ms :automq-wal-max-bytes-in-batch
   :r2-bucket :r2-endpoint])

(def host-re #"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")
(def email-re #"^[^@\s]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")
(def image-re #"^[^\s:@]+(?:/[^\s:@]+)*(?::[^\s:@]+)?(?:@sha256:[0-9a-f]{64})?$")
(def digest-re #"@sha256:[0-9a-f]{64}$")
(def bucket-re #"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
(def endpoint-re #"^https://[a-z0-9.-]+(?::\d+)?/?$")
(def prefix-re #"^[a-z][a-z0-9-]{0,15}$")
;; kafka-storage.sh random-uuid: a UUID in unpadded URL-safe base64.
(def cluster-id-re #"^[A-Za-z0-9_-]{22}$")
(def principal-re #"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

(defn missing? [x] (or (nil? x) (and (string? x) (str/blank? x))))

(defn keygen?
  "Whether this deployment owns its machine keypair. Delegates to ONCE, the
  standard's reference implementation, so one rule decides it everywhere."
  [opts]
  (= "managed" (:mode (compute-ssh/mode opts))))

(defn env-errors [env]
  (when (not-empty (str (get env profile-par)))
    [(str profile-par " is set; profile must come from colors.yml only")]))

(defn- port? [x] (and (integer? x) (<= 1 x 65535)))

(defn state-errors
  "Every problem with desired state at once: the missing keys (this package's
  and the selected provider's), the package's own checks, then the Compute
  Cluster Standard's — selection, the source lists, the provider rules, the
  created network's CIDR and the topology — which are ONCE's over `spec`."
  [opts]
  (vec
   (concat
    (for [k required
          :when (missing? (get opts k))]
      (str k " is required"))
    (when-not (= "cloudflare" (:provider-dns opts))
      [":provider-dns must be cloudflare"])
    (when-not (contains? #{"s3" "r2"} (:provider-backend opts))
      [":provider-backend must be s3 or r2"])
    ;; boolean?, not true?. The guard is lifted for exactly one run by
    ;; COLORS_PAR_COMPUTE_PREVENT_DESTROY=false, which arrives through the same
    ;; overlay as every other parameter — so demanding `true` here would reject
    ;; the override before the delete-time guard could honour it, and the
    ;; documented way to destroy this deployment would not work at all. What
    ;; must stay true is the value COMMITTED to colors.yml, and that is a
    ;; review rule rather than something validation can see.
    (when-not (boolean? (:compute-prevent-destroy opts))
      [":compute-prevent-destroy must be true or false"])

    ;; --- cluster shape
    ;; An even count is not merely unusual, it is worse than the odd count
    ;; below it: four voters tolerate one failure, exactly as three do, while
    ;; adding a node that can fail. One node is allowed because it is a
    ;; legitimate development shape, but it is not a quorum.
    (let [n (:automq-node-count opts)]
      (cond
        (missing? n) nil
        (not (integer? n)) [":automq-node-count must be an integer"]
        (not (<= 1 n 9)) [":automq-node-count must be from 1 to 9"]
        (and (even? n) (> n 1))
        [":automq-node-count must be odd: an even quorum tolerates no more failures than the odd size below it"]
        :else nil))
    (when-not (or (missing? (:automq-cluster-id opts))
                  (re-matches cluster-id-re (str (:automq-cluster-id opts))))
      [":automq-cluster-id must be a 22-character base64 UUID as produced by `kafka-storage.sh random-uuid`"])
    (when-not (or (missing? (:automq-host opts))
                  (re-matches host-re (str (:automq-host opts))))
      [":automq-host must be a fully qualified hostname"])
    (when-not (or (missing? (:automq-broker-name-prefix opts))
                  (re-matches prefix-re (str (:automq-broker-name-prefix opts))))
      [":automq-broker-name-prefix must be a short lowercase label"])
    (when-not (or (missing? (:automq-letsencrypt-email opts))
                  (re-matches email-re (str (:automq-letsencrypt-email opts))))
      [":automq-letsencrypt-email must be an email address"])

    ;; --- image
    (when-not (or (missing? (:automq-image opts))
                  (re-matches image-re (str (:automq-image opts))))
      [":automq-image must be a container image reference"])
    ;; This package owns its unit and configuration templates rather than
    ;; running an upstream installer, so nothing tells it when a floating tag
    ;; moves underneath it. A digest is what turns a silent retag into a
    ;; failure at pull time instead of a behaviour change at run time.
    (when-not (or (missing? (:automq-image opts))
                  (re-find digest-re (str (:automq-image opts))))
      [":automq-image must be pinned by digest (…@sha256:…)"])

    ;; --- listeners
    (for [k [:automq-kafka-port :automq-internal-port :automq-controller-port]
          :when (and (not (missing? (get opts k))) (not (port? (get opts k))))]
      (str k " must be an integer from 1 to 65535"))
    (let [ports (keep #(get opts %) [:automq-kafka-port :automq-internal-port
                                     :automq-controller-port])]
      (when (and (= 3 (count ports)) (not= 3 (count (distinct ports))))
        [":automq-kafka-port, :automq-internal-port and :automq-controller-port must differ"]))
    (when-not (or (missing? (:automq-sasl-mechanism opts))
                  (= "SCRAM-SHA-512" (:automq-sasl-mechanism opts)))
      [":automq-sasl-mechanism must be SCRAM-SHA-512"])
    ;; Four principals share one namespace in the metadata log, and two that
    ;; collide would silently merge authorities — the client principal is ACL
    ;; scoped and the others are superusers, so a collision is a privilege
    ;; escalation rather than a naming annoyance.
    (for [[k v] [[:automq-sasl-user (cluster/client-user opts)]
                 [:automq-admin-user (cluster/admin-user opts)]
                 [:automq-broker-user (cluster/broker-user opts)]
                 [:automq-controller-user (cluster/controller-user opts)]]
          :when (not (re-matches principal-re (str v)))]
      (str k " must be a safe 1-64 character principal name"))
    (let [users [(cluster/client-user opts) (cluster/admin-user opts)
                 (cluster/broker-user opts) (cluster/controller-user opts)]]
      (when-not (= (count users) (count (distinct users)))
        ["the client, admin, broker and controller principals must all differ"]))

    ;; --- object storage
    (for [k [:automq-data-r2-bucket :automq-ops-r2-bucket]
          :when (and (not (missing? (get opts k)))
                     (not (re-matches bucket-re (str (get opts k)))))]
      (str k " must be a valid bucket name"))
    ;; AutoMQ addresses the two roles by distinct bucket ids and writes
    ;; different key layouts under each; it also supports no path prefix at
    ;; all, so one bucket cannot host both roles side by side.
    (when (and (not (missing? (:automq-data-r2-bucket opts)))
               (= (:automq-data-r2-bucket opts) (:automq-ops-r2-bucket opts)))
      [":automq-data-r2-bucket and :automq-ops-r2-bucket must be different buckets"])
    ;; The state bucket is the operator's, holds every deployment's tfstate,
    ;; and AutoMQ writes hash-prefixed keys at the bucket root. Sharing them
    ;; is not a style question.
    (for [k [:automq-data-r2-bucket :automq-ops-r2-bucket]
          :when (and (not (missing? (get opts k)))
                     (= (str (get opts k)) (str (:r2-bucket opts))))]
      (str k " must not be the OpenTofu state bucket: AutoMQ writes keys at the bucket root"))
    (for [k [:automq-r2-endpoint]
          :when (and (not (missing? (get opts k)))
                     (not (re-matches endpoint-re (str (get opts k)))))]
      (str k " must be an https endpoint URL"))
    (when-not (or (missing? (:automq-wal-batch-interval-ms opts))
                  (and (integer? (:automq-wal-batch-interval-ms opts))
                       (<= 1 (:automq-wal-batch-interval-ms opts) 60000)))
      [":automq-wal-batch-interval-ms must be an integer from 1 to 60000"])
    (when-not (or (missing? (:automq-wal-max-bytes-in-batch opts))
                  (and (integer? (:automq-wal-max-bytes-in-batch opts))
                       (pos? (:automq-wal-max-bytes-in-batch opts))))
      [":automq-wal-max-bytes-in-batch must be a positive integer"])

    ;; --- compute: the Compute Cluster Standard's checks are ONCE's over the
    ;; spec — selection, the source lists, the Vultr os id and name rules, the
    ;; canonical VPC CIDR, and the node count as a positive integer.
    (concat (compute/validate opts)
            (when (empty? (compute/validate opts))
              (try (planning/plan-deployment opts (cluster/topology opts) (cluster/requirements opts)) []
                   (catch Exception error [(.getMessage error)])))))))

(defn backend-secrets [opts] (map keyword (get-in compute/registry [:backend (keyword (:provider-backend opts)) :secrets])))

(def dns-secrets
  "What talking to Cloudflare needs, on any real event. The compute
  provider's credential comes from the registry."
  [:cloudflare-api-token])

(def application-secrets
  "What converging the cluster needs, and therefore only a create. Every SASL
  password, the keystore password, and the SCRAM salts are generated on the
  hosts and are never supplied by the operator."
  [:automq-r2-access-key-id :automq-r2-secret-access-key])

(defn secret-errors
  "Credentials a real event needs: the selected compute provider's,
  Cloudflare's, the backend's, and on a create the storage keys. A delete
  tears down infrastructure and never converges anything, so it asks for the
  provider credentials only; demanding the storage keys to destroy machines
  would be a lock on the exit."
  [opts event]
  (let [ks (concat (map #(keyword (str/replace (str/lower-case (subs % 11)) "_" "-")) (when (= :validate event) (compute/credential-requirements opts)))
                   dns-secrets
                   (when (= :create event) application-secrets)
                   (backend-secrets opts))]
    (for [k (distinct ks) :when (missing? (get opts k))]
      (str "required credential is not set: " (green-cli/par-name k)))))

(defn tofu-env [opts slot]
  (case slot :provider-dns {:cloudflare-api-token "CLOUDFLARE_API_TOKEN"}
    :provider-backend (get-in once-validate/providers [:provider-backend (:provider-backend opts) :tofu-env] {}) {}))

(def required-tools ["tofu" "aws" "ansible-playbook" "ssh" "ssh-keygen" "curl" "openssl"])
(defn runtime-errors
  ([opts] (runtime-errors opts process/run))
  ([_ runner]
   (vec (for [tool required-tools
              :when (not= 0 (:exit (runner ["sh" "-c" "command -v \"$1\" >/dev/null 2>&1" "sh" tool] {})))]
          (str "required tool is not on PATH: " tool)))))
