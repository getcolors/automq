(ns io.github.getcolors.automq.validate
  "Desired-state, credential, tool, and Vultr validation."
  (:require [clojure.string :as str]
            [green.cli :as green-cli]
            [green.process :as process]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.once.ssh :as once-ssh]
            [io.github.getcolors.once.validate :as once-validate]))

(def profile-par (green-cli/par-name :profile))

(def required
  "Every key desired state must carry.

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
   :vultr-region :vultr-plan :vultr-os-id :vultr-vpc-subnet
   :vultr-ssh-sources :vultr-kafka-sources
   :r2-bucket :r2-endpoint])

(def host-re #"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")
(def email-re #"^[^@\s]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")
(def image-re #"^[^\s:@]+(?:/[^\s:@]+)*(?::[^\s:@]+)?(?:@sha256:[0-9a-f]{64})?$")
(def digest-re #"@sha256:[0-9a-f]{64}$")
(def bucket-re #"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
(def endpoint-re #"^https://[a-z0-9.-]+(?::\d+)?/?$")
(def cidr-re #"^(?:\d{1,3}\.){3}\d{1,3}/\d{1,2}$")
(def prefix-re #"^[a-z][a-z0-9-]{0,15}$")
;; kafka-storage.sh random-uuid: a UUID in unpadded URL-safe base64.
(def cluster-id-re #"^[A-Za-z0-9_-]{22}$")
(def principal-re #"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

(defn missing? [x] (or (nil? x) (and (string? x) (str/blank? x))))

(defn keygen?
  "Whether this deployment owns its machine keypair. Delegates to ONCE, the
  standard's reference implementation, so one rule decides it everywhere."
  [opts]
  (once-ssh/keygen? opts))

(defn env-errors [env]
  (when (not-empty (str (get env profile-par)))
    [(str profile-par " is set; profile must come from colors.yml only")]))

(defn- port? [x] (and (integer? x) (<= 1 x 65535)))

(defn- source-list-errors [opts k]
  (let [v (get opts k)]
    (cond
      (missing? v) nil
      (not (sequential? v)) [(str k " must be a YAML list of CIDR sources")]
      (empty? v) [(str k " must list at least one source, or be removed")]
      :else (for [[i s] (map-indexed vector v)
                  :when (or (not (string? s)) (not (str/includes? (str s) "/")))]
              (str k "[" i "] must be a CIDR block such as 0.0.0.0/0")))))

(defn state-errors [opts]
  (vec
   (concat
    (for [k required :when (missing? (get opts k))] (str k " is required"))
    (when-not (= "vultr" (:provider-compute opts))
      [":provider-compute must be vultr"])
    (when-not (= "cloudflare" (:provider-dns opts))
      [":provider-dns must be cloudflare"])
    (when-not (contains? #{"local" "s3" "r2"} (:provider-backend opts))
      [":provider-backend must be local, s3, or r2"])
    (when-not (true? (:compute-prevent-destroy opts))
      [":compute-prevent-destroy must remain true in desired state"])

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

    ;; --- compute
    (when-not (or (missing? (:vultr-os-id opts)) (integer? (:vultr-os-id opts)))
      [":vultr-os-id must be Vultr's numeric operating-system id"])
    (when-not (or (missing? (:vultr-vpc-subnet opts))
                  (re-matches cidr-re (str (:vultr-vpc-subnet opts))))
      [":vultr-vpc-subnet must be a CIDR block such as 10.40.0.0/24"])
    (source-list-errors opts :vultr-ssh-sources)
    (source-list-errors opts :vultr-kafka-sources)
    (when (and (not (missing? (:vultr-name opts)))
               (not (re-matches principal-re (str (:vultr-name opts)))))
      [":vultr-name must be a safe 1-64 character name"]))))

(defn backend-secrets [opts]
  (:secrets (get-in once-validate/providers
                    [:provider-backend (:provider-backend opts)])))

(def provider-secrets
  "What talking to the providers needs, on any real event."
  [:vultr-api-key :cloudflare-api-token])

(def application-secrets
  "What converging the cluster needs, and therefore only a create. Every SASL
  password, the keystore password, and the SCRAM salts are generated on the
  hosts and are never supplied by the operator."
  [:automq-r2-access-key-id :automq-r2-secret-access-key])

(defn secret-errors
  "Credentials a real event needs. A delete tears down infrastructure and never
  converges anything, so it asks for the provider credentials only; demanding
  the storage keys to destroy machines would be a lock on the exit."
  [opts event]
  (let [ks (concat provider-secrets
                   (when (= :create event) application-secrets)
                   (backend-secrets opts))]
    (for [k (distinct ks) :when (missing? (get opts k))]
      (str "required credential is not set: " (green-cli/par-name k)))))

(defn tofu-env [opts slot]
  (case slot
    :provider-compute {:vultr-api-key "VULTR_API_KEY"}
    :provider-dns {:cloudflare-api-token "CLOUDFLARE_API_TOKEN"}
    :provider-backend (:tofu-env (get-in once-validate/providers
                                         [:provider-backend (:provider-backend opts)]) {})
    {}))

;; ------------------------------------------------------------ runtime checks

(def required-tools ["tofu" "ansible-playbook" "ssh" "curl" "openssl"])

(defn- command-present? [runner command]
  (zero? (:exit (runner ["sh" "-c" "command -v \"$1\" >/dev/null 2>&1" "sh" command] {}))))

(def account-url "https://api.vultr.com/v2/account")

(defn api-error
  "Turn one probe of the Vultr account endpoint into an error, or nil.

  The distinction is the point. A single message covering every non-2xx status
  reports a Vultr outage as a bad credential and sends the operator off to
  rotate a key that was never the problem. Only 401 and 403 say anything about
  the key. A request that never reached the API at all shows up as curl's
  literal `000`, which is not an HTTP status: that is the operator's network,
  and naming it saves the same wasted rotation."
  [{:keys [exit out]}]
  (let [status (some-> out str str/trim (as-> s (re-find #"\d{3}\z" s)) parse-long)]
    (cond
      (or (nil? status) (zero? status))
      (str "could not reach the Vultr API at " account-url
           " (curl exit " exit "): this is a local network, DNS, or TLS "
           "failure, not a credential problem. Check connectivity and retry.")

      (<= 200 status 299) nil

      (#{401 403} status)
      (str "Vultr rejected COLORS_PAR_VULTR_API_KEY (HTTP " status
           "): the key is missing, revoked, or its allowed-subnet list does "
           "not include this machine. Check the key in the Vultr console and "
           "update .envrc.private.")

      (= 429 status)
      (str "Vultr rate-limited the credential check (HTTP 429). The key is "
           "valid; wait for the limit to reset and retry.")

      (<= 500 status 599)
      (str "the Vultr API returned HTTP " status " for " account-url
           ". That is a failure on Vultr's side, not your credential — do not "
           "rotate COLORS_PAR_VULTR_API_KEY. Check https://status.vultr.com "
           "and retry.")

      :else
      (str "unexpected HTTP " status " from " account-url
           " during the credential check."))))

(defn runtime-errors
  "Check local tools and authenticate the configured Vultr key. The runner
  arity keeps command decisions testable without network access."
  ([opts] (runtime-errors opts process/run))
  ([opts runner]
   (let [present (into {} (map (fn [t] [t (command-present? runner t)])) required-tools)
         tool-errors (for [t required-tools :when (not (get present t))]
                       (str "required tool is not on PATH: " t))
         key (:vultr-api-key opts)
         ;; No `-f`: the status code is the diagnosis, so it has to survive
         ;; into stdout instead of collapsing into curl's exit code.
         result (when (and (not (missing? key)) (get present "curl"))
                  (runner ["curl" "-sS" "-o" "/dev/null" "-w" "%{http_code}"
                           "--connect-timeout" "10" "--max-time" "20"
                           "-H" (str "Authorization: Bearer " key)
                           account-url] {}))]
     (vec (concat tool-errors
                  (when-let [err (some-> result api-error)] [err]))))))
