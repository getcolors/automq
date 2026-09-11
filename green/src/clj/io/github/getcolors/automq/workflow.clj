(ns io.github.getcolors.automq.workflow
  "AutoMQ lifecycle DAG, validation, and package-specific backend state keys."
  (:require [green.cli :as green-cli]
            [green.dry-run :as dry-run]
            [green.lifecycle :as lifecycle]
            [green.progress :as progress]
            [green.tofu :as tofu]
            [green.workflow :as wf]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.storage :as storage]
            [io.github.getcolors.automq.ssh :as ssh]
            [io.github.getcolors.automq.ssh-config :as ssh-config]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.compute-inspection :as inspection]
            [io.github.getcolors.compute :as compute]))

(def defaults
  {:provider-compute validate/default-compute-provider
   :provider-dns "cloudflare"
   :automq-tls-mode "acme"
   :provider-backend "r2"
   :compute-prevent-destroy true
   :workdir ".colors"
   :automq-node-count cluster/default-node-count
   :automq-broker-name-prefix "b"
   :automq-kafka-port 9092
   :automq-internal-port 9094
   :automq-controller-port 9093
   :automq-sasl-user "automq"
   :automq-admin-user "automq-admin"
   :automq-broker-user "automq-broker"
   :automq-controller-user "automq-controller"
   :automq-sasl-mechanism "SCRAM-SHA-512"
   :automq-client-topic-prefix "colors-"
   :automq-topic-partitions 6
   :automq-log-retention-hours 168
   :automq-r2-region "auto"
   :automq-wal-batch-interval-ms 250
   :automq-wal-max-bytes-in-batch 8388608
})

(defn start-step
  ([opts] (start-step opts (System/getenv)))
  ([opts env]
   (lifecycle/preflight opts
     {:defaults defaults :overlay green-cli/read-pars
      :validators [(fn [_ env _] (validate/env-errors env))
                   (fn [opts _ _] (validate/state-errors opts))
                   (fn [opts _ {:keys [event real?]}]
                     (when (and real? (contains? #{:create :delete :validate} event) (empty? (validate/state-errors opts)))
                       (validate/secret-errors opts event)))
                   (fn [opts _ {:keys [event real?]}]
                     (when (and real? (= :delete event) (:compute-prevent-destroy opts))
                       ["compute destruction is protected; set COLORS_PAR_COMPUTE_PREVENT_DESTROY=false to delete"]))
                   (fn [opts _ {:keys [event real?]}]
                     (when (and real? (contains? #{:create :delete :validate} event)) (validate/runtime-errors opts)))]
      :after-validate
      (fn [opts _ {:keys [event real?]}]
        (cond
          (and real? (= event :delete))
          (let [result (inspection/read-deployment opts (into {} env))]
            (if (and (= "managed" (get opts (keyword (str (:provider-backend opts) "-bucket-mode")))) (not= "present" (:status result)))
              (assoc opts :automq/finalize-only true :green/exit 0)
            (case (:status result)
              "destroyed" (assoc opts :automq/already-destroyed true :green/exit 0)
              "present" (cond-> (assoc opts :colors-compute/cluster (:cluster result) :green/exit 0)
                          (get-in result [:key :private_key_path]) (assoc :ssh-private-key-path (get-in result [:key :private_key_path])))
              (assoc opts :green/exit 1 :green/err "compute state unavailable; legacy monolithic state requires explicit migration"))))
          (and real? (= event :create)) (ssh-config/preflight! opts)
          :else (assoc (ssh/with-machine-key opts) :green/exit 0)))} env)))

(defn backend-finalize-step [opts]
  (try
    (let [result ((requiring-resolve 'io.github.getcolors.compute-managed-backend/finalize-backend!) opts)]
      (if (contains? #{"destroyed" "absent" "skipped"} (:status result))
        (assoc opts :green/exit 0)
        (assoc opts :green/exit 1 :green/err "managed backend finalization refused")))
    (catch Exception _ (assoc opts :green/exit 1 :green/err "managed backend finalization refused; live or unowned state remains"))))

(defn wire-fn [step run-opts]
  (case (:green/event run-opts)
    ;; `validate` answers "would this run?" and must not render or plan
    ;; anything to do it. Falling through to the create chain would call
    ;; `tofu validate` on a compute stage that reads the machine public key —
    ;; a file only a real create generates — so the check would fail on
    ;; exactly the fresh checkout it exists to serve.
    :validate
    (case step
      :automq/start [start-step])

    :delete
    ;; The `~/.ssh/config` block goes before the destroy, the keypair after it.
    ;; A block that outlives its host is stale but harmless; a key that
    ;; predeceases its host locks the operator out of machines that still
    ;; exist. Both orders are deliberate — standards/ssh-config.md §4 is
    ;; explicit that they must not be tidied into agreement.
    (case step
      :automq/start [start-step :automq/ansible]
      :automq/ansible [tools/ansible-step :automq/ssh-config]
      :automq/ssh-config [tools/ansible-local-step :automq/dns]
      ;; DNS goes before the compute destroy: records pointing at addresses
      ;; that have been released are worse than no records, because a reissued
      ;; address makes them point at somebody else's machine.
      :automq/dns [tools/dns-step (if (storage/managed? run-opts) :automq/storage :automq/infrastructure)]
      :automq/storage [storage/step :automq/infrastructure]
      :automq/infrastructure (cond-> [tools/infrastructure-step] (= "managed" (get run-opts (keyword (str (:provider-backend run-opts) "-bucket-mode")))) (conj :automq/backend-finalize))
      :automq/backend-finalize [backend-finalize-step])

    (case step
      :automq/start [start-step :automq/infrastructure]
      :automq/infrastructure [tools/infrastructure-step (if (storage/managed? run-opts) :automq/storage :automq/ssh-config)]
      :automq/storage [storage/step :automq/ssh-config]
      :automq/ssh-config [tools/ansible-local-step :automq/dns]
      ;; DNS before convergence, because every broker advertises a name that
      ;; must already resolve — and because the certificate is issued for
      ;; those names during the play.
      :automq/dns [tools/dns-step :automq/ansible]
      :automq/ansible [tools/ansible-step :automq/acceptance]
      :automq/acceptance [tools/acceptance-step])))

(defn backend-advice [tool]
  (fn [opts]
    ((cond
       (= "oci" (:provider-backend opts))
       (tofu/s3-backend-advice #(tools/tool-dir % tool)
         #(get-in (compute/backend-plan % (str (:profile %) "/" tool ".tfstate")) [:config :terraform :backend :s3]))
       (= "gcs" (:provider-backend opts))
       (tofu/gcs-backend-advice #(tools/tool-dir % tool)
         #(hash-map :bucket (:gcs-bucket %) :prefix (str (:profile %) "/" tool ".tfstate")))
       :else (tofu/conventional-backend-advice
         {:dir-fn #(tools/tool-dir % tool)
          :key-fn #(str (:profile %) "/" tool ".tfstate")})) opts)))

(def side-effecting-steps
  [:automq/backend-finalize :automq/storage :automq/infrastructure :automq/dns :automq/ssh-config :automq/ansible
   :automq/acceptance :automq/ssh-cleanup])

(def workflow
  (-> (wf/workflow {:start :automq/start :wire-fn wire-fn
                    :next-fn (fn [step successors opts]
                               (cond
                                 (or (:automq/already-destroyed opts) (wf/failed? opts)) []
                                 (and (= step :automq/start) (:automq/finalize-only opts)) [[:automq/backend-finalize opts]]
                                 :else (mapv #(vector % opts) successors)))})
      (wf/advice-add :automq/dns :before ::backend
                     (backend-advice tools/dns-tool))
      (wf/advice-add :automq/storage :before ::storage-backend
                     (backend-advice storage/tool))
      progress/advise
      (dry-run/advise side-effecting-steps)))
