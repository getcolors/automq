(ns io.github.getcolors.automq.workflow
  "AutoMQ lifecycle DAG, validation, and package-specific backend state keys."
  (:require [green.cli :as green-cli]
            [green.dry-run :as dry-run]
            [green.lifecycle :as lifecycle]
            [green.progress :as progress]
            [green.tofu :as tofu]
            [green.workflow :as wf]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.ssh :as ssh]
            [io.github.getcolors.automq.ssh-config :as ssh-config]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.once.compute :as compute]
            [io.github.getcolors.once.compute-cluster :as once-cluster]))

(def defaults
  {:provider-compute validate/default-compute-provider
   :provider-dns "cloudflare"
   :provider-backend "local"
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
   :vultr-vpc-subnet "10.40.0.0/24"})

(defn start-step
  ([opts] (start-step opts (System/getenv)))
  ([opts env]
   ;; The state is read once, up front, on the same defaulted and overlaid
   ;; opts the validators see — the overlay is what carries the backend
   ;; credentials — and only for the two events that touch a provider. The
   ;; validator and the after-validate share the one read.
   (let [overlaid (green-cli/read-pars (merge defaults opts) env)
         context {:event (:green/event overlaid) :real? (lifecycle/real-run? overlaid)}
         state (when (compute/lifecycle-event? context)
                 (once-cluster/read-state overlaid tools/state-output))]
     (lifecycle/preflight
      opts
      {:defaults defaults :overlay green-cli/read-pars
       :validators
       [(fn [_ env _] (validate/env-errors env))
        (fn [opts _ _] (validate/state-errors opts))
        ;; Compute Provider Standard §4 before the credentials: a recorded
        ;; provider that differs from the selected one reports the actionable
        ;; error, not a missing token for the provider that was just selected.
        (fn [opts _ {:keys [event] :as ctx}]
          (when (compute/lifecycle-event? ctx)
            (once-cluster/provider-validator validate/spec opts (:params state)
                                             #(validate/secret-errors opts event))))
        (fn [opts _ {:keys [event real?]}]
          (when (and real? (= :delete event) (:compute-prevent-destroy opts))
            [(str "compute destruction is protected; set "
                  (green-cli/par-name :compute-prevent-destroy) "=false to delete")]))
        (fn [opts _ {:keys [event real?]}]
          (when (and real? (contains? #{:create :delete :validate} event))
            (validate/runtime-errors opts)))]
       :after-validate
       ;; The machine key's create matrix and the Vultr preflight run before any
       ;; template is rendered: an unowned key on disk or at the provider stops
       ;; the run while stopping is still free. Delete fills the same template
       ;; values — a destroy renders before it destroys — and adopts the
       ;; recorded cluster under `:once/cluster`, failing closed on a backend it
       ;; cannot read and on a state that does not describe every node; but it
       ;; checks no key, because its key cleanup runs after the compute destroy.
       (fn [opts _ {:keys [event real?]}]
         (cond
           (and real? (= :delete event))
           (once-cluster/adopt-state validate/spec opts :delete state)

           (and real? (= :create event))
           (let [opts (ssh/ensure-key! opts (fn [_] (:params state)))]
             (if (wf/failed? opts)
               opts
               (let [opts (ssh/preflight! (ssh/with-machine-key opts))
                     opts (if (wf/failed? opts) opts (ssh-config/preflight! opts))]
                 (if (wf/failed? opts) opts (assoc opts :green/exit 0)))))

           :else
           (assoc (ssh/with-machine-key opts) :green/exit 0)))}
      env))))

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
      :automq/dns [tools/dns-step :automq/infrastructure]
      :automq/infrastructure [tools/infrastructure-step :automq/ssh-cleanup]
      :automq/ssh-cleanup [ssh/cleanup-step])

    (case step
      :automq/start [start-step :automq/infrastructure]
      :automq/infrastructure [tools/infrastructure-step :automq/ssh-config]
      :automq/ssh-config [tools/ansible-local-step :automq/dns]
      ;; DNS before convergence, because every broker advertises a name that
      ;; must already resolve — and because the certificate is issued for
      ;; those names during the play.
      :automq/dns [tools/dns-step :automq/ansible]
      :automq/ansible [tools/ansible-step :automq/acceptance]
      :automq/acceptance [tools/acceptance-step])))

(defn backend-advice [tool]
  (tofu/conventional-backend-advice
   {:dir-fn #(tools/tool-dir % tool)
    :key-fn #(str (:profile %) "/" tool ".tfstate")}))

(def side-effecting-steps
  [:automq/infrastructure :automq/dns :automq/ssh-config :automq/ansible
   :automq/acceptance :automq/ssh-cleanup])

(def workflow
  (-> (wf/workflow {:start :automq/start :wire-fn wire-fn})
      (wf/advice-add :automq/infrastructure :before ::backend
                     (backend-advice tools/infrastructure-tool))
      (wf/advice-add :automq/dns :before ::backend
                     (backend-advice tools/dns-tool))
      progress/advise
      (dry-run/advise side-effecting-steps)))
