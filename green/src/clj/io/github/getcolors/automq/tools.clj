(ns io.github.getcolors.automq.tools
  "Compute, DNS, local SSH config, cluster convergence, and acceptance stages."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [green.ansible :as ansible]
            [green.cli :as green-cli]
            [green.process :as process]
            [green.scaffold :as sc]
            [green.tofu :as tofu]
            [green.workflow :as wf]
            [io.github.getcolors.automq.cluster :as cluster]
            [io.github.getcolors.automq.storage :as storage]
            [io.github.getcolors.automq.ssh-config :as ssh-config]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.compute-orchestration :as compute]
            [io.github.getcolors.compute-planning :as planning]
            [io.github.getcolors.once.utils :as once-utils]))

(def infrastructure-tool "automq-infrastructure")
(def dns-tool "automq-dns")
(def ansible-tool "automq-ansible")
(def ansible-local-tool "automq-ansible-local")
(def acceptance-tool "automq-acceptance")
(def root "io.github.getcolors.automq.tools")
(def template-opts sc/preserve-jinja-delimiters)

(defn tool-dir [opts tool] (green-cli/stage-dir opts tool {:default-profile "automq"}))
(defn template [path file] (keyword (str root "." path) file))
(defn spec [source target data] {:template source :target target :data data :opts template-opts})
(defn raw-spec [target content] (sc/content-spec target content))

(defn credential-env [opts & slots]
  (not-empty
   (into {} (keep (fn [[k env-var]]
                    (when-let [v (not-empty (str (get opts k)))] [env-var v])))
         (apply merge (map #(validate/tofu-env opts %) (conj (vec slots) :provider-backend))))))

(defn nodes [opts] (cluster/nodes opts))
(defn- compute-json [value indent]
  (let [padding #(apply str (repeat % " "))]
    (cond
      (map? value) (if (empty? value) "{}"
                      (str "{\n" (str/join ",\n" (for [[key item] (sort-by key value)]
                                                       (str (padding (+ indent 2)) (json/generate-string key) ": " (compute-json item (+ indent 2)))))
                           "\n" (padding indent) "}"))
      (sequential? value) (if (empty? value) "[]"
                              (str "[\n" (str/join ",\n" (map #(str (padding (+ indent 2)) (compute-json % (+ indent 2))) value)) "\n" (padding indent) "]"))
      :else (json/generate-string value))))

(defn infrastructure-step [opts]
  (try
    (let [planning? (or (= :build (:green/event opts)) (:green/dry-run opts))
          result (if planning?
                   (planning/plan-deployment opts (cluster/topology opts) (cluster/requirements opts))
                   (compute/orchestrate opts (cluster/topology opts) (cluster/requirements opts)))]
      (when planning?
        (doseq [[stage documents] (cons ["shared" (get-in result [:documents :shared])]
                                      (map (fn [[id documents]] [(str "nodes/" id) documents]) (get-in result [:documents :nodes])))
                [filename document] documents]
          (let [target (io/file (tool-dir opts infrastructure-tool) stage filename)]
            (io/make-parents target)
            (spit target (str (compute-json document 0) "\n")))))
      (if-not (contains? #{"ready" "planned" "destroyed"} (:status result))
        (assoc opts :green/exit 1 :green/err (if (seq (:errors result)) (str/join "\n" (:errors result)) "compute lifecycle refused; inspect state ownership and configuration"))
        (cond-> (assoc opts :green/exit 0)
          (:cluster result) (assoc :colors-compute/cluster (:cluster result))
          (get-in result [:key :private_key_path])
          (assoc :ssh-private-key-path (if planning? (str/replace (get-in result [:key :private_key_path]) "$HOME/.ssh" "/home/build-placeholder/.ssh") (get-in result [:key :private_key_path]))))))
    (catch Exception _ (assoc opts :green/exit 1 :green/err "compute lifecycle refused; legacy monolithic state requires explicit migration"))))

;; ---------------------------------------------------------------------- dns

(defn zone
  "The Cloudflare zone the cluster's names belong to (their registrable
  domain)."
  [opts]
  (once-utils/registrable-domain (:automq-host opts)))

(defn dns-json
  "Every A record this cluster needs.

  The bootstrap name carries one record per node, so a client that knows only
  that name reaches some broker and is redirected from there. Each broker also
  gets its own name, because that is what it advertises and what its
  certificate must cover.

  `proxied` is false on every record and is not a preference. Cloudflare's
  proxy terminates HTTP; Kafka is a raw TCP protocol on 9092, and a proxied
  record would publish an address that speaks HTTP to a client speaking
  Kafka."
  [opts nodes*]
  (tofu/constructs-json
   (concat
    (map-indexed
     (fn [i n]
       (tofu/construct :resource :cloudflare_dns_record (keyword (str "bootstrap_" i))
                       {:zone_id "${data.cloudflare_zone.zone.id}"
                        :name (:automq-host opts) :content (:ip n) :type "A"
                        :proxied false :ttl 60}))
     nodes*)
    (map (fn [n]
           (tofu/construct :resource :cloudflare_dns_record
                           (keyword (str "broker_" (:index n)))
                           {:zone_id "${data.cloudflare_zone.zone.id}"
                            :name (:broker-name n) :content (:ip n) :type "A"
                            :proxied false :ttl 60}))
         nodes*))))

(defn dns-step [opts]
  (if (= "none" (:provider-dns opts)) (assoc opts :green/exit 0)
  (let [dir (tool-dir opts dns-tool)
        nodes* (nodes opts)
        data (assoc opts :automq-zone (zone opts))
        specs [(spec (template "dns" "main.tf") (str dir "/main.tf") data)
               (raw-spec (str dir "/record.tf.json") (dns-json data nodes*))]]
    (tofu/tofu-with-spec opts specs {:dir dir :env (merge (storage/aws-env opts) (credential-env opts :provider-dns))}))))

;; ------------------------------------------------------- ssh config (local)

(defn ansible-local-data
  "Only what a `build` genuinely knows. Addresses are run-time facts and reach
  the play as extra-vars instead, so the rendered playbook carries no IP and
  is identical on every workstation (SSH Config Standard §6)."
  [opts]
  (assoc opts
         :ssh-keygen (validate/keygen? opts)
         :ssh-config-identity-file (if (validate/keygen? opts) (ssh-config/identity-file opts) (or (:ssh-private-key-path opts) ""))
         :host-alias (ssh-config/host-alias opts)))

(defn ansible-local-specs [opts]
  (let [dir (tool-dir opts ansible-local-tool) data (ansible-local-data opts)]
    [(spec (template "ansible-local" "ansible.cfg") (str dir "/ansible.cfg") data)
     (spec (template "ansible-local" "inventory.ini") (str dir "/inventory.ini") data)
     (spec (template "ansible-local" "main.yml") (str dir "/main.yml") data)]))

(defn ssh-config-hosts
  "The `~/.ssh/config` entries, as data the play loops over: the bare profile
  pointing at node 0, then one alias per normalized library node."
  [opts nodes*]
  (into [(assoc (first nodes*) :name (:profile opts))]
        (map #(assoc % :name (str (:profile opts) "-" (:index %))) nodes*)))

(defn ansible-local-step
  "Write or remove the `~/.ssh/config` block. The same playbook serves both
  events; `block_state` is what distinguishes them."
  [opts]
  (let [dir (tool-dir opts ansible-local-tool)
        delete? (= :delete (:green/event opts))]
    (ansible/ansible-with-spec opts
      {:dir dir :inventory "inventory.ini"
       :playbooks {:create "main.yml" :delete "main.yml"}
       :extra-vars {:host_alias (ssh-config/host-alias opts)
                    :ssh_hosts (ssh-config-hosts opts (nodes opts))
                    :block_state (if delete? "absent" "present")}}
      (ansible-local-specs opts))))

;; ------------------------------------------------------------------ ansible

(defn inventory
  "One host per node, each carrying the facts only it has.

  Per-node values live here rather than in the rendered templates because
  there is one template set for the whole cluster: the playbook fills
  `node.id`, the listeners and the advertised names from these variables. The
  cluster-wide values that must be *identical* everywhere — the quorum string
  above all — are rendered once into the play instead, so three nodes cannot
  disagree about them."
  [opts nodes*]
  (json/generate-string
   {:all
    {:children
     {:automq
      ;; Sorted maps throughout, and not for tidiness: an unsorted Clojure map
       ;; of this size stops preserving insertion order once it outgrows an
       ;; array-map, so adding one key silently reorders the others and every
       ;; committed golden churns for a change that altered nothing.
       {:hosts
       (into (sorted-map)
             (map (fn [n]
                    [(str (:name n))
                     (into (sorted-map)
                           (cond-> {:ansible_host (:ip n)
                                    :ansible_user (or (:user n) "root")
                                    :automq_node_id (:index n)
                                    :automq_vpc_ip (:vpc-ip n)
                                    :automq_broker_name (:broker-name n)
                                    :automq_listeners (cluster/listeners opts n)
                                    :automq_advertised_listeners (cluster/advertised-listeners opts n)
                                    ;; Node 0 is the only ACME client and the
                                    ;; only host that receives the zone-editing
                                    ;; token.
                                    :automq_cert_issuer (zero? (:index n))}
                             (validate/keygen? opts)
                             (assoc :ansible_ssh_private_key_file (:ssh-private-key-path opts))))]))
             nodes*)}}}}
   {:pretty true}))

(defn ansible-data
  "Template values for the convergence stage.

  Deliberately carries no credential. The R2 keys and the Cloudflare token
  reach the hosts as Ansible `lookup('env', ...)` expressions written
  literally into main.yml, where `preserve-jinja-delimiters` passes them
  through untouched — routing them through this map would let Selmer
  HTML-escape the quotes and hand Ansible `&#39;`. The secret therefore exists
  only in the process that needs it: not in `.colors/`, not in a golden, not
  in this map."
  [opts]
  (let [nodes* (nodes opts)
        certificate-names (if (= "none" (:provider-dns opts)) (mapv :ip nodes*) (cluster/certificate-names opts))]
    (assoc (dissoc opts :automq/storage-credentials)
           :ssh-keygen (or (validate/keygen? opts) (boolean (:ssh-private-key-path opts)))
           :node-count (cluster/node-count opts)
           :quorum-voters (cluster/quorum-voters opts nodes*)
           :automq-tls-mode (get opts :automq-tls-mode "acme")
           :certificate-names certificate-names
           :certificate-names-csv (str/join "," certificate-names)
           :bootstrap-internal (str/join ","
                                         (map #(str (:vpc-ip %) ":"
                                                    (cluster/internal-port opts))
                                              nodes*))
           :bootstrap-external (str (if (= "none" (:provider-dns opts)) (:ip (first nodes*)) (:automq-host opts)) ":" (cluster/kafka-port opts))
           :admin-user (cluster/admin-user opts)
           :broker-user (cluster/broker-user opts)
           :controller-user (cluster/controller-user opts)
           :client-user (cluster/client-user opts)
           :scram-principals (cluster/scram-principals opts)
           :super-users (cluster/super-users opts)
           :client-acls (cluster/client-acls opts)
           :topic-prefix (cluster/topic-prefix opts)
           :controller-port (cluster/controller-port opts)
           :internal-port (cluster/internal-port opts)
           :kafka-port (cluster/kafka-port opts))))

(def ansible-files
  ["ansible.cfg" "main.yml" "cleanup.yml" "compose.yml" "server.properties"
   "store.py" "secrets.sh" "render-config.sh" "format.sh" "acl.sh" "scram.sh"
   "cert.sh" "cert-deploy.sh" "cert-deploy.service" "cert-deploy.timer"
   "cert-renew.service" "cert-renew.timer"
   "status.sh" "credential.sh" "smoke.sh" "rotate.sh"])

(defn ansible-specs [opts]
  (let [dir (tool-dir opts ansible-tool) data (ansible-data opts)]
    (conj (mapv (fn [f] (spec (template "ansible" f) (str dir "/" f) data))
                ansible-files)
          (raw-spec (str dir "/inventory.json") (inventory data (nodes opts))))))

(declare process-result)

(defn ansible-step [opts]
  (let [dir (tool-dir opts ansible-tool)]
    (if (and (= :delete (:green/event opts)) (nil? (:colors-compute/cluster opts)))
      ;; A readable state without compute: there is nothing to stop, and the
      ;; cleanup play would only fail against the placeholder addresses. (An
      ;; unreadable state, or a partial one, never reaches here — the delete
      ;; failed closed at adoption.)
      (assoc opts :green/exit 0)
      (if (and (storage/managed? opts) (= :create (:green/event opts)))
        (let [rendered (sc/scaffold opts (ansible-specs opts))
              result (process/run-with-timeout
                      ["ansible-playbook" "-i" "inventory.json" "main.yml"]
                      {:dir dir :extra-env (storage/credential-env opts)} 7200000)]
          (if (zero? (:exit result))
            (assoc rendered :green/exit 0 :ansible/recap (ansible/parse-recap (:out result)))
            (assoc rendered :green/exit 1 :green/err (str "Ansible convergence failed: " (:out result) (:err result)))))
      (ansible/ansible-with-spec opts
        {:dir dir :inventory "inventory.json"
         :playbooks {:create "main.yml" :delete "cleanup.yml"}
         :host-key-checking false}
        (ansible-specs opts))))))

;; --------------------------------------------------------------- acceptance

(defn acceptance-specs [opts]
  (let [dir (tool-dir opts acceptance-tool)]
    [(spec (template "acceptance" "acceptance.sh") (str dir "/acceptance.sh")
           (ansible-data opts))]))

(defn process-result [opts label {:keys [exit out err]}]
  (if (zero? exit)
    (assoc opts :green/exit 0)
    (assoc opts :green/exit (max 1 exit)
           :green/err (str label " failed: "
                           (or (not-empty err) (not-empty out) "(no output)")))))

(defn acceptance-step
  "The operator path, proved from the workstation.

  Everything the playbook can prove, the playbook already proved on the hosts
  before the ready marker was written. What is left is what only a client
  outside the deployment can establish: that the public names resolve, that
  the certificate they serve validates, that SASL_SSL admits the client
  principal and refuses a wrong password, that the ACLs deny what they should,
  and that killing a broker which leads a partition does not lose the records
  written to it.

  Forty-five minutes, not twenty. Every wait in that script is bounded, but
  the bounds add up: the partition becoming writable again (300s), the
  survival read retried while the partition is reassigned (120s), the victim
  rejoining with bounded lag (600s), and the controller quorum re-forming
  (600s). Those are worst cases and the usual run is a fraction of them — but
  a ceiling below the sum of the parts turns a slow cluster into a killed
  test, and a killed test cannot run the trap that restarts the broker it
  stopped."
  [opts]
  (let [rendered (sc/scaffold opts (acceptance-specs opts))]
    (if (not= :create (:green/event opts))
      rendered
      (let [result (process/run-with-timeout
                    ["bash" (str (tool-dir opts acceptance-tool) "/acceptance.sh")] {} 2700000)]
        ;; The script emits public gate measurements, never client credentials.
        (when (seq (:out result)) (print (:out result)) (flush))
        (process-result rendered "acceptance" result)))))

(defn generated-cleanup-step [opts]
  (-> opts
      (sc/scaffold (ansible-specs opts))
      (sc/scaffold (acceptance-specs opts))))
