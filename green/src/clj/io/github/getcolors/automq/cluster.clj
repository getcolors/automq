(ns io.github.getcolors.automq.cluster
  "AutoMQ application facts derived from the shared compute contract."
  (:require [clojure.string :as str]
            [io.github.getcolors.compute :as compute]
            [io.github.getcolors.compute-deployment-request :as deployment]
            [io.github.getcolors.compute-planning :as planning]))
(def default-compute-provider "vultr")
(def default-node-count 3)
(defn topology [opts] [{:role nil :count (get opts :automq-node-count default-node-count)}])
(declare kafka-port controller-port internal-port)
(defn requirements [opts]
  (let [sources #(deployment/source-cidrs opts % (str "automq-" %)) kafka (sources "kafka-sources")]
    {:security {:ingress (into (cond-> [{:id "ssh" :protocol "tcp" :from_port 22 :to_port 22 :sources (sources "ssh-sources")}]
                                (seq kafka) (conj {:id "kafka" :protocol "tcp" :from_port (kafka-port opts) :to_port (kafka-port opts) :sources kafka}))
                              (map (fn [[id port]] {:id id :protocol "tcp" :from_port port :to_port port :sources ["private"]})
                                   [["controller" (controller-port opts)] ["internal" (internal-port opts)]]))
                :egress "all" :private_filter true}
     :private true :legacy_state_keys [(str (:profile opts) "/automq-infrastructure.tfstate")]}))
(defn- requests [opts] (deployment/deployment-requests opts (topology opts) (requirements opts) {:mode "managed" :public_key "ssh-ed25519 PLACEHOLDER managed-by-colors"}))
(defn node-count [opts] (:count (first (topology opts))))
(defn indexes [opts] (mapv :index (compute/expand (topology opts))))
(defn broker-name [opts i] (str (or (not-empty (str (:automq-broker-name-prefix opts))) "b") i "." (:automq-host opts)))
(defn broker-names [opts] (mapv #(broker-name opts %) (indexes opts)))
(defn certificate-names [opts] (into [(:automq-host opts)] (broker-names opts)))
(defn compute-name [opts] (get-in (requests opts) [:shared :name]))
(defn machine-name [opts i] (get-in (requests opts) [:nodes i :name]))
(defn machine-names [opts] (mapv #(machine-name opts %) (indexes opts)))
(defn- automq-node [opts node] (-> node (dissoc :vpc_ip) (assoc :vpc-ip (:vpc_ip node) :broker-name (if (= "none" (:provider-dns opts)) (:ip node) (broker-name opts (:index node))))))
(defn fallback-nodes [opts] (mapv #(automq-node opts %) (get-in (planning/plan-deployment opts (topology opts) (requirements opts)) [:cluster :nodes])))
(defn nodes
  ([opts] (nodes opts (:colors-compute/cluster opts)))
  ([opts params]
   (if (nil? params)
     (if (or (= :build (:green/event opts)) (:green/dry-run opts)) (fallback-nodes opts)
         (throw (ex-info "compute cluster unavailable; refusing placeholder inventory" {})))
     (let [declarations (if (= :delete (:green/event opts)) (:nodes params) (compute/expand (topology opts)))
           requests (mapv #(assoc % :private true :provider (:provider-compute opts)) declarations)
           checked (compute/collect requests (:nodes params) (:node_id (first requests)))]
       (mapv #(automq-node opts %) (:nodes checked))))))

;; ----------------------------------------------------------------- listeners

(defn controller-port [opts] (or (:automq-controller-port opts) 9093))
(defn internal-port [opts] (or (:automq-internal-port opts) 9094))
(defn kafka-port [opts] (or (:automq-kafka-port opts) 9092))

(defn quorum-voters
  "`controller.quorum.voters`, identical on every node.

  Static rather than dynamic: three fixed nodes are desired state, and a
  static list is what makes the rendered configuration deterministic and the
  goldens meaningful. Built from VPC addresses — the quorum never crosses the
  public interface."
  [opts nodes*]
  (str/join "," (map #(str (:index %) "@" (:vpc-ip %) ":" (controller-port opts))
                     nodes*)))

(defn listeners
  "`listeners` for node `n`. CONTROLLER and INTERNAL bind the VPC address
  specifically, which is why the container runs with host networking: a
  bridged container cannot bind an address that belongs only to the host.
  EXTERNAL binds every interface because it is the public endpoint."
  [opts n]
  (str "CONTROLLER://" (:vpc-ip n) ":" (controller-port opts)
       ",INTERNAL://" (:vpc-ip n) ":" (internal-port opts)
       ",EXTERNAL://0.0.0.0:" (kafka-port opts)))

(defn advertised-listeners
  "What node `n` tells clients to come back to. INTERNAL advertises the VPC
  address; EXTERNAL advertises this broker's own public name, which must
  resolve and must be in its certificate. CONTROLLER is deliberately absent —
  Kafka rejects a controller entry in `advertised.listeners`."
  [opts n]
  (str "INTERNAL://" (:vpc-ip n) ":" (internal-port opts)
       ",EXTERNAL://" (:broker-name n) ":" (kafka-port opts)))

;; ---------------------------------------------------------------- principals

(defn admin-user [opts] (or (not-empty (str (:automq-admin-user opts))) "automq-admin"))
(defn broker-user [opts] (or (not-empty (str (:automq-broker-user opts))) "automq-broker"))
(defn controller-user [opts] (or (not-empty (str (:automq-controller-user opts))) "automq-controller"))
(defn client-user [opts] (or (not-empty (str (:automq-sasl-user opts))) "automq"))

(defn scram-principals
  "The principals bootstrapped into the metadata log by the genesis format.

  The controller principal is deliberately absent: it authenticates with PLAIN
  from a static JAAS file, precisely so that forming the controller quorum
  depends on nothing stored in the metadata log the quorum is trying to
  serve."
  [opts]
  [(admin-user opts) (broker-user opts) (client-user opts)])

(defn super-users
  "`super.users`. The client principal is never here — it is ACL-scoped, and a
  public endpoint whose only authenticated identity is a superuser is an
  authorization hole with a password on it."
  [opts]
  (str/join ";" (map #(str "User:" %)
                     [(admin-user opts) (broker-user opts) (controller-user opts)])))

(defn topic-prefix [opts]
  (or (not-empty (str (:automq-client-topic-prefix opts))) "colors-"))

(defn client-acls
  "The client principal's complete authority, enumerated so it can be read and
  tested rather than inferred. No Create, no Alter, no ClusterAction, no
  TransactionalId — acceptance asserts the denials as well as the grants."
  [opts]
  (let [user (client-user opts) prefix (topic-prefix opts)]
    [{:principal user :resource-type "topic" :pattern-type "prefixed"
      :name prefix :operations ["Describe" "Read" "Write"]}
     {:principal user :resource-type "group" :pattern-type "prefixed"
      :name prefix :operations ["Describe" "Read"]}]))
