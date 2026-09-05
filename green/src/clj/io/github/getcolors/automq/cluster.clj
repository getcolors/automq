(ns io.github.getcolors.automq.cluster
  "Everything that turns `automq-node-count` into concrete cluster facts.

  This namespace exists because a three-node cluster has far more derived
  identity than a single-node one, and every derivation is a place to be
  wrong in a way no exit code reports: a broker that advertises the wrong
  name is reachable and useless, a quorum string that disagrees between
  nodes forms no quorum at all, and a certificate whose SAN list misses one
  broker fails only for the client that happens to be routed there.

  The node set itself — how many nodes, their ids, the fallback addresses a
  `build` renders with, and the refusal of a state that does not describe
  the whole cluster — is the Compute Cluster Standard's
  (`workspace/standards/compute-cluster.md`) and is ONCE's
  `compute-cluster` namespace, called with the `spec` below and never
  copied. What stays here is AutoMQ's: broker names, the SAN list, the
  quorum string, listeners, principals and ACLs.

  Everything here is a pure function of desired state plus the compute
  stage's outputs, so the whole of it is reachable from the test suite and
  visible in the goldens. Nothing in this file may read the environment,
  the filesystem, or the network."
  (:require [clojure.string :as str]
            [io.github.getcolors.once.compute :as compute]
            [io.github.getcolors.once.compute-cluster :as once-cluster]))

;; ---------------------------------------------------------------- the spec

(def compute-providers
  "provider-compute -> what that choice implies.

  `:required` are the non-secret keys the provider's template interpolates,
  `:secrets` the credentials it needs through COLORS_PAR_*, `:tofu-env` the
  subset OpenTofu reads from the process environment itself, and `:network`
  the private network the cluster's quorum crosses — created by this package
  from `vultr-vpc-subnet`, never discovered. Keeping them together is what
  stops a provider being validated against one set of keys and run with
  another. The keys of this map are the advertised providers; Vultr is the
  only one this package has a template and a golden for.

  Two keys the template reads are deliberately not required. `vultr-name` is
  an optional override of the profile (Compute Name Standard), and
  `vultr-ssh-keys` is meaningful by its absence (SSH Keypair Standard)."
  {"vultr"
   {:required [:vultr-region :vultr-plan :vultr-os-id :vultr-vpc-subnet
               :vultr-ssh-sources :vultr-kafka-sources]
    :secrets [:vultr-api-key]
    :tofu-env {:vultr-api-key "VULTR_API_KEY"}
    :network {:mode :created :key :vultr-vpc-subnet}}})

(def default-compute-provider
  "The provider a deployment created before this package recorded one in its
  compute output must be running: the only one it ever offered."
  "vultr")

(def default-node-count 3)

(def spec
  "How this package describes itself to ONCE's `compute-cluster`. One
  homogeneous role whose count is `automq-node-count` (three by default);
  the bare `<profile>` alias reaches node 0, the default entry. `:sources`
  names the firewall lists the template reads — SSH must list at least one
  CIDR, an empty Kafka list means no public Kafka access."
  {:registry compute-providers
   :default default-compute-provider
   :sources {:non-empty ["ssh-sources"] :may-be-empty ["kafka-sources"]}
   :roles [{:role nil :count-key :automq-node-count :count default-node-count}]})

;; ------------------------------------------------------------------- names

(defn node-count
  "How many nodes the cluster has: `automq-node-count` when desired state
  carries it, else three. ONCE's; validation refuses a present value that is
  not a positive integer before any derivation runs."
  [opts]
  (once-cluster/node-count spec opts nil))

(defn indexes
  "Node indexes, `0..n-1`. The index is the KRaft `node.id`, the suffix in the
  machine label, and the ordinal in the broker name: one number, so the three
  can never disagree. ONCE's ids are 0-based per role, which is what keeps
  `node.id = index` true."
  [opts]
  (mapv :index (once-cluster/node-ids spec opts)))

(defn broker-name
  "The public name broker `i` advertises, `b<i>.<automq-host>`.

  Kafka redirects a client from the bootstrap name to whatever a broker
  advertises, so this name must resolve publicly and must appear in that
  broker's certificate. Both the DNS stage and the SAN list below derive from
  this one function."
  [opts i]
  (str (or (not-empty (str (:automq-broker-name-prefix opts))) "b")
       i "." (:automq-host opts)))

(defn broker-names [opts]
  (mapv #(broker-name opts %) (indexes opts)))

(defn certificate-names
  "The exact SAN list: the bootstrap name plus every broker name.

  Derived rather than guessed. An earlier design used a wildcard, which
  required deriving the zone from the host and left the apex needing its own
  SAN anyway; enumerating the names this cluster actually serves is both
  shorter and checkable."
  [opts]
  (into [(:automq-host opts)] (broker-names opts)))

(defn compute-name
  "The cluster's base machine name (Compute Name Standard §1-2): the profile,
  unless desired state overrides it with `vultr-name`. ONCE's, so every label
  derives from the same value."
  [opts]
  (compute/name opts))

(defn machine-name
  "The label of machine `i`, `<compute-name>-<i>`: the Cluster Standard's
  fallback name for the nil role, which is also what the template labels the
  instance. Numbered because there is more than one; the standard names the
  machine after the profile, and the index disambiguates without introducing
  a second naming scheme."
  [opts i]
  (once-cluster/fallback-node-name spec opts {:role nil :index i}))

(defn machine-names [opts]
  (mapv #(machine-name opts %) (indexes opts)))

;; --------------------------------------------------------------------- nodes

(defn- automq-node
  "One of ONCE's nodes as this package's renderers read it: `:vpc-ip` in the
  package's kebab spelling — the templates, the inventory and the quorum
  string were written against it, and adapting here keeps every rendered
  file byte-identical — plus the broker name this node advertises."
  [opts node]
  (-> node
      (dissoc :vpc_ip)
      (assoc :vpc-ip (:vpc_ip node)
             :broker-name (broker-name opts (:index node)))))

(defn fallback-nodes
  "What a credential-free `build` renders in place of a compute output:
  ONCE's fallbacks — public addresses from `192.0.2.0/24`, private ones cut
  from `vultr-vpc-subnet`, offset 10 — so a build is byte-identical on every
  workstation and the committed goldens mean something."
  [opts]
  (mapv #(automq-node opts %) (once-cluster/fallback-nodes spec opts)))

(defn nodes
  "The node list the Ansible stage and the templates consume.

  `params` is the compute stage's recorded `params` map, adopted under
  `:once/cluster` on a real run. On a build there is none, so the fallbacks
  stand in. On a real run ONCE refuses a state that does not describe every
  declared node with every field, and never substitutes a fallback: rendering
  a two-voter quorum string for a three-node cluster would produce a cluster
  that starts and then cannot elect."
  ([opts] (nodes opts (:once/cluster opts)))
  ([opts params]
   (mapv #(automq-node opts %) (once-cluster/nodes spec opts params))))

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
