(ns io.github.getcolors.automq.cluster
  "Everything that turns `automq-node-count` into concrete cluster facts.

  This namespace exists because a three-node cluster has far more derived
  identity than a single-node one, and every derivation is a place to be
  wrong in a way no exit code reports: a broker that advertises the wrong
  name is reachable and useless, a quorum string that disagrees between
  nodes forms no quorum at all, and a certificate whose SAN list misses one
  broker fails only for the client that happens to be routed there.

  Everything here is a pure function of desired state plus the compute
  stage's outputs, so the whole of it is reachable from the test suite and
  visible in the goldens. Nothing in this file may read the environment,
  the filesystem, or the network."
  (:require [clojure.string :as str]))

(def default-node-count 3)

(defn node-count [opts]
  (let [n (:automq-node-count opts)]
    (if (integer? n) n default-node-count)))

(defn indexes
  "Node indexes, `0..n-1`. The index is the KRaft `node.id`, the suffix in the
  machine label, and the ordinal in the broker name: one number, so the three
  can never disagree."
  [opts]
  (vec (range (node-count opts))))

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
  unless desired state overrides it with `vultr-name`."
  [opts]
  (let [override (str (:vultr-name opts))]
    (if (str/blank? (str/trim override))
      (str (:profile opts))
      override)))

(defn machine-name
  "The label of machine `i`. Numbered because there is more than one; the
  standard names the machine after the profile, and the index disambiguates
  without introducing a second naming scheme."
  [opts i]
  (str (compute-name opts) "-" i))

(defn machine-names [opts]
  (mapv #(machine-name opts %) (indexes opts)))

;; --------------------------------------------------------------------- nodes

(def fallback-node
  "What a credential-free `build` renders in place of a compute output. Fixed
  addresses from the documentation ranges (RFC 5737 / RFC 1918) so a build is
  byte-identical on every workstation and the committed goldens mean
  something."
  {:ip "192.0.2.10" :vpc-ip "10.40.0.10" :user "root" :sudoer "root"})

(defn fallback-nodes [opts]
  (mapv (fn [i]
          (assoc fallback-node
                 :index i
                 :name (machine-name opts i)
                 :ip (str "192.0.2." (+ 10 i))
                 :vpc-ip (str "10.40.0." (+ 10 i))
                 :broker-name (broker-name opts i)))
        (indexes opts)))

(defn nodes
  "The node list the Ansible stage and the templates consume.

  `params` is the compute stage's output, a list of maps keyed by index. On a
  build there is none, so the fallbacks stand in. On a real run a missing or
  short list is a hard error rather than a silent partial cluster: rendering
  a two-voter quorum string for a three-node cluster would produce a cluster
  that starts and then cannot elect."
  ([opts] (nodes opts (:automq/params opts)))
  ([opts params]
   (if (empty? params)
     (fallback-nodes opts)
     (let [by-index (into {} (map (juxt #(int (:index %)) identity)) params)]
       (mapv (fn [i]
               (let [p (get by-index i)]
                 (merge fallback-node
                        {:index i
                         :name (machine-name opts i)
                         :broker-name (broker-name opts i)}
                        (select-keys p [:ip :vpc-ip :user :sudoer]))))
             (indexes opts))))))

(defn missing-node-error
  "The error for a compute output that does not cover every index, or that
  omits an address. Returned rather than thrown so the workflow can report it
  the same way it reports every other failure."
  [opts params]
  (when (seq params)
    (let [by-index (into {} (map (juxt #(int (:index %)) identity)) params)
          missing (remove #(let [p (get by-index %)]
                             (and p
                                  (not (str/blank? (str (:ip p))))
                                  (not (str/blank? (str (:vpc-ip p))))))
                          (indexes opts))]
      (when (seq missing)
        (str "the compute stage did not report an address for node"
             (when (> (count missing) 1) "s") " "
             (str/join ", " missing)
             ". Refusing to render a partial cluster: a quorum string that "
             "names fewer voters than the cluster has will start and then "
             "fail to elect a controller.")))))

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
