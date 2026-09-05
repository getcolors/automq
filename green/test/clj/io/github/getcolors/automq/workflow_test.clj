(ns io.github.getcolors.automq.workflow-test
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.cluster-test :refer [params]]
            [io.github.getcolors.automq.tools :as tools]
            [io.github.getcolors.automq.validate :as validate]
            [io.github.getcolors.automq.validate-test :refer [base]]
            [io.github.getcolors.automq.workflow :as workflow]))

(defn- chain [event]
  (loop [step :automq/start seen []]
    (let [[_ next-step] (workflow/wire-fn step {:green/event event})]
      (if next-step
        (recur next-step (conj seen next-step))
        seen))))

(deftest create-resolves-addresses-before-it-needs-them
  ;; DNS needs the compute output; the brokers advertise names that must
  ;; already resolve, and the certificate is issued for those names during the
  ;; play. The order is the dependency, not a preference.
  (is (= [:automq/infrastructure :automq/ssh-config :automq/dns
          :automq/ansible :automq/acceptance]
         (chain :create))))

(deftest delete-unwinds-in-the-order-that-keeps-access
  ;; The ssh_config block goes before the destroy and the keypair after it: a
  ;; stale block is harmless, a key that predeceases its host locks the
  ;; operator out of machines that still exist. DNS goes before the destroy so
  ;; no record survives pointing at an address Vultr can hand to someone else.
  (is (= [:automq/ansible :automq/ssh-config :automq/dns
          :automq/infrastructure :automq/ssh-cleanup]
         (chain :delete))))

(deftest validate-answers-the-question-without-rendering-anything
  ;; It must work on a fresh checkout with no keypair and no state. Falling
  ;; through to the create chain would plan a compute stage that reads the
  ;; machine public key, so the check would fail on exactly the case it exists
  ;; to serve.
  (is (= [] (chain :validate))))

(deftest the-destroy-guard-is-desired-state-not-a-flag
  (let [errs (:green/err (workflow/start-step
                          {:green/event :delete :green/real? true
                           :compute-prevent-destroy true}
                          {}))]
    (is (some? errs))))

(deftest defaults-cover-every-key-an-operator-should-not-have-to-write
  (testing "the shape of the cluster and its ports have sensible defaults"
    (is (= 3 (:automq-node-count workflow/defaults)))
    (is (= 9092 (:automq-kafka-port workflow/defaults)))
    (is (= "vultr" (:provider-compute workflow/defaults))))
  (testing "but the guard defaults to protecting the deployment"
    (is (true? (:compute-prevent-destroy workflow/defaults)))))

(deftest each-tofu-stage-keys-its-own-state
  (is (not= tools/infrastructure-tool tools/dns-tool))
  (is (every? #(re-find #"^automq-" %)
              [tools/infrastructure-tool tools/dns-tool tools/ansible-tool
               tools/ansible-local-tool tools/acceptance-tool])))

;; --- the lifecycle against the compute state --------------------------------

;; The compute state is read once per run, through `tools/state-output`, on a
;; real create or delete. Every lifecycle test stubs it: nil is a readable
;; state holding no compute, a map is a recorded `params`, and a throw is a
;; backend that cannot be read. The Vultr API probe is stubbed too — these
;; tests are about the state, and they must not reach the network.
(defn- start [opts state]
  (with-redefs [tools/state-output (fn [_] state)
                validate/runtime-errors (fn [_] [])]
    (workflow/start-step opts {})))

(defn- start-unreadable [opts]
  ;; The shape `green.tofu/outputs` throws: an ex-info carrying `:dir`. Only
  ;; that is an unreadable backend; anything else propagates as a defect.
  (with-redefs [tools/state-output (fn [_] (throw (ex-info "tofu output failed: no backend" {:dir "x"})))
                validate/runtime-errors (fn [_] [])]
    (workflow/start-step opts {})))

(def credentials
  {:vultr-api-key "v" :cloudflare-api-token "c"
   :r2-access-key-id "a" :r2-secret-access-key "s"
   :automq-r2-access-key-id "k" :automq-r2-secret-access-key "z"})

(def deleting (merge base credentials {:green/event :delete :compute-prevent-destroy false}))

(deftest build-and-dry-run-never-touch-the-state
  ;; A throwing state read proves nothing on these paths reaches the backend,
  ;; and the machine key stays the placeholder rather than the operator's home.
  (doseq [opts [(assoc base :green/event :build)
                (assoc base :green/event :create :green/dry-run true)
                (assoc base :green/event :delete :green/dry-run true :compute-prevent-destroy false)]]
    (let [result (start-unreadable opts)]
      (is (= 0 (:green/exit result)) (:green/err result))
      (is (str/starts-with? (str (:ssh-public-key-path result)) "/home/build-placeholder"))
      (is (nil? (:once/cluster result)) "a build renders the fallbacks, it adopts nothing"))))

(deftest a-real-create-requires-the-credentials
  (let [r (start (assoc base :green/event :create) nil)]
    (is (= 2 (:green/exit r)))
    (is (str/includes? (:green/err r) "COLORS_PAR_VULTR_API_KEY"))
    (is (str/includes? (:green/err r) "COLORS_PAR_CLOUDFLARE_API_TOKEN"))
    (is (str/includes? (:green/err r) "COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID"))))

(deftest a-provider-switch-is-refused-before-the-credentials
  ;; Provider switching is a rebuild, never an apply. The validator order is
  ;; the thing under test: the actionable error, not a missing token for the
  ;; provider that was just selected.
  (doseq [event [:create :delete]]
    (let [r (start (assoc base :green/event event :compute-prevent-destroy false)
                   (assoc params :provider "digitalocean"))]
      (is (= 2 (:green/exit r)) (name event))
      (is (str/includes? (:green/err r)
                         "state holds a digitalocean machine; set provider-compute back to digitalocean and delete first"))
      (is (not (str/includes? (:green/err r) "required credential is not set"))))))

(deftest legacy-state-is-accepted-on-the-default-provider
  ;; A `params` recorded before this package wrote `provider` — every
  ;; pre-adoption AutoMQ state — is a Vultr cluster and needs no translation.
  (let [legacy (dissoc params :provider)]
    (let [r (start (assoc base :green/event :create) legacy)]
      (is (not (str/includes? (:green/err r) "state holds")))
      (is (str/includes? (:green/err r) "required credential is not set")))
    (let [r (start deleting legacy)]
      (is (= 0 (:green/exit r)) (:green/err r))
      (is (= legacy (:once/cluster r))))))

(deftest an-unreadable-backend-counts-as-no-state-on-create
  ;; A fresh clone has no readable state and must still be able to create.
  (let [r (start-unreadable (assoc base :green/event :create))]
    (is (= 2 (:green/exit r)))
    (is (not (str/includes? (:green/err r) "could not read")))
    (is (not (str/includes? (:green/err r) "state holds")))
    (is (str/includes? (:green/err r) "COLORS_PAR_VULTR_API_KEY"))))

(deftest a-real-create-on-a-fresh-work-directory-reports-the-credentials-not-a-crash
  ;; No state stub: the real `state-output` runs against a work directory that
  ;; holds no stage yet, as a fresh clone's does. Green's SDK shells out to
  ;; tofu in a directory that does not exist and reports that launch failure
  ;; as its own `tofu output failed:` step error; ONCE's `read-state` counts
  ;; that as an unreadable state, so the create reports its credentials
  ;; instead of crashing.
  (let [work (str (fs/create-temp-dir {:prefix "automq-fresh"}))]
    (try
      (let [r (with-redefs [validate/runtime-errors (fn [_] [])]
                (workflow/start-step (assoc base :workdir work :green/event :create) {}))]
        (is (= 2 (:green/exit r)))
        (is (str/includes? (str (:green/err r)) "COLORS_PAR_VULTR_API_KEY"))
        (is (not (str/includes? (str (:green/err r)) "could not read"))))
      (finally (fs/delete-tree work)))))

(deftest an-unreadable-backend-fails-a-real-delete-closed
  ;; Before adoption a delete proceeded on nil here and would have rendered
  ;; the cleanup play against the documentation addresses.
  (let [r (start-unreadable deleting)]
    (is (= 1 (:green/exit r)))
    (is (str/includes? (:green/err r) "could not read the infrastructure state for the delete cleanup"))
    (is (str/includes? (:green/err r) "no backend"))))

(deftest a-real-delete-adopts-the-recorded-cluster
  (let [r (start deleting params)]
    (is (= 0 (:green/exit r)) (:green/err r))
    (is (= params (:once/cluster r)) "the whole recorded params, extension keys and all")
    (is (= ["203.0.113.10" "203.0.113.11" "203.0.113.12"] (mapv :ip (tools/nodes r)))))
  (testing "a readable state without compute adopts nothing, and the cleanup play skips itself"
    (let [r (start deleting nil)]
      (is (= 0 (:green/exit r)) (:green/err r))
      (is (not (contains? r :once/cluster))))))

(deftest a-real-delete-refuses-a-state-that-does-not-describe-every-node
  ;; Three nodes are declared; a state that reports two is not a smaller
  ;; cluster to tear down but a state that cannot be trusted. ONCE's message,
  ;; unreworded.
  (let [r (start deleting (update params :nodes pop))]
    (is (= 1 (:green/exit r)))
    (is (= "the compute stage did not report nodes this package declares: 2" (:green/err r))))
  (testing "a node without an address is refused the same way"
    (let [r (start deleting (assoc-in params [:nodes 1 :vpc_ip] ""))]
      (is (= 1 (:green/exit r)))
      (is (str/includes? (:green/err r) "did not report a complete node (ip, vpc_ip, name, user, sudoer) for 1")))))
