(ns io.github.getcolors.automq.ssh
  "The deployment's machine keypair, per the workspace SSH Keypair Standard.

  The behaviour itself is ONCE's (`io.github.getcolors.once.ssh`): keygen mode
  when desired state carries no `vultr-ssh-keys`, an ed25519 key named after
  the profile in `~/.ssh`, the create matrix, the Vultr REST preflight, and a
  cleanup that runs only after a successful destroy. Reusing it rather than
  reimplementing means one standard has one implementation, and a fix upstream
  reaches this package when the pin moves.

  What is added here is a build-time placeholder. ONCE derives the key paths
  from `$HOME` and does not commit rendered output; automq does commit goldens,
  so on `:build` the rendered paths must not name the operator's home directory
  or the goldens would differ per workstation. Real events use the real paths."
  (:require [clojure.java.io :as io]
            [io.github.getcolors.once.ssh :as once-ssh]
            [io.github.getcolors.automq.validate :as validate]))

(def build-placeholder-dir
  "The `~/.ssh` stand-in rendered on `:build`. Fixed, so a build is
  byte-identical on every workstation and the committed goldens mean something."
  "/home/build-placeholder/.ssh")

(defn rendered-only?
  "Whether this event only renders: a `build`, or any `--dry-run`. The standard
  holds both to the same rule — neither may read, create, or require anything
  under `~/.ssh`, and both must render byte-identically whether or not the
  keypair exists. A dry-run is a create that touches nothing, so testing the
  event alone would let it reach the real key path."
  [opts]
  (or (= :build (:green/event opts))
      (boolean (:green/dry-run opts))))

(defn with-machine-key
  "Fill the template values keygen mode owns. Opt-out opts pass through
  untouched, byte-for-byte as before the standard."
  [opts]
  (if-not (validate/keygen? opts)
    opts
    (let [build? (rendered-only? opts)
          opts (once-ssh/with-machine-key opts (not build?))]
      (if-not build?
        opts
        (let [profile (or (:profile opts) "automq")
              prv (str build-placeholder-dir "/" profile)
              pub (str prv ".pub")]
          (assoc opts
                 :ssh-private-key-path prv
                 :ssh-public-key-path pub
                 :vultr-ssh-keys pub))))))

(defn ensure-key!
  "The standard's create matrix and key generation, on a real create."
  [opts state-fn]
  (once-ssh/ensure-key! opts state-fn))

(defn preflight!
  "Refuse a real create when the Vultr account holds a key named after the
  profile that this deployment's state does not own."
  [opts]
  (once-ssh/preflight! opts))

(defn cleanup-step
  "Remove the generated keypair, strictly after the compute destroy succeeded."
  [opts]
  (once-ssh/cleanup-step opts))

(defn identity-args
  "ssh arguments selecting this deployment's key, empty in opt-out mode."
  [opts]
  (once-ssh/identity-args opts))

(defn private-key-path [opts]
  (str (.getAbsolutePath (io/file (once-ssh/private-key-path opts)))))
