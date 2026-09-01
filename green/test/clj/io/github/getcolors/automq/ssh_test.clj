(ns io.github.getcolors.automq.ssh-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.ssh :as ssh]
            [io.github.getcolors.automq.validate :as validate]))

(def keygen-opts
  {:profile "automq-vultr" :provider-compute "vultr" :green/event :build})

(deftest keygen-mode-is-the-absence-of-a-supplied-key
  (is (validate/keygen? keygen-opts))
  (is (not (validate/keygen? (assoc keygen-opts :vultr-ssh-keys "abc-123")))))

(deftest a-build-never-names-the-operators-home
  ;; Committed goldens must mean the same thing on every workstation, so a
  ;; build renders a fixed placeholder rather than reading ~/.ssh.
  (let [opts (ssh/with-machine-key keygen-opts)]
    (is (= "/home/build-placeholder/.ssh/automq-vultr" (:ssh-private-key-path opts)))
    (is (= "/home/build-placeholder/.ssh/automq-vultr.pub" (:ssh-public-key-path opts)))
    (is (not (re-find #"build-placeholder" (str (System/getenv "HOME")))))))

(deftest a-dry-run-is-held-to-the-same-rule-as-a-build
  ;; A dry-run is a create that touches nothing; testing the event alone would
  ;; let it reach the real key path.
  (is (ssh/rendered-only? {:green/event :build}))
  (is (ssh/rendered-only? {:green/event :create :green/dry-run true}))
  (is (not (ssh/rendered-only? {:green/event :create}))))

(deftest opt-out-opts-pass-through-untouched
  (let [opts (assoc keygen-opts :vultr-ssh-keys "abc-123")]
    (is (= opts (ssh/with-machine-key opts)))
    (testing "nothing about the operator's key material is invented"
      (is (nil? (:ssh-private-key-path (ssh/with-machine-key opts)))))))
