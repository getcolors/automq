(ns io.github.getcolors.automq.ssh-config-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.automq.ssh-config :as ssh-config]))

(def opts {:profile "automq-vultr" :automq-node-count 3})

(deftest the-deployment-claims-one-alias-per-node-and-the-bare-profile
  ;; `ssh automq-vultr` is what the standard promises; the numbered aliases are
  ;; what make a quorum operable, since half of running one is reaching a
  ;; specific member.
  (is (= ["automq-vultr" "automq-vultr-0" "automq-vultr-1" "automq-vultr-2"]
         (ssh-config/aliases opts))))

(deftest the-identity-file-stays-unexpanded
  (is (= "~/.ssh/automq-vultr" (ssh-config/identity-file opts))))

(deftest a-foreign-stanza-is-found-for-any-alias-not-just-the-first
  (let [lines (str/split-lines "Host something\n  HostName 1.2.3.4\n\nHost automq-vultr-2\n  HostName 5.6.7.8\n")]
    (is (nil? (ssh-config/foreign-stanza-line lines "automq-vultr")))
    (is (= 4 (ssh-config/foreign-stanza-line lines "automq-vultr-2")))))

(deftest our-own-managed-block-is-not-foreign
  (let [lines (str/split-lines
               (str "# BEGIN automq-vultr ANSIBLE MANAGED BLOCK\n"
                    "Host automq-vultr\n  HostName 1.2.3.4\n"
                    "# END automq-vultr ANSIBLE MANAGED BLOCK\n"))]
    (is (nil? (ssh-config/foreign-stanza-line lines "automq-vultr")))))

(deftest a-global-option-above-the-first-host-blocks-the-run
  ;; The block is inserted at BOF, so it would capture such an option into one
  ;; stanza and silently narrow a setting that applied to every host.
  (is (= 1 (ssh-config/leading-option-line ["ServerAliveInterval 60" "Host x"])))
  (is (nil? (ssh-config/leading-option-line ["# a comment" "" "Host x" "  User root"])))
  (testing "an option below a Host line belongs to that host and is fine"
    (is (nil? (ssh-config/leading-option-line ["Host x" "  ServerAliveInterval 60"])))))
