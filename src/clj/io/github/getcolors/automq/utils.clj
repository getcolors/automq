(ns io.github.getcolors.automq.utils
  "Launcher contract and package path helpers."
  (:require [green.cli :as green-cli]))

(def contract 1)

(defn tool-dir [opts tool]
  (green-cli/stage-dir opts tool {:default-profile "automq"}))

(defn host-alias [opts]
  (or (not-empty (str (:profile opts))) "automq"))

(defn ssh-config-path []
  (str (java.io.File. (System/getProperty "user.home") ".ssh/config")))
