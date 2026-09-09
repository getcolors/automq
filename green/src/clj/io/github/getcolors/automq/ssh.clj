(ns io.github.getcolors.automq.ssh
  "Application SSH arguments; colors-compute owns key lifecycle."
  (:require [clojure.java.io :as io] [io.github.getcolors.compute-ssh :as ssh]))
(def build-placeholder-dir "/home/build-placeholder/.ssh")
(defn rendered-only? [opts] (or (= :build (:green/event opts)) (boolean (:green/dry-run opts))))
(defn with-machine-key [opts]
  (if (not= "managed" (:mode (ssh/mode opts))) opts
      (let [path (if (rendered-only? opts) (str build-placeholder-dir "/" (:profile opts)) (:ssh-private-key-path opts))]
        (cond-> opts path (assoc :ssh-private-key-path path :ssh-public-key-path (str path ".pub"))))))
(defn identity-args [opts] (if-let [path (:ssh-private-key-path opts)] ["-i" path "-o" "IdentitiesOnly=yes"] []))
(defn private-key-path [opts]
  (when-not (:ssh-private-key-path opts) (throw (ex-info "deployment SSH identity unavailable" {})))
  (.getAbsolutePath (io/file (:ssh-private-key-path opts))))
