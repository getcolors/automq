(ns pin
  "Stamp the bundled launcher with a clean pushed AutoMQ commit."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]))

(def ^:private site
  {:path "skills/package-automq-green/green"
   :rx #"\(def \^:private automq-sha (nil|\"[0-9a-f]{40}\")\)"})

(defn- git-out [dir & args]
  (let [{:keys [exit out]} (apply sh/sh "git" "-C" (str dir) args)]
    (when (zero? exit) (str/trim out))))

(defn- repo-head [dir label]
  (if-let [top (git-out dir "rev-parse" "--show-toplevel")]
    (let [dirty (git-out top "status" "--porcelain")
          sha (git-out top "rev-parse" "HEAD")
          remotes (git-out top "branch" "-r" "--contains" (str sha))]
      (cond
        (seq dirty) [nil (str label " working tree is dirty; commit before pinning")]
        (not (str/includes? (str remotes) "origin/"))
        [nil (str label " HEAD " (subs sha 0 7)
                  " is not on any remote branch; push before pinning")]
        :else [sha nil]))
    [nil (str label " is not a git repository: " dir)]))

(defn- current-pin [text] (second (re-find (:rx site) text)))

(defn- replace-pin [text sha]
  (let [m (re-matcher (:rx site) text)]
    (when (.find m)
      (str (subs text 0 (.start m 1)) (pr-str sha)
           (subs text (.end m 1))))))

(defn pin []
  (let [file (io/file (:path site))]
    (cond
      (not (.exists file))
      {:green/exit 2 :green/err (str "pin site is missing: " (:path site))}

      :else
      (let [text (slurp file)
            current (current-pin text)]
        (if-not current
          {:green/exit 2 :green/err (str "could not locate automq-sha in " (:path site))}
          (let [[head err] (repo-head "." "automq")]
            (cond
              err {:green/exit 2 :green/err err}
              (= (pr-str head) current)
              {:green/exit 0 :green/err (str "already pinned to " (subs head 0 7))}
              :else
              (do (spit file (replace-pin text head))
                  {:green/exit 0
                   :green/err (str "pinned " (:path site) " to "
                                   (subs head 0 7) " (was " current ")")}))))))))

(defn -main [& _]
  (let [{:green/keys [exit err]} (pin)]
    (when err
      (binding [*out* (if (zero? exit) *out* *err*)]
        (println err)))
    (System/exit exit)))

(-main)
