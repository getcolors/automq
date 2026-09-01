(ns pin
  "Stamp the three bundled launchers with a clean pushed AutoMQ commit."
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]))

;; One SHA, three payloads. Every payload is born unpinned — no invented SHAs —
;; and `bb pin` stamps or re-stamps it after a clean, pushed HEAD. Each site
;; recognises exactly two forms, its unpinned birth shape and its pinned shape,
;; and the run fails loudly when a payload matches neither.

(defn git [& args]
  (let [{:keys [exit out]} (apply sh/sh "git" args)]
    (when (zero? exit) (str/trim out))))

(defn stamp-green [s sha]
  (when (re-find #"\(def \^:private automq-sha (?:nil|\"[0-9a-f]{40}\")\)" s)
    (str/replace-first s #"\(def \^:private automq-sha (?:nil|\"[0-9a-f]{40}\")\)"
                       (str "(def ^:private automq-sha \"" sha "\")"))))

(defn stamp-red [s sha]
  (let [pinned (str "\"package-automq-red\": \"github:getcolors/automq#" sha "\",")]
    (cond (str/includes? s "\"package-automq-red\": null,")
          (str/replace-first s "\"package-automq-red\": null," pinned)
          (re-find #"\"package-automq-red\": \"github:getcolors/automq#[0-9a-f]{40}\"," s)
          (str/replace-first s #"\"package-automq-red\": \"github:getcolors/automq#[0-9a-f]{40}\","
                             pinned))))

(def blue-unpinned-meta "# dependencies = []\n# ///")

(defn blue-pinned-meta [sha]
  (str "# dependencies = [\"package-automq-blue\", \"blue\"]\n"
       "#\n"
       "# [tool.uv.sources]\n"
       "# package-automq-blue = { git = \"https://github.com/getcolors/automq.git\", rev = \"" sha "\", subdirectory = \"blue\" }\n"
       "# blue = { git = \"https://github.com/getcolors/blue.git\", rev = \"290f313ead5ca162875c33a049c880da017eae09\" }\n"
       "#\n"
       ;; package-once-blue carries its own, older blue pin; the override makes
       ;; this package's blue pin win, as it does in blue/pyproject.toml.
       "# [tool.uv]\n"
       "# override-dependencies = [\"blue @ git+https://github.com/getcolors/blue.git@290f313ead5ca162875c33a049c880da017eae09\"]\n"
       "# ///"))

(defn stamp-blue [s sha]
  ;; First stamp is structural: the metadata block gains its git sources and the
  ;; UNPINNED paragraph collapses to a pinned-state note. Re-pinning is a SHA swap.
  (cond (str/includes? s blue-unpinned-meta)
        (-> s
            (str/replace-first blue-unpinned-meta (blue-pinned-meta sha))
            (str/replace-first #"(?s)# UNPINNED:.*?AUTOMQ_LIB_ROOT=/path/to/automq\n"
                               "# Stamped by `bb pin`. AUTOMQ_LIB_ROOT=/path/to/automq still overrides the\n# pin with a working tree.\n"))
        (re-find #"automq\.git\", rev = \"[0-9a-f]{40}\"" s)
        (str/replace-first s #"automq\.git\", rev = \"[0-9a-f]{40}\""
                           (str "automq.git\", rev = \"" sha "\""))))

(def sites
  [{:path "../skills/package-automq-green/green" :stamp stamp-green}
   {:path "../skills/package-automq-red/red" :stamp stamp-red}
   {:path "../skills/package-automq-blue/blue" :stamp stamp-blue}])

(let [dirty (git "status" "--porcelain")
      sha (git "rev-parse" "HEAD")
      remotes (git "branch" "-r" "--contains" (str sha))]
  (cond
    (seq dirty)
    (do (binding [*out* *err*]
          (println "automq working tree is dirty; commit before pinning"))
        (System/exit 2))

    (not (str/includes? (str remotes) "origin/"))
    (do (binding [*out* *err*]
          (println "automq HEAD" (some-> sha (subs 0 7))
                   "is not on any remote branch; push before pinning"))
        (System/exit 2))

    :else
    (let [errors (atom [])]
      (doseq [{:keys [path stamp]} sites]
        (let [s (slurp path) n (stamp s sha)]
          (if n
            (spit path n)
            (swap! errors conj (str "could not locate a pin form in " path)))))
      (if (seq @errors)
        (do (binding [*out* *err*] (println (str/join "\n" @errors)))
            (System/exit 2))
        (println "pinned 3 launchers to" (subs sha 0 7))))))
