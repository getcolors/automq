// The deployment's machine keypair, per the workspace SSH Keypair Standard.
//
// The behaviour itself is ONCE's (red/src/ssh.ts in getcolors/once): keygen
// mode when desired state carries no `vultr-ssh-keys`, an ed25519 key named
// after the profile in `~/.ssh`, the create matrix, the Vultr REST preflight,
// and a cleanup that runs only after a successful destroy. Reusing it rather
// than reimplementing means one standard has one implementation, and a fix
// upstream reaches this package when the pin moves. See once.ts for how the
// unexported module is resolved.
//
// What is added here is a build-time placeholder. ONCE derives the key paths
// from `$HOME` and does not commit rendered output; automq does commit goldens,
// so on `build` the rendered paths must not name the operator's home directory
// or the goldens would differ per workstation. Real events use the real paths.

import { resolve } from "node:path";
import type { Opts } from "red/workflow";
import { onceSsh, type FetchFn, type Runner, type StateFn } from "./once.ts";
import { keygen } from "./validate.ts";

export type { AccountKey, FetchFn, Runner, StateFn } from "./once.ts";

// The `~/.ssh` stand-in rendered on `build`. Fixed, so a build is
// byte-identical on every workstation and the committed goldens mean something.
export const buildPlaceholderDir = "/home/build-placeholder/.ssh";

// Whether this event only renders: a `build`, or any `--dry-run`. The standard
// holds both to the same rule — neither may read, create, or require anything
// under `~/.ssh`, and both must render byte-identically whether or not the
// keypair exists. A dry-run is a create that touches nothing, so testing the
// event alone would let it reach the real key path.
export function renderedOnly(opts: Opts): boolean {
  return opts["red/event"] === "build" || Boolean(opts["red/dry-run"]);
}

// Fill the template values keygen mode owns. Opt-out opts pass through
// untouched, byte-for-byte as before the standard.
export function withMachineKey(opts: Opts): Opts {
  if (!keygen(opts)) return opts;
  const build = renderedOnly(opts);
  const filled = onceSsh.withMachineKey(opts, !build);
  if (!build) return filled;
  const profile = String(opts.profile ?? "automq");
  const prv = `${buildPlaceholderDir}/${profile}`;
  const pub = `${prv}.pub`;
  return {
    ...filled,
    "ssh-private-key-path": prv,
    "ssh-public-key-path": pub,
    "vultr-ssh-keys": pub,
  };
}

// The standard's create matrix and key generation, on a real create.
export function ensureKey(opts: Opts, stateFn: StateFn, runFn?: Runner): Promise<Opts> {
  return onceSsh.ensureKey(opts, stateFn, runFn);
}

// Refuse a real create when the Vultr account holds a key named after the
// profile that this deployment's state does not own.
export function preflight(opts: Opts, fetchFn?: FetchFn): Promise<Opts> {
  return onceSsh.preflight(opts, fetchFn);
}

// Remove the generated keypair, strictly after the compute destroy succeeded.
export function cleanupStep(opts: Opts): Opts {
  return onceSsh.cleanupStep(opts);
}

// ssh arguments selecting this deployment's key, empty in opt-out mode.
export function identityArgs(opts: Opts): string[] {
  return onceSsh.identityArgs(opts);
}

export function privateKeyPath(opts: Opts): string {
  return resolve(onceSsh.privateKeyPath(opts));
}
