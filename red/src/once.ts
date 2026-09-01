// Resolution of ONCE's ssh module, which its package.json does not export.
//
// ONCE's index surfaces `providers`, `registrableDomain` and the rest of its
// public API, but the SSH Keypair Standard's reference implementation lives in
// red/src/ssh.ts, reachable only by path: the exports map admits bare
// specifiers for "." alone. So the module is resolved from the package entry —
// the same resolved-file technique the airflow package uses for ONCE's compute
// templates — and typed here with exactly the surface this package consumes.

import { dirname, join } from "node:path";
import type { Opts } from "red/workflow";

export interface AccountKey {
  id: string;
  name: string;
  public: string;
}

export type StateFn = (opts: Opts) => Promise<Record<string, unknown> | undefined>;
export type FetchFn = (provider: string, token: string) => Promise<AccountKey[]>;
export type Runner = (
  cmd: string[],
  opts?: { env?: Record<string, string | undefined>; timeoutMs?: number },
) => Promise<{ exit: number; out: string; err: string }>;

export interface OnceSsh {
  keygen(opts: Opts): boolean;
  withMachineKey(opts: Opts, real: boolean): Opts;
  ensureKey(opts: Opts, stateFn: StateFn, runFn?: Runner): Promise<Opts>;
  preflight(opts: Opts, fetchFn?: FetchFn): Promise<Opts>;
  cleanupStep(opts: Opts): Opts;
  identityArgs(opts: Opts): string[];
  privateKeyPath(opts: Opts): string;
  fetchAccountKeys: FetchFn;
}

const onceEntry = Bun.resolveSync("package-once-red", import.meta.dir);
export const onceSsh = (await import(join(dirname(onceEntry), "ssh.ts"))) as OnceSsh;
