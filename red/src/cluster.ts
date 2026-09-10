import type {Opts} from 'red/workflow';
import {collect,expand,deployment_requests,plan_deployment,source_cidrs} from 'colors-compute-red';
export const defaultComputeProvider='vultr';
export const defaultNodeCount=3;
export const topology=(opts:Opts)=>[{role:null,count:opts['automq-node-count']??defaultNodeCount}];
export function requirements(opts:Opts) {
 const sources=(name:string)=>source_cidrs(opts,name,'automq-'+name);
 const ingress:any[]=[{id:'ssh',protocol:'tcp',from_port:22,to_port:22,sources:sources('ssh-sources')}];
 const kafka=sources('kafka-sources');if(kafka.length)ingress.push({id:'kafka',protocol:'tcp',from_port:kafkaPort(opts),to_port:kafkaPort(opts),sources:kafka});
 for(const [id,port] of [['controller',controllerPort(opts)],['internal',internalPort(opts)]])ingress.push({id,protocol:'tcp',from_port:port,to_port:port,sources:['private']});
 return {security:{ingress,egress:'all',private_filter:true},private:true,legacy_state_keys:[opts.profile+'/automq-infrastructure.tfstate']};
}
const requests=(opts:Opts)=>deployment_requests(opts,topology(opts),requirements(opts),{mode:'managed',public_key:'ssh-ed25519 PLACEHOLDER managed-by-colors'});
export const nodeCount=(opts:Opts):number=>topology(opts)[0].count;
export const indexes=(opts:Opts):number[]=>expand(topology(opts)).map(n=>n.index);
export const brokerName=(opts:Opts,i:number)=>`${opts['automq-broker-name-prefix']||'b'}${i}.${opts['automq-host']}`;
export const brokerNames=(opts:Opts)=>indexes(opts).map(i=>brokerName(opts,i));
export const certificateNames=(opts:Opts)=>[String(opts['automq-host']),...brokerNames(opts)];
export const computeName=(opts:Opts)=>requests(opts).shared.name;
export const machineName=(opts:Opts,i:number)=>requests(opts).nodes[i].name;
export const machineNames=(opts:Opts)=>indexes(opts).map(i=>machineName(opts,i));
export interface Node {role:string|null;index:number;name:string;ip:string;'vpc-ip':string;user:string;sudoer:string;'broker-name':string;[extra:string]:any}
function automqNode(opts:Opts,node:any):Node{const {vpc_ip,...rest}=node;return {...rest,'vpc-ip':vpc_ip,'broker-name':opts['provider-dns']==='none'?node.ip:brokerName(opts,node.index)};}
export const fallbackNodes=(opts:Opts):Node[]=>plan_deployment(opts,topology(opts),requirements(opts)).cluster.nodes.map(n=>automqNode(opts,n));
export function nodes(opts:Opts,params?:any):Node[]{
 const recorded=params??opts['colors-compute/cluster'];if(!recorded){if(opts['red/event']==='build'||opts['red/dry-run'])return fallbackNodes(opts);throw Error('compute cluster unavailable; refusing placeholder inventory');}
 const declarations=opts['red/event']==='delete'?recorded.nodes:expand(topology(opts));
 const requests=declarations.map((n:any)=>({...n,private:true,provider:opts['provider-compute']}));
 return collect(requests,recorded.nodes,requests[0].node_id).nodes.map(n=>automqNode(opts,n));
}
// ----------------------------------------------------------------- listeners

export function controllerPort(opts: Opts): number {
  return (opts["automq-controller-port"] as number) ?? 9093;
}

export function internalPort(opts: Opts): number {
  return (opts["automq-internal-port"] as number) ?? 9094;
}

export function kafkaPort(opts: Opts): number {
  return (opts["automq-kafka-port"] as number) ?? 9092;
}

// `controller.quorum.voters`, identical on every node.
//
// Static rather than dynamic: three fixed nodes are desired state, and a static
// list is what makes the rendered configuration deterministic and the goldens
// meaningful. Built from VPC addresses — the quorum never crosses the public
// interface.
export function quorumVoters(opts: Opts, list: Node[]): string {
  return list.map((n) => `${n.index}@${n["vpc-ip"]}:${controllerPort(opts)}`).join(",");
}

// `listeners` for node `n`. CONTROLLER and INTERNAL bind the VPC address
// specifically, which is why the container runs with host networking: a bridged
// container cannot bind an address that belongs only to the host. EXTERNAL
// binds every interface because it is the public endpoint.
export function listeners(opts: Opts, n: Node): string {
  return `CONTROLLER://${n["vpc-ip"]}:${controllerPort(opts)}` +
    `,INTERNAL://${n["vpc-ip"]}:${internalPort(opts)}` +
    `,EXTERNAL://0.0.0.0:${kafkaPort(opts)}`;
}

// What node `n` tells clients to come back to. INTERNAL advertises the VPC
// address; EXTERNAL advertises this broker's own public name, which must
// resolve and must be in its certificate. CONTROLLER is deliberately absent —
// Kafka rejects a controller entry in `advertised.listeners`.
export function advertisedListeners(opts: Opts, n: Node): string {
  return `INTERNAL://${n["vpc-ip"]}:${internalPort(opts)}` +
    `,EXTERNAL://${n["broker-name"]}:${kafkaPort(opts)}`;
}

// ---------------------------------------------------------------- principals

function principal(value: unknown, fallback: string): string {
  const text = String(value ?? "");
  return text.length > 0 ? text : fallback;
}

export const adminUser = (opts: Opts) => principal(opts["automq-admin-user"], "automq-admin");
export const brokerUser = (opts: Opts) => principal(opts["automq-broker-user"], "automq-broker");
export const controllerUser = (opts: Opts) =>
  principal(opts["automq-controller-user"], "automq-controller");
export const clientUser = (opts: Opts) => principal(opts["automq-sasl-user"], "automq");

// The principals bootstrapped into the metadata log by the genesis format.
//
// The controller principal is deliberately absent: it authenticates with PLAIN
// from a static JAAS file, precisely so that forming the controller quorum
// depends on nothing stored in the metadata log the quorum is trying to serve.
export function scramPrincipals(opts: Opts): string[] {
  return [adminUser(opts), brokerUser(opts), clientUser(opts)];
}

// `super.users`. The client principal is never here — it is ACL-scoped, and a
// public endpoint whose only authenticated identity is a superuser is an
// authorization hole with a password on it.
export function superUsers(opts: Opts): string {
  return [adminUser(opts), brokerUser(opts), controllerUser(opts)]
    .map((user) => `User:${user}`).join(";");
}

export function topicPrefix(opts: Opts): string {
  return principal(opts["automq-client-topic-prefix"], "colors-");
}

export interface Acl {
  principal: string;
  "resource-type": string;
  "pattern-type": string;
  name: string;
  operations: string[];
}

// The client principal's complete authority, enumerated so it can be read and
// tested rather than inferred. No Create, no Alter, no ClusterAction, no
// TransactionalId — acceptance asserts the denials as well as the grants.
export function clientAcls(opts: Opts): Acl[] {
  const user = clientUser(opts);
  const prefix = topicPrefix(opts);
  return [
    { principal: user, "resource-type": "topic", "pattern-type": "prefixed",
      name: prefix, operations: ["Describe", "Read", "Write"] },
    { principal: user, "resource-type": "group", "pattern-type": "prefixed",
      name: prefix, operations: ["Describe", "Read"] },
  ];
}
