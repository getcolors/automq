import {resolve} from 'node:path';
import type {Opts} from 'red/workflow';
import {keyMode} from 'colors-compute-red';
export const buildPlaceholderDir='/home/build-placeholder/.ssh';
export const renderedOnly=(opts:Opts)=>opts['red/event']==='build'||Boolean(opts['red/dry-run']);
export function withMachineKey(opts:Opts):Opts {
 if(keyMode(opts).mode!=='managed')return opts;
 const path=renderedOnly(opts)?buildPlaceholderDir+'/'+opts.profile:opts['ssh-private-key-path'];
 return {...opts,...(path?{'ssh-private-key-path':path,'ssh-public-key-path':path+'.pub'}:{})};
}
export const identityArgs=(opts:Opts):string[]=>opts['ssh-private-key-path']?['-i',opts['ssh-private-key-path'],'-o','IdentitiesOnly=yes']:[];
export function privateKeyPath(opts:Opts):string {if(!opts['ssh-private-key-path'])throw Error('deployment SSH identity unavailable');return resolve(opts['ssh-private-key-path']);}
