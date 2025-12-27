import { ethers } from 'ethers';
import Logger from './core/logger.js';
const log=new Logger('gas-strategy');
function parsePositiveNumber(value,fallback){
if(value===undefined||value===null||value==='')return fallback;
const n=Number(value);
if(!Number.isFinite(n)||n<=0)return fallback;
return n;
}
function parsePositiveOptional(value){
if(value===undefined||value===null||value==='')return null;
const n=Number(value);
return Number.isFinite(n)&&n>0?n:null;
}
function validUrgency(u){
return u==='urgent'||u==='high'||u==='normal'?u:'normal';
}
function getPremiumPercent(urgency){
const key=urgency==='urgent'?'GAS_PREMIUM_URGENT_PERCENT':urgency==='high'?'GAS_PREMIUM_HIGH_PERCENT':'GAS_PREMIUM_NORMAL_PERCENT';
const fallback=urgency==='urgent'?50:urgency==='high'?25:10;
return parsePositiveNumber(process.env[key],fallback);
}
function getChainScoped(key,chainId){
return chainId?process.env[`${key}_${chainId}`]??process.env[key]:process.env[key];
}
function getMinBaseGwei(chainId){
const v=parsePositiveOptional(getChainScoped('GAS_MIN_BASE_GWEI',chainId));
return v??50;
}
function getMinPriorityGwei(chainId){
const v=parsePositiveOptional(getChainScoped('GAS_MIN_PRIORITY_GWEI',chainId));
return v??2;
}
function getMaxBaseGwei(chainId){
return parsePositiveOptional(getChainScoped('GAS_MAX_BASE_GWEI',chainId));
}
function getMaxPriorityGwei(chainId){
return parsePositiveOptional(getChainScoped('GAS_MAX_PRIORITY_GWEI',chainId));
}
function getMaxFeeGwei(chainId){
return parsePositiveOptional(getChainScoped('GAS_MAX_FEE_GWEI',chainId));
}
function getBaseFeeMultiplierPercent(){
const raw=process.env.GAS_BASE_FEE_MULTIPLIER;
const n=Number(raw);
let m=Number.isFinite(n)&&n>0?n:1;
if(m<1)m=1;
if(m>10)m=10;
return Math.round(m*100);
}
function applyPercent(value,percent){
const mul=BigInt(100+percent);
return value*mul/100n;
}
function applyMultiplier(value,multPercent){
const mul=BigInt(multPercent);
return value*mul/100n;
}
function clampBigInt(v,min,max){
let r=v<min?min:v;
return max!==null&&max!==undefined&&r>max?max:r;
}
export async function getSafeFees(provider,options={}){
const urgency=validUrgency(options.urgency||'normal');
try{
if(!provider||typeof provider.getFeeData!=='function')throw new Error('invalid provider');
let chainId;
try{
const net=await provider.getNetwork?.();
chainId=net?.chainId!==null&&net?.chainId!==undefined?String(net.chainId):undefined;
}catch(e){
try{log.warn('network detection failed',{err:String(e)})}catch {}
}
let feeData={};
try{
feeData=await provider.getFeeData();
}catch(e){
try{log.warn('getFeeData failed',{err:String(e)})}catch {}
}
const minBaseGwei=getMinBaseGwei(chainId);
const minPrioGwei=getMinPriorityGwei(chainId);
const maxBaseGwei=getMaxBaseGwei(chainId);
const maxPrioGwei=getMaxPriorityGwei(chainId);
const maxFeeGwei=getMaxFeeGwei(chainId);
const minBase=ethers.parseUnits(String(minBaseGwei),'gwei');
const minPrio=ethers.parseUnits(String(minPrioGwei),'gwei');
const capBase=maxBaseGwei!==null&&maxBaseGwei!==undefined?ethers.parseUnits(String(maxBaseGwei),'gwei'):null;
const capPrio=maxPrioGwei!==null&&maxPrioGwei!==undefined?ethers.parseUnits(String(maxPrioGwei),'gwei'):null;
const capFee=maxFeeGwei!==null&&maxFeeGwei!==undefined?ethers.parseUnits(String(maxFeeGwei),'gwei'):null;
const baseIncludesPriority=feeData.maxFeePerGas!==null&&feeData.maxFeePerGas!==undefined;
const rawBase=baseIncludesPriority?feeData.maxFeePerGas:(feeData.gasPrice!==null&&feeData.gasPrice!==undefined?feeData.gasPrice:minBase);
const rawPrio=feeData.maxPriorityFeePerGas!==null&&feeData.maxPriorityFeePerGas!==undefined?feeData.maxPriorityFeePerGas:minPrio;
const base0=clampBigInt(rawBase<minBase?minBase:rawBase,minBase,capBase);
const prio0=clampBigInt(rawPrio<minPrio?minPrio:rawPrio,minPrio,capPrio);
const premiumPercent=getPremiumPercent(urgency);
const multPercent=getBaseFeeMultiplierPercent();
const baseMul=applyMultiplier(base0,multPercent);
const baseWithPremium=applyPercent(baseMul,premiumPercent);
const prioWithPremium=applyPercent(prio0,premiumPercent);
let maxPriorityFeePerGas=prioWithPremium;
let maxFeePerGas=baseIncludesPriority?baseWithPremium:baseWithPremium+maxPriorityFeePerGas;
maxFeePerGas=clampBigInt(maxFeePerGas,minBase,capFee);
if(maxPriorityFeePerGas>maxFeePerGas)maxPriorityFeePerGas=maxFeePerGas;
let gasPrice=feeData.gasPrice!==null&&feeData.gasPrice!==undefined?applyPercent(feeData.gasPrice,premiumPercent):maxFeePerGas;
gasPrice=clampBigInt(gasPrice,minBase,capFee);
if(process.env.GAS_STRATEGY_DEBUG==='1'){
try{
log.info('gas',{chainId,urgency,premiumPercent,multPercent,minBaseGwei,minPrioGwei,maxBaseGwei:maxBaseGwei??null,maxPrioGwei:maxPrioGwei??null,maxFeeGwei:maxFeeGwei??null,rawBase:String(rawBase??0n),rawPrio:String(rawPrio??0n),maxFeePerGas:String(maxFeePerGas),maxPriorityFeePerGas:String(maxPriorityFeePerGas),gasPrice:String(gasPrice)});
}catch {}
}
return{maxFeePerGas,maxPriorityFeePerGas,gasPrice};
}catch(e){
const minBase=ethers.parseUnits(String(getMinBaseGwei()),'gwei');
const minPrio=ethers.parseUnits(String(getMinPriorityGwei()),'gwei');
const premiumPercent=getPremiumPercent(urgency);
const multPercent=getBaseFeeMultiplierPercent();
const baseWithPremium=applyPercent(applyMultiplier(minBase,multPercent),premiumPercent);
const prioWithPremium=applyPercent(minPrio,premiumPercent);
let maxPriorityFeePerGas=prioWithPremium;
let maxFeePerGas=baseWithPremium+maxPriorityFeePerGas;
let gasPrice=maxFeePerGas;
try{log.warn('safe-fees fallback',{err:String(e)})}catch {}
return{maxFeePerGas,maxPriorityFeePerGas,gasPrice};
}
}
