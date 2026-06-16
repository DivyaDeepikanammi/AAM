var VE=Object.defineProperty,jE=Object.defineProperties;var BE=Object.getOwnPropertyDescriptors;var Rs=Object.getOwnPropertySymbols;var Rp=Object.prototype.hasOwnProperty,Fp=Object.prototype.propertyIsEnumerable;var kp=(n,t,e)=>t in n?VE(n,t,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[t]=e,I=(n,t)=>{for(var e in t||={})Rp.call(t,e)&&kp(n,e,t[e]);if(Rs)for(var e of Rs(t))Fp.call(t,e)&&kp(n,e,t[e]);return n},ee=(n,t)=>jE(n,BE(t));var Fs=(n,t)=>{var e={};for(var i in n)Rp.call(n,i)&&t.indexOf(i)<0&&(e[i]=n[i]);if(n!=null&&Rs)for(var i of Rs(n))t.indexOf(i)<0&&Fp.call(n,i)&&(e[i]=n[i]);return e};var bn=(n,t,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(n,t)).next())});var Ze=null,Os=!1,zl=1,HE=null,Pe=Symbol("SIGNAL");function F(n){let t=Ze;return Ze=n,t}function js(){return Ze}var gi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function vi(n){if(Os)throw new Error("");if(Ze===null)return;Ze.consumerOnSignalRead(n);let t=Ze.producersTail;if(t!==void 0&&t.producer===n)return;let e,i=Ze.recomputing;if(i&&(e=t!==void 0?t.nextProducer:Ze.producers,e!==void 0&&e.producer===n)){Ze.producersTail=e,e.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===Ze&&(!i||$E(r,Ze)))return;let o=ar(Ze),s={producer:n,consumer:Ze,nextProducer:e,prevConsumer:void 0,lastReadVersion:n.version,nextConsumer:void 0};Ze.producersTail=s,t!==void 0?t.nextProducer=s:Ze.producers=s,o&&Vp(n,s)}function Op(){zl++}function Bs(n){if(!(ar(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===zl)){if(!n.producerMustRecompute(n)&&!sr(n)){Vs(n);return}n.producerRecomputeValue(n),Vs(n)}}function Gl(n){if(n.consumers===void 0)return;let t=Os;Os=!0;try{for(let e=n.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||UE(i)}}finally{Os=t}}function Wl(){return Ze?.consumerAllowSignalWrites!==!1}function UE(n){n.dirty=!0,Gl(n),n.consumerMarkedDirty?.(n)}function Vs(n){n.dirty=!1,n.lastCleanEpoch=zl}function Gn(n){return n&&Pp(n),F(n)}function Pp(n){n.producersTail=void 0,n.recomputing=!0}function yi(n,t){F(t),n&&Lp(n)}function Lp(n){n.recomputing=!1;let t=n.producersTail,e=t!==void 0?t.nextProducer:n.producers;if(e!==void 0){if(ar(n))do e=ql(e);while(e!==void 0);t!==void 0?t.nextProducer=void 0:n.producers=void 0}}function sr(n){for(let t=n.producers;t!==void 0;t=t.nextProducer){let e=t.producer,i=t.lastReadVersion;if(i!==e.version||(Bs(e),i!==e.version))return!0}return!1}function Wn(n){if(ar(n)){let t=n.producers;for(;t!==void 0;)t=ql(t)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Vp(n,t){let e=n.consumersTail,i=ar(n);if(e!==void 0?(t.nextConsumer=e.nextConsumer,e.nextConsumer=t):(t.nextConsumer=void 0,n.consumers=t),t.prevConsumer=e,n.consumersTail=t,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Vp(r.producer,r)}function ql(n){let t=n.producer,e=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:t.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(t.consumers=i,!ar(t)){let o=t.producers;for(;o!==void 0;)o=ql(o)}return e}function ar(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Hs(n){HE?.(n)}function $E(n,t){let e=t.producersTail;if(e!==void 0){let i=t.producers;do{if(i===n)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Us(n,t){return Object.is(n,t)}function so(n,t){let e=Object.create(zE);e.computation=n,t!==void 0&&(e.equal=t);let i=()=>{if(Bs(e),vi(e),e.value===oo)throw e.error;return e.value};return i[Pe]=e,Hs(e),i}var Ps=Symbol("UNSET"),Ls=Symbol("COMPUTING"),oo=Symbol("ERRORED"),zE=ee(I({},gi),{value:Ps,dirty:!0,error:null,equal:Us,kind:"computed",producerMustRecompute(n){return n.value===Ps||n.value===Ls},producerRecomputeValue(n){if(n.value===Ls)throw new Error("");let t=n.value;n.value=Ls;let e=Gn(n),i,r=!1;try{i=n.computation(),F(null),r=t!==Ps&&t!==oo&&i!==oo&&n.equal(t,i)}catch(o){i=oo,n.error=o}finally{yi(n,e)}if(r){n.value=t;return}n.value=i,n.version++}});function GE(){throw new Error}var jp=GE;function Bp(n){jp(n)}function Kl(n){jp=n}var WE=null;function Ql(n,t){let e=Object.create(ao);e.value=n,t!==void 0&&(e.equal=t);let i=()=>Hp(e);return i[Pe]=e,Hs(e),[i,s=>cr(e,s),s=>Zl(e,s)]}function Hp(n){return vi(n),n.value}function cr(n,t){Wl()||Bp(n),n.equal(n.value,t)||(n.value=t,qE(n))}function Zl(n,t){Wl()||Bp(n),cr(n,t(n.value))}var ao=ee(I({},gi),{equal:Us,value:void 0,kind:"signal"});function qE(n){n.version++,Op(),Gl(n),WE?.(n)}var Yl=ee(I({},gi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Xl(n){if(n.dirty=!1,n.version>0&&!sr(n))return;n.version++;let t=Gn(n);try{n.cleanup(),n.fn()}finally{yi(n,t)}}var Jl;function $s(){return Jl}function on(n){let t=Jl;return Jl=n,t}var Up=Symbol("NotFound");function lr(n){return n===Up||n?.name==="\u0275NotFound"}function $p(n){let t=F(null);try{return n()}finally{F(t)}}function j(n){return typeof n=="function"}function zs(n){let e=n(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Gs=zs(n=>function(e){n(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function co(n,t){if(n){let e=n.indexOf(t);0<=e&&n.splice(e,1)}}var Ne=class n{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(j(i))try{i()}catch(o){t=o instanceof Gs?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{zp(o)}catch(s){t=t??[],s instanceof Gs?t=[...t,...s.errors]:t.push(s)}}if(t)throw new Gs(t)}}add(t){var e;if(t&&t!==this)if(this.closed)zp(t);else{if(t instanceof n){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(t)}}_hasParent(t){let{_parentage:e}=this;return e===t||Array.isArray(e)&&e.includes(t)}_addParent(t){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(t),e):e?[e,t]:t}_removeParent(t){let{_parentage:e}=this;e===t?this._parentage=null:Array.isArray(e)&&co(e,t)}remove(t){let{_finalizers:e}=this;e&&co(e,t),t instanceof n&&t._removeParent(this)}};Ne.EMPTY=(()=>{let n=new Ne;return n.closed=!0,n})();var ed=Ne.EMPTY;function Ws(n){return n instanceof Ne||n&&"closed"in n&&j(n.remove)&&j(n.add)&&j(n.unsubscribe)}function zp(n){j(n)?n():n.unsubscribe()}var zt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var dr={setTimeout(n,t,...e){let{delegate:i}=dr;return i?.setTimeout?i.setTimeout(n,t,...e):setTimeout(n,t,...e)},clearTimeout(n){let{delegate:t}=dr;return(t?.clearTimeout||clearTimeout)(n)},delegate:void 0};function qs(n){dr.setTimeout(()=>{let{onUnhandledError:t}=zt;if(t)t(n);else throw n})}function bi(){}var Gp=td("C",void 0,void 0);function Wp(n){return td("E",void 0,n)}function qp(n){return td("N",n,void 0)}function td(n,t,e){return{kind:n,value:t,error:e}}var _i=null;function ur(n){if(zt.useDeprecatedSynchronousErrorHandling){let t=!_i;if(t&&(_i={errorThrown:!1,error:null}),n(),t){let{errorThrown:e,error:i}=_i;if(_i=null,e)throw i}}else n()}function Kp(n){zt.useDeprecatedSynchronousErrorHandling&&_i&&(_i.errorThrown=!0,_i.error=n)}var Ei=class extends Ne{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Ws(t)&&t.add(this)):this.destination=ZE}static create(t,e,i){return new _n(t,e,i)}next(t){this.isStopped?id(qp(t),this):this._next(t)}error(t){this.isStopped?id(Wp(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?id(Gp,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},KE=Function.prototype.bind;function nd(n,t){return KE.call(n,t)}var rd=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:e}=this;if(e.next)try{e.next(t)}catch(i){Ks(i)}}error(t){let{partialObserver:e}=this;if(e.error)try{e.error(t)}catch(i){Ks(i)}else Ks(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(e){Ks(e)}}},_n=class extends Ei{constructor(t,e,i){super();let r;if(j(t)||!t)r={next:t??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&zt.useDeprecatedNextContext?(o=Object.create(t),o.unsubscribe=()=>this.unsubscribe(),r={next:t.next&&nd(t.next,o),error:t.error&&nd(t.error,o),complete:t.complete&&nd(t.complete,o)}):r=t}this.destination=new rd(r)}};function Ks(n){zt.useDeprecatedSynchronousErrorHandling?Kp(n):qs(n)}function QE(n){throw n}function id(n,t){let{onStoppedNotification:e}=zt;e&&dr.setTimeout(()=>e(n,t))}var ZE={closed:!0,next:bi,error:QE,complete:bi};var fr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function qn(n){return n}function Qp(n){return n.length===0?qn:n.length===1?n[0]:function(e){return n.reduce((i,r)=>r(i),e)}}var q=(()=>{class n{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new n;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=XE(e)?e:new _n(e,i,r);return ur(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Zp(i),new i((r,o)=>{let s=new _n({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[fr](){return this}pipe(...e){return Qp(e)(this)}toPromise(e){return e=Zp(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return n.create=t=>new n(t),n})();function Zp(n){var t;return(t=n??zt.Promise)!==null&&t!==void 0?t:Promise}function YE(n){return n&&j(n.next)&&j(n.error)&&j(n.complete)}function XE(n){return n&&n instanceof Ei||YE(n)&&Ws(n)}function JE(n){return j(n?.lift)}function oe(n){return t=>{if(JE(t))return t.lift(function(e){try{return n(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function he(n,t,e,i,r){return new od(n,t,e,i,r)}var od=class extends Ei{constructor(t,e,i,r,o,s){super(t),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){t.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};var Yp=zs(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Q=(()=>{class n extends q{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Qs(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Yp}next(e){ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?ed:(this.currentObservers=null,o.push(e),new Ne(()=>{this.currentObservers=null,co(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new q;return e.source=this,e}}return n.create=(t,e)=>new Qs(t,e),n})(),Qs=class extends Q{constructor(t,e){super(),this.destination=t,this.source=e}next(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,t)}error(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,t)}complete(){var t,e;(e=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||e===void 0||e.call(t)}_subscribe(t){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(t))!==null&&i!==void 0?i:ed}};var wi=class extends Q{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let e=super._subscribe(t);return!e.closed&&t.next(this._value),e}getValue(){let{hasError:t,thrownError:e,_value:i}=this;if(t)throw e;return this._throwIfClosed(),i}next(t){super.next(this._value=t)}};var sd={now(){return(sd.delegate||Date).now()},delegate:void 0};var Zs=class extends Q{constructor(t=1/0,e=1/0,i=sd){super(),this._bufferSize=t,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,e)}next(t){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(t),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(t),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!t.closed;s+=i?1:2)t.next(o[s]);return this._checkFinalizedStatuses(t),e}_trimBuffer(){let{_bufferSize:t,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*t;if(t<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Di=new q(n=>n.complete());function Xp(n){return n&&j(n.schedule)}function ad(n){return n[n.length-1]}function Jp(n){return j(ad(n))?n.pop():void 0}function Kn(n){return Xp(ad(n))?n.pop():void 0}function eh(n,t){return typeof ad(n)=="number"?n.pop():t}function nh(n,t,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(u){s(u)}}function c(d){try{l(i.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(n,t||[])).next())})}function th(n){var t=typeof Symbol=="function"&&Symbol.iterator,e=t&&n[t],i=0;if(e)return e.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ci(n){return this instanceof Ci?(this.v=n,this):new Ci(n)}function ih(n,t,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(n,t||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(f){return function(v){return Promise.resolve(v).then(f,u)}}function a(f,v){i[f]&&(r[f]=function(E){return new Promise(function(D,x){o.push([f,E,D,x])>1||c(f,E)})},v&&(r[f]=v(r[f])))}function c(f,v){try{l(i[f](v))}catch(E){h(o[0][3],E)}}function l(f){f.value instanceof Ci?Promise.resolve(f.value.v).then(d,u):h(o[0][2],f)}function d(f){c("next",f)}function u(f){c("throw",f)}function h(f,v){f(v),o.shift(),o.length&&c(o[0][0],o[0][1])}}function rh(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=n[Symbol.asyncIterator],e;return t?t.call(n):(n=typeof th=="function"?th(n):n[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=n[o]&&function(s){return new Promise(function(a,c){s=n[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Ys=n=>n&&typeof n.length=="number"&&typeof n!="function";function Xs(n){return j(n?.then)}function Js(n){return j(n[fr])}function ea(n){return Symbol.asyncIterator&&j(n?.[Symbol.asyncIterator])}function ta(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function ew(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var na=ew();function ia(n){return j(n?.[na])}function ra(n){return ih(this,arguments,function*(){let e=n.getReader();try{for(;;){let{value:i,done:r}=yield Ci(e.read());if(r)return yield Ci(void 0);yield yield Ci(i)}}finally{e.releaseLock()}})}function oa(n){return j(n?.getReader)}function Ce(n){if(n instanceof q)return n;if(n!=null){if(Js(n))return tw(n);if(Ys(n))return nw(n);if(Xs(n))return iw(n);if(ea(n))return oh(n);if(ia(n))return rw(n);if(oa(n))return ow(n)}throw ta(n)}function tw(n){return new q(t=>{let e=n[fr]();if(j(e.subscribe))return e.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function nw(n){return new q(t=>{for(let e=0;e<n.length&&!t.closed;e++)t.next(n[e]);t.complete()})}function iw(n){return new q(t=>{n.then(e=>{t.closed||(t.next(e),t.complete())},e=>t.error(e)).then(null,qs)})}function rw(n){return new q(t=>{for(let e of n)if(t.next(e),t.closed)return;t.complete()})}function oh(n){return new q(t=>{sw(n,t).catch(e=>t.error(e))})}function ow(n){return oh(ra(n))}function sw(n,t){var e,i,r,o;return nh(this,void 0,void 0,function*(){try{for(e=rh(n);i=yield e.next(),!i.done;){let s=i.value;if(t.next(s),t.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}t.complete()})}function St(n,t,e,i=0,r=!1){let o=t.schedule(function(){e(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(o),!r)return o}function sa(n,t=0){return oe((e,i)=>{e.subscribe(he(i,r=>St(i,n,()=>i.next(r),t),()=>St(i,n,()=>i.complete(),t),r=>St(i,n,()=>i.error(r),t)))})}function aa(n,t=0){return oe((e,i)=>{i.add(n.schedule(()=>e.subscribe(i),t))})}function sh(n,t){return Ce(n).pipe(aa(t),sa(t))}function ah(n,t){return Ce(n).pipe(aa(t),sa(t))}function ch(n,t){return new q(e=>{let i=0;return t.schedule(function(){i===n.length?e.complete():(e.next(n[i++]),e.closed||this.schedule())})})}function lh(n,t){return new q(e=>{let i;return St(e,t,()=>{i=n[na](),St(e,t,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>j(i?.return)&&i.return()})}function ca(n,t){if(!n)throw new Error("Iterable cannot be null");return new q(e=>{St(e,t,()=>{let i=n[Symbol.asyncIterator]();St(e,t,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function dh(n,t){return ca(ra(n),t)}function uh(n,t){if(n!=null){if(Js(n))return sh(n,t);if(Ys(n))return ch(n,t);if(Xs(n))return ah(n,t);if(ea(n))return ca(n,t);if(ia(n))return lh(n,t);if(oa(n))return dh(n,t)}throw ta(n)}function En(n,t){return t?uh(n,t):Ce(n)}function lt(...n){let t=Kn(n);return En(n,t)}function cd(n,t){let e=j(n)?n:()=>n,i=r=>r.error(e());return new q(t?r=>t.schedule(i,0,r):i)}function ne(n,t){return oe((e,i)=>{let r=0;e.subscribe(he(i,o=>{i.next(n.call(t,o,r++))}))})}var{isArray:aw}=Array;function cw(n,t){return aw(t)?n(...t):n(t)}function fh(n){return ne(t=>cw(n,t))}var{isArray:lw}=Array,{getPrototypeOf:dw,prototype:uw,keys:fw}=Object;function mh(n){if(n.length===1){let t=n[0];if(lw(t))return{args:t,keys:null};if(mw(t)){let e=fw(t);return{args:e.map(i=>t[i]),keys:e}}}return{args:n,keys:null}}function mw(n){return n&&typeof n=="object"&&dw(n)===uw}function ph(n,t){return n.reduce((e,i,r)=>(e[i]=t[r],e),{})}function hh(n,t,e,i,r,o,s,a){let c=[],l=0,d=0,u=!1,h=()=>{u&&!c.length&&!l&&t.complete()},f=E=>l<i?v(E):c.push(E),v=E=>{o&&t.next(E),l++;let D=!1;Ce(e(E,d++)).subscribe(he(t,x=>{r?.(x),o?f(x):t.next(x)},()=>{D=!0},void 0,()=>{if(D)try{for(l--;c.length&&l<i;){let x=c.shift();s?St(t,s,()=>v(x)):v(x)}h()}catch(x){t.error(x)}}))};return n.subscribe(he(t,f,()=>{u=!0,h()})),()=>{a?.()}}function mr(n,t,e=1/0){return j(t)?mr((i,r)=>ne((o,s)=>t(i,o,r,s))(Ce(n(i,r))),e):(typeof t=="number"&&(e=t),oe((i,r)=>hh(i,r,n,e)))}function la(n=1/0){return mr(qn,n)}function gh(){return la(1)}function ld(...n){return gh()(En(n,Kn(n)))}function lo(...n){let t=Jp(n),{args:e,keys:i}=mh(n),r=new q(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;Ce(e[d]).subscribe(he(o,h=>{u||(u=!0,l--),a[d]=h},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(i?ph(i,a):a),o.complete())}))}});return t?r.pipe(fh(t)):r}function uo(...n){let t=Kn(n),e=eh(n,1/0),i=n;return i.length?i.length===1?Ce(i[0]):la(e)(En(i,t)):Di}var wn=new q(bi);function Mt(n,t){return oe((e,i)=>{let r=0;e.subscribe(he(i,o=>n.call(t,o,r++)&&i.next(o)))})}function Qn(n){return oe((t,e)=>{let i=null,r=!1,o;i=t.subscribe(he(e,void 0,void 0,s=>{o=Ce(n(s,Qn(n)(t))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function dd(n,t){return j(t)?mr(n,t,1):mr(n,1)}function Dn(n){return n<=0?()=>Di:oe((t,e)=>{let i=0;t.subscribe(he(e,r=>{++i<=n&&(e.next(r),n<=i&&e.complete())}))})}function ud(n,t=qn){return n=n??pw,oe((e,i)=>{let r,o=!0;e.subscribe(he(i,s=>{let a=t(s);(o||!n(r,a))&&(o=!1,r=a,i.next(s))}))})}function pw(n,t){return n===t}function fo(n){return oe((t,e)=>{try{t.subscribe(e)}finally{e.add(n)}})}function fd(){return oe((n,t)=>{let e,i=!1;n.subscribe(he(t,r=>{let o=e;e=r,i&&t.next([o,r]),i=!0}))})}function mo(n={}){let{connector:t=()=>new Q,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=n;return o=>{let s,a,c,l=0,d=!1,u=!1,h=()=>{a?.unsubscribe(),a=void 0},f=()=>{h(),s=c=void 0,d=u=!1},v=()=>{let E=s;f(),E?.unsubscribe()};return oe((E,D)=>{l++,!u&&!d&&h();let x=c=c??t();D.add(()=>{l--,l===0&&!u&&!d&&(a=md(v,r))}),x.subscribe(D),!s&&l>0&&(s=new _n({next:X=>x.next(X),error:X=>{u=!0,h(),a=md(f,e,X),x.error(X)},complete:()=>{d=!0,h(),a=md(f,i),x.complete()}}),Ce(E).subscribe(s))})(o)}}function md(n,t,...e){if(t===!0){n();return}if(t===!1)return;let i=new _n({next:()=>{i.unsubscribe(),n()}});return Ce(t(...e)).subscribe(i)}function pd(n,t,e){let i,r=!1;return n&&typeof n=="object"?{bufferSize:i=1/0,windowTime:t=1/0,refCount:r=!1,scheduler:e}=n:i=n??1/0,mo({connector:()=>new Zs(i,t,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function hd(n){return Mt((t,e)=>n<=e)}function gd(...n){let t=Kn(n);return oe((e,i)=>{(t?ld(n,e,t):ld(n,e)).subscribe(i)})}function Ii(n,t){return oe((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(he(i,c=>{r?.unsubscribe();let l=0,d=o++;Ce(n(c,d)).subscribe(r=he(i,u=>i.next(t?t(c,u,d,l++):u),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function xi(n){return oe((t,e)=>{Ce(n).subscribe(he(e,()=>e.complete(),bi)),!e.closed&&t.subscribe(e)})}function po(n,t,e){let i=j(n)||t||e?{next:n,error:t,complete:e}:n;return i?oe((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(he(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):qn}var ga="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",_=class extends Error{code;constructor(t,e){super(Xn(t,e)),this.code=t}};function hw(n){return`NG0${Math.abs(n)}`}function Xn(n,t){return`${hw(n)}${t?": "+t:""}`}function ce(n){for(let t in n)if(n[t]===ce)return t;throw Error("")}function Eh(n,t){for(let e in t)t.hasOwnProperty(e)&&!n.hasOwnProperty(e)&&(n[e]=t[e])}function va(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(va).join(", ")}]`;if(n==null)return""+n;let t=n.overriddenName||n.name;if(t)return`${t}`;let e=n.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function ya(n,t){return n?t?`${n} ${t}`:n:t||""}var gw=ce({__forward_ref__:ce});function _t(n){return n.__forward_ref__=_t,n}function ze(n){return Td(n)?n():n}function Td(n){return typeof n=="function"&&n.hasOwnProperty(gw)&&n.__forward_ref__===_t}function P(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function z(n){return{providers:n.providers||[],imports:n.imports||[]}}function ba(n){return vw(n,_a)}function vw(n,t){return n.hasOwnProperty(t)&&n[t]||null}function yw(n){let t=n?.[_a]??null;return t||null}function yd(n){return n&&n.hasOwnProperty(ua)?n[ua]:null}var _a=ce({\u0275prov:ce}),ua=ce({\u0275inj:ce}),w=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,e){this._desc=t,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=P({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ad(n){return n&&!!n.\u0275providers}var Nd=ce({\u0275cmp:ce}),kd=ce({\u0275dir:ce}),Rd=ce({\u0275pipe:ce});var go=ce({\u0275fac:ce}),Ni=ce({__NG_ELEMENT_ID__:ce}),vh=ce({__NG_ENV_ID__:ce});function Jn(n){return Od(n,"@Component"),n[Nd]||null}function Fd(n){return Od(n,"@Directive"),n[kd]||null}function wh(n){return Od(n,"@Pipe"),n[Rd]||null}function Od(n,t){if(n==null)throw new _(-919,!1)}function _o(n){return typeof n=="string"?n:n==null?"":String(n)}var Dh=ce({ngErrorCode:ce}),bw=ce({ngErrorMessage:ce}),_w=ce({ngTokenPath:ce});function Pd(n,t){return Ch("",-200,t)}function Ea(n,t){throw new _(-201,!1)}function Ch(n,t,e){let i=new _(t,n);return i[Dh]=t,i[bw]=n,e&&(i[_w]=e),i}function Ew(n){return n[Dh]}var bd;function Ih(){return bd}function st(n){let t=bd;return bd=n,t}function Ld(n,t,e){let i=ba(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(t!==void 0)return t;Ea(n,"")}var an=globalThis;var ww={},Si=ww,Dw="__NG_DI_FLAG__",_d=class{injector;constructor(t){this.injector=t}retrieve(t,e){let i=Mi(e)||0;try{return this.injector.get(t,i&8?null:Si,i)}catch(r){if(lr(r))return r;throw r}}};function Cw(n,t=0){let e=$s();if(e===void 0)throw new _(-203,!1);if(e===null)return Ld(n,void 0,t);{let i=Iw(t),r=e.retrieve(n,i);if(lr(r)){if(i.optional)return null;throw r}return r}}function M(n,t=0){return(Ih()||Cw)(ze(n),t)}function m(n,t){return M(n,Mi(t))}function Mi(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Iw(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Ed(n){let t=[];for(let e=0;e<n.length;e++){let i=ze(n[e]);if(Array.isArray(i)){if(i.length===0)throw new _(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=xw(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}t.push(M(r,o))}else t.push(M(i))}return t}function xw(n){return n[Dw]}function Zn(n,t){let e=n.hasOwnProperty(go);return e?n[go]:null}function xh(n,t,e){if(n.length!==t.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],o=t[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Sh(n){return n.flat(Number.POSITIVE_INFINITY)}function wa(n,t){n.forEach(e=>Array.isArray(e)?wa(e,t):t(e))}function Vd(n,t,e){t>=n.length?n.push(e):n.splice(t,0,e)}function Eo(n,t){return t>=n.length-1?n.pop():n.splice(t,1)[0]}function Mh(n,t){let e=[];for(let i=0;i<n;i++)e.push(t);return e}function Th(n,t,e,i){let r=n.length;if(r==t)n.push(e,i);else if(r===1)n.push(i,n[0]),n[0]=e;else{for(r--,n.push(n[r-1],n[r]);r>t;){let o=r-2;n[r]=n[o],r--}n[t]=e,n[t+1]=i}}function Da(n,t,e){let i=gr(n,t);return i>=0?n[i|1]=e:(i=~i,Th(n,i,t,e)),i}function Ca(n,t){let e=gr(n,t);if(e>=0)return n[e|1]}function gr(n,t){return Sw(n,t,1)}function Sw(n,t,e){let i=0,r=n.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=n[o<<e];if(t===s)return o<<e;s>t?r=o:i=o+1}return~(r<<e)}var ei={},Ye=[],vr=new w(""),wo=new w("",-1),jd=new w(""),hr=class{get(t,e=Si){if(e===Si){let r=Ch("",-201);throw r.name="\u0275NotFound",r}return e}};function ti(n){return{\u0275providers:n}}function Ia(...n){return{\u0275providers:Bd(!0,n),\u0275fromNgModule:!0}}function Bd(n,...t){let e=[],i=new Set,r,o=s=>{e.push(s)};return wa(t,s=>{let a=s;fa(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Ah(r,o),e}function Ah(n,t){for(let e=0;e<n.length;e++){let{ngModule:i,providers:r}=n[e];Hd(r,o=>{t(o,i)})}}function fa(n,t,e,i){if(n=ze(n),!n)return!1;let r=null,o=yd(n),s=!o&&Jn(n);if(!o&&!s){let c=n.ngModule;if(o=yd(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=n}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)fa(l,t,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;wa(o.imports,d=>{fa(d,t,e,i)&&(l||=[],l.push(d))}),l!==void 0&&Ah(l,t)}if(!a){let l=Zn(r)||(()=>new r);t({provide:r,useFactory:l,deps:Ye},r),t({provide:jd,useValue:r,multi:!0},r),t({provide:vr,useValue:()=>M(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=n;Hd(c,d=>{t(d,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Hd(n,t){for(let e of n)Ad(e)&&(e=e.\u0275providers),Array.isArray(e)?Hd(e,t):t(e)}var Mw=ce({provide:String,useValue:ce});function Nh(n){return n!==null&&typeof n=="object"&&Mw in n}function Tw(n){return!!(n&&n.useExisting)}function Aw(n){return!!(n&&n.useFactory)}function Ti(n){return typeof n=="function"}function kh(n){return!!n.useClass}var Do=new w(""),da={},yh={},vd;function yr(){return vd===void 0&&(vd=new hr),vd}var Ge=class{},Ai=class extends Ge{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Dd(t,s=>this.processProvider(s)),this.records.set(wo,pr(void 0,this)),r.has("environment")&&this.records.set(Ge,pr(void 0,this));let o=this.records.get(Do);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(jd,Ye,{self:!0}))}retrieve(t,e){let i=Mi(e)||0;try{return this.get(t,Si,i)}catch(r){if(lr(r))return r;throw r}}destroy(){ho(this),this._destroyed=!0;let t=F(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),F(t)}}onDestroy(t){return ho(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){ho(this);let e=on(this),i=st(void 0),r;try{return t()}finally{on(e),st(i)}}get(t,e=Si,i){if(ho(this),t.hasOwnProperty(vh))return t[vh](this);let r=Mi(i),o,s=on(this),a=st(void 0);try{if(!(r&4)){let l=this.records.get(t);if(l===void 0){let d=Ow(t)&&ba(t);d&&this.injectableDefInScope(d)?l=pr(wd(t),da):l=null,this.records.set(t,l)}if(l!=null)return this.hydrate(t,l,r)}let c=r&2?yr():this.parent;return e=r&8&&e===Si?null:e,c.get(t,e)}catch(c){let l=Ew(c);throw l===-200||l===-201?new _(l,null):c}finally{st(a),on(s)}}resolveInjectorInitializers(){let t=F(null),e=on(this),i=st(void 0),r;try{let o=this.get(vr,Ye,{self:!0});for(let s of o)s()}finally{on(e),st(i),F(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=ze(t);let e=Ti(t)?t:ze(t&&t.provide),i=kw(t);if(!Ti(t)&&t.multi===!0){let r=this.records.get(e);r||(r=pr(void 0,da,!0),r.factory=()=>Ed(r.multi),this.records.set(e,r)),e=t,r.multi.push(t)}this.records.set(e,i)}hydrate(t,e,i){let r=F(null);try{if(e.value===yh)throw Pd("");return e.value===da&&(e.value=yh,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Fw(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{F(r)}}injectableDefInScope(t){if(!t.providedIn)return!1;let e=ze(t.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(t){let e=this._onDestroyHooks.indexOf(t);e!==-1&&this._onDestroyHooks.splice(e,1)}};function wd(n){let t=ba(n),e=t!==null?t.factory:Zn(n);if(e!==null)return e;if(n instanceof w)throw new _(-204,!1);if(n instanceof Function)return Nw(n);throw new _(-204,!1)}function Nw(n){if(n.length>0)throw new _(-204,!1);let e=yw(n);return e!==null?()=>e.factory(n):()=>new n}function kw(n){if(Nh(n))return pr(void 0,n.useValue);{let t=Ud(n);return pr(t,da)}}function Ud(n,t,e){let i;if(Ti(n)){let r=ze(n);return Zn(r)||wd(r)}else if(Nh(n))i=()=>ze(n.useValue);else if(Aw(n))i=()=>n.useFactory(...Ed(n.deps||[]));else if(Tw(n))i=(r,o)=>M(ze(n.useExisting),o!==void 0&&o&8?8:void 0);else{let r=ze(n&&(n.useClass||n.provide));if(Rw(n))i=()=>new r(...Ed(n.deps));else return Zn(r)||wd(r)}return i}function ho(n){if(n.destroyed)throw new _(-205,!1)}function pr(n,t,e=!1){return{factory:n,value:t,multi:e?[]:void 0}}function Rw(n){return!!n.deps}function Fw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Ow(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Dd(n,t){for(let e of n)Array.isArray(e)?Dd(e,t):e&&Ad(e)?Dd(e.\u0275providers,t):t(e)}function br(n,t){let e;n instanceof Ai?(ho(n),e=n):e=new _d(n);let i,r=on(e),o=st(void 0);try{return t()}finally{on(r),st(o)}}function Rh(){return Ih()!==void 0||$s()!=null}var Gt=0,A=1,R=2,je=3,Tt=4,Xe=5,_r=6,Er=7,We=8,xn=9,cn=10,_e=11,wr=12,$d=13,ki=14,ut=15,ni=16,Ri=17,ln=18,Sn=19,zd=20,Cn=21,xa=22,Yn=23,Et=24,Fi=25,ii=26,Me=27,Fh=1;var ri=7,Co=8,Oi=9,qe=10;function Mn(n){return Array.isArray(n)&&typeof n[Fh]=="object"}function At(n){return Array.isArray(n)&&n[Fh]===!0}function Gd(n){return(n.flags&4)!==0}function Tn(n){return n.componentOffset>-1}function Io(n){return(n.flags&1)===1}function dn(n){return!!n.template}function Dr(n){return(n[R]&512)!==0}function Pi(n){return(n[R]&256)===256}var Wd="svg",Oh="math";function Nt(n){for(;Array.isArray(n);)n=n[Gt];return n}function qd(n,t){return Nt(t[n])}function kt(n,t){return Nt(t[n.index])}function Sa(n,t){return n.data[t]}function Kd(n,t){return n[t]}function Qd(n,t,e,i){e>=n.data.length&&(n.data[e]=null,n.blueprint[e]=null),t[e]=i}function Rt(n,t){let e=t[n];return Mn(e)?e:e[Gt]}function Ph(n){return(n[R]&4)===4}function Ma(n){return(n[R]&128)===128}function Lh(n){return At(n[je])}function un(n,t){return t==null?null:n[t]}function Zd(n){n[Ri]=0}function Yd(n){n[R]&1024||(n[R]|=1024,Ma(n)&&Li(n))}function Vh(n,t){for(;n>0;)t=t[ki],n--;return t}function xo(n){return!!(n[R]&9216||n[Et]?.dirty)}function Ta(n){n[cn].changeDetectionScheduler?.notify(8),n[R]&64&&(n[R]|=1024),xo(n)&&Li(n)}function Li(n){n[cn].changeDetectionScheduler?.notify(0);let t=In(n);for(;t!==null&&!(t[R]&8192||(t[R]|=8192,!Ma(t)));)t=In(t)}function Aa(n,t){if(Pi(n))throw new _(911,!1);n[Cn]===null&&(n[Cn]=[]),n[Cn].push(t)}function jh(n,t){if(n[Cn]===null)return;let e=n[Cn].indexOf(t);e!==-1&&n[Cn].splice(e,1)}function In(n){let t=n[je];return At(t)?t[je]:t}function Xd(n){return n[Er]??=[]}function Jd(n){return n.cleanup??=[]}function Bh(n,t,e,i){let r=Xd(t);r.push(e),n.firstCreatePass&&Jd(n).push(i,r.length-1)}var V={lFrame:Jh(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Cd=!1;function Hh(){return V.lFrame.elementDepthCount}function Uh(){V.lFrame.elementDepthCount++}function eu(){V.lFrame.elementDepthCount--}function tu(){return V.bindingsEnabled}function nu(){return V.skipHydrationRootTNode!==null}function iu(n){return V.skipHydrationRootTNode===n}function ru(){V.skipHydrationRootTNode=null}function L(){return V.lFrame.lView}function Te(){return V.lFrame.tView}function Je(n){return V.lFrame.contextLView=n,n[We]}function et(n){return V.lFrame.contextLView=null,n}function Be(){let n=ou();for(;n!==null&&n.type===64;)n=n.parent;return n}function ou(){return V.lFrame.currentTNode}function $h(){let n=V.lFrame,t=n.currentTNode;return n.isParent?t:t.parent}function Cr(n,t){let e=V.lFrame;e.currentTNode=n,e.isParent=t}function su(){return V.lFrame.isParent}function au(){V.lFrame.isParent=!1}function zh(){return V.lFrame.contextLView}function cu(){return Cd}function vo(n){let t=Cd;return Cd=n,t}function Gh(){let n=V.lFrame,t=n.bindingRootIndex;return t===-1&&(t=n.bindingRootIndex=n.tView.bindingStartIndex),t}function Wh(){return V.lFrame.bindingIndex}function qh(n){return V.lFrame.bindingIndex=n}function Ir(){return V.lFrame.bindingIndex++}function Na(n){let t=V.lFrame,e=t.bindingIndex;return t.bindingIndex=t.bindingIndex+n,e}function Kh(){return V.lFrame.inI18n}function Qh(n,t){let e=V.lFrame;e.bindingIndex=e.bindingRootIndex=n,ka(t)}function Zh(){return V.lFrame.currentDirectiveIndex}function ka(n){V.lFrame.currentDirectiveIndex=n}function Yh(n){let t=V.lFrame.currentDirectiveIndex;return t===-1?null:n[t]}function Ra(){return V.lFrame.currentQueryIndex}function So(n){V.lFrame.currentQueryIndex=n}function Pw(n){let t=n[A];return t.type===2?t.declTNode:t.type===1?n[Xe]:null}function lu(n,t,e){if(e&4){let r=t,o=n;for(;r=r.parent,r===null&&!(e&1);)if(r=Pw(o),r===null||(o=o[ki],r.type&10))break;if(r===null)return!1;t=r,n=o}let i=V.lFrame=Xh();return i.currentTNode=t,i.lView=n,!0}function Fa(n){let t=Xh(),e=n[A];V.lFrame=t,t.currentTNode=e.firstChild,t.lView=n,t.tView=e,t.contextLView=n,t.bindingIndex=e.bindingStartIndex,t.inI18n=!1}function Xh(){let n=V.lFrame,t=n===null?null:n.child;return t===null?Jh(n):t}function Jh(n){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=t),t}function eg(){let n=V.lFrame;return V.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var du=eg;function Oa(){let n=eg();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function tg(n){return(V.lFrame.contextLView=Vh(n,V.lFrame.contextLView))[We]}function An(){return V.lFrame.selectedIndex}function oi(n){V.lFrame.selectedIndex=n}function Mo(){let n=V.lFrame;return Sa(n.tView,n.selectedIndex)}function To(){V.lFrame.currentNamespace=Wd}function uu(){return V.lFrame.currentNamespace}var ng=!0;function Pa(){return ng}function La(n){ng=n}function Id(n,t=null,e=null,i){let r=ig(n,t,e,i);return r.resolveInjectorInitializers(),r}function ig(n,t=null,e=null,i,r=new Set){let o=[e||Ye,Ia(n)],s;return new Ai(o,t||yr(),s||null,r)}var Ie=class n{static THROW_IF_NOT_FOUND=Si;static NULL=new hr;static create(t,e){if(Array.isArray(t))return Id({name:""},e,t,"");{let i=t.name??"";return Id({name:i},t.parent,t.providers,i)}}static \u0275prov=P({token:n,providedIn:"any",factory:()=>M(wo)});static __NG_ELEMENT_ID__=-1},Z=new w(""),fn=(()=>{class n{static __NG_ELEMENT_ID__=Lw;static __NG_ENV_ID__=e=>e}return n})(),ma=class extends fn{_lView;constructor(t){super(),this._lView=t}get destroyed(){return Pi(this._lView)}onDestroy(t){let e=this._lView;return Aa(e,t),()=>jh(e,t)}};function Lw(){return new ma(L())}var rg=!1,og=new w(""),Vi=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new wi(!1);debugTaskTracker=m(og,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new q(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=P({token:n,providedIn:"root",factory:()=>new n})}return n})(),xd=class extends Q{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,Rh()&&(this.destroyRef=m(fn,{optional:!0})??void 0,this.pendingTasks=m(Vi,{optional:!0})??void 0)}emit(t){let e=F(null);try{super.next(t)}finally{F(e)}}subscribe(t,e,i){let r=t,o=e||(()=>null),s=i;if(t&&typeof t=="object"){let c=t;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return t instanceof Ne&&t.add(a),a}wrapInTimeout(t){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{t(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},ve=xd;function pa(...n){}function fu(n){let t,e;function i(){n=pa;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),t!==void 0&&clearTimeout(t)}catch(r){}}return t=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{n(),i()})),()=>i()}function sg(n){return queueMicrotask(()=>n()),()=>{n=pa}}var mu="isAngularZone",yo=mu+"_ID",Vw=0,O=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ve(!1);onMicrotaskEmpty=new ve(!1);onStable=new ve(!1);onError=new ve(!1);constructor(t){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=rg}=t;if(typeof Zone>"u")throw new _(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,Hw(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(mu)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new _(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new _(909,!1)}run(t,e,i){return this._inner.run(t,e,i)}runTask(t,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,t,jw,pa,pa);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(t,e,i){return this._inner.runGuarded(t,e,i)}runOutsideAngular(t){return this._outer.run(t)}},jw={};function pu(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function Bw(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function t(){fu(()=>{n.callbackScheduled=!1,Sd(n),n.isCheckStableRunning=!0,pu(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{t()}):n._outer.run(()=>{t()}),Sd(n)}function Hw(n){let t=()=>{Bw(n)},e=Vw++;n._inner=n._inner.fork({name:"angular",properties:{[mu]:!0,[yo]:e,[yo+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(Uw(c))return i.invokeTask(o,s,a,c);try{return bh(n),i.invokeTask(o,s,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&t(),_h(n)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return bh(n),i.invoke(o,s,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!$w(c)&&t(),_h(n)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,Sd(n),pu(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function Sd(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function bh(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function _h(n){n._nesting--,pu(n)}var bo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ve;onMicrotaskEmpty=new ve;onStable=new ve;onError=new ve;run(t,e,i){return t.apply(e,i)}runGuarded(t,e,i){return t.apply(e,i)}runOutsideAngular(t){return t()}runTask(t,e,i,r){return t.apply(e,i)}};function Uw(n){return ag(n,"__ignore_ng_zone__")}function $w(n){return ag(n,"__scheduler_tick__")}function ag(n,t){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[t]===!0}var dt=class{_console=console;handleError(t){this._console.error("ERROR",t)}},si=new w("",{factory:()=>{let n=m(O),t=m(Ge),e;return i=>{n.runOutsideAngular(()=>{t.destroyed&&!e?setTimeout(()=>{throw i}):(e??=t.get(dt),e.handleError(i))})}}}),cg={provide:vr,useValue:()=>{let n=m(dt,{optional:!0})},multi:!0};function xe(n,t){let[e,i,r]=Ql(n,t?.equal),o=e,s=o[Pe];return o.set=i,o.update=r,o.asReadonly=lg.bind(o),o}function lg(){let n=this[Pe];if(n.readonlyFn===void 0){let t=()=>this();t[Pe]=n,n.readonlyFn=t}return n.readonlyFn}var ji=new w("",{factory:()=>zw}),zw="ng";var Va=new w(""),Bi=new w("",{providedIn:"platform",factory:()=>"unknown"}),xr=new w(""),Hi=new w("",{factory:()=>m(Z).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ao=(()=>{class n{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Gw}return n})();function Gw(){return new Ao(L(),Be())}var sn=class{},No=new w("",{factory:()=>!0});var hu=new w(""),ja=(()=>{class n{static \u0275prov=P({token:n,providedIn:"root",factory:()=>new Md})}return n})(),Md=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let e=t.zone,i=this.queues.get(e);i.has(t)&&(i.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let e=t.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(t)||i.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[e,i]of this.queues)e===null?t||=this.flushQueue(i):t||=e.run(()=>this.flushQueue(i));t||(this.dirtyEffectCount=0)}}flushQueue(t){let e=!1;for(let i of t)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},ha=class{[Pe];constructor(t){this[Pe]=t}destroy(){this[Pe].destroy()}};function Ui(n,t){let e=t?.injector??m(Ie),i=t?.manualCleanup!==!0?e.get(fn):null,r,o=e.get(Ao,null,{optional:!0}),s=e.get(sn);return o!==null?(r=Kw(o.view,s,n),i instanceof ma&&i._lView===o.view&&(i=null)):r=Qw(n,e.get(ja),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new ha(r)}var dg=ee(I({},Yl),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=vo(!1);try{Xl(this)}finally{vo(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=F(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],F(n)}}}),Ww=ee(I({},dg),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Wn(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),qw=ee(I({},dg),{consumerMarkedDirty(){this.view[R]|=8192,Li(this.view),this.notifier.notify(13)},destroy(){if(Wn(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Yn]?.delete(this)}});function Kw(n,t,e){let i=Object.create(qw);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=t,i.fn=ug(i,e),n[Yn]??=new Set,n[Yn].add(i),i.consumerMarkedDirty(i),i}function Qw(n,t,e){let i=Object.create(Ww);return i.fn=ug(i,n),i.scheduler=t,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function ug(n,t){return()=>{t(e=>(n.cleanupFns??=[]).push(e))}}function ko(n){return typeof n=="function"&&n[Pe]!==void 0}var Ba=(()=>{class n{internalPendingTasks=m(Vi);scheduler=m(sn);errorHandler=m(si);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=P({token:n,providedIn:"root",factory:()=>new n})}return n})();function $o(n){return{toString:n}.toString()}var Ka=class{previousValue;currentValue;firstChange;constructor(t,e,i){this.previousValue=t,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Gg(n,t,e,i){t!==null?t.applyValueToInputSignal(t,i):n[e]=i}var Ft=(()=>{let n=()=>Wg;return n.ngInherit=!0,n})();function Wg(n){return n.type.prototype.ngOnChanges&&(n.setInput=uD),dD}function dD(){let n=qg(this),t=n?.current;if(t){let e=n.previous;if(e===ei)n.previous=t;else for(let i in t)e[i]=t[i];n.current=null,this.ngOnChanges(t)}}function uD(n,t,e,i,r){let o=this.declaredInputs[i],s=qg(n)||fD(n,{previous:ei,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Ka(l&&l.currentValue,e,c===ei),Gg(n,t,r,e)}var xu="__ngSimpleChanges__";function qg(n){return Object.hasOwn(n,xu)&&n[xu]||null}function fD(n,t){return n[xu]=t}var fg=[];var se=function(n,t=null,e){for(let i=0;i<fg.length;i++){let r=fg[i];r(n,t,e)}},te=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(te||{});function mD(n,t,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=t.type.prototype;if(i){let s=Wg(t);(e.preOrderHooks??=[]).push(n,s),(e.preOrderCheckHooks??=[]).push(n,s)}r&&(e.preOrderHooks??=[]).push(0-n,r),o&&((e.preOrderHooks??=[]).push(n,o),(e.preOrderCheckHooks??=[]).push(n,o))}function Kg(n,t){for(let e=t.directiveStart,i=t.directiveEnd;e<i;e++){let o=n.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(n.contentHooks??=[]).push(-e,s),a&&((n.contentHooks??=[]).push(e,a),(n.contentCheckHooks??=[]).push(e,a)),c&&(n.viewHooks??=[]).push(-e,c),l&&((n.viewHooks??=[]).push(e,l),(n.viewCheckHooks??=[]).push(e,l)),d!=null&&(n.destroyHooks??=[]).push(e,d)}}function za(n,t,e){Qg(n,t,3,e)}function Ga(n,t,e,i){(n[R]&3)===e&&Qg(n,t,e,i)}function gu(n,t){let e=n[R];(e&3)===t&&(e&=16383,e+=1,n[R]=e)}function Qg(n,t,e,i){let r=i!==void 0?n[Ri]&65535:0,o=i??-1,s=t.length-1,a=0;for(let c=r;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],i!=null&&a>=i)break}else t[c]<0&&(n[Ri]+=65536),(a<o||o==-1)&&(pD(n,e,t,c),n[Ri]=(n[Ri]&4294901760)+c+2),c++}function mg(n,t){se(te.LifecycleHookStart,n,t);let e=F(null);try{t.call(n)}finally{F(e),se(te.LifecycleHookEnd,n,t)}}function pD(n,t,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=n[s];r?n[R]>>14<n[Ri]>>16&&(n[R]&3)===t&&(n[R]+=16384,mg(a,o)):mg(a,o)}var Mr=-1,$i=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,e,i,r){this.factory=t,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function hD(n){return(n.flags&8)!==0}function gD(n){return(n.flags&16)!==0}function vD(n,t,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];n.setAttribute(t,s,a,o)}else{let o=r,s=e[++i];yD(o)?n.setProperty(t,o,s):n.setAttribute(t,o,s),i++}}return i}function Zg(n){return n===3||n===4||n===6}function yD(n){return n.charCodeAt(0)===64}function Ar(n,t){if(!(t===null||t.length===0))if(n===null||n.length===0)n=t.slice();else{let e=-1;for(let i=0;i<t.length;i++){let r=t[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?pg(n,e,r,null,t[++i]):pg(n,e,r,null,null))}}return n}function pg(n,t,e,i,r){let o=0,s=n.length;if(t===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===e){r!==null&&(n[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(n.splice(s,0,t),o=s+1),n.splice(o++,0,e),r!==null&&n.splice(o++,0,r)}function Yg(n){return n!==Mr}function Qa(n){return n&32767}function bD(n){return n>>16}function Za(n,t){let e=bD(n),i=t;for(;e>0;)i=i[ki],e--;return i}var Su=!0;function Ya(n){let t=Su;return Su=n,t}var _D=256,Xg=_D-1,Jg=5,ED=0,mn={};function wD(n,t,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Ni)&&(i=e[Ni]),i==null&&(i=e[Ni]=ED++);let r=i&Xg,o=1<<r;t.data[n+(r>>Jg)]|=o}function Xa(n,t){let e=ev(n,t);if(e!==-1)return e;let i=t[A];i.firstCreatePass&&(n.injectorIndex=t.length,vu(i.data,n),vu(t,null),vu(i.blueprint,null));let r=nf(n,t),o=n.injectorIndex;if(Yg(r)){let s=Qa(r),a=Za(r,t),c=a[A].data;for(let l=0;l<8;l++)t[o+l]=a[s+l]|c[s+l]}return t[o+8]=r,o}function vu(n,t){n.push(0,0,0,0,0,0,0,0,t)}function ev(n,t){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||t[n.injectorIndex+8]===null?-1:n.injectorIndex}function nf(n,t){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let e=0,i=null,r=t;for(;r!==null;){if(i=ov(r),i===null)return Mr;if(e++,r=r[ki],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Mr}function Mu(n,t,e){wD(n,t,e)}function DD(n,t){if(t==="class")return n.classes;if(t==="style")return n.styles;let e=n.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Zg(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===t)return e[r+1];r=r+2}}}return null}function tv(n,t,e){if(e&8||n!==void 0)return n;Ea(t,"NodeInjector")}function nv(n,t,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=n[xn],o=st(void 0);try{return r?r.get(t,i,e&8):Ld(t,i,e&8)}finally{st(o)}}return tv(i,t,e)}function iv(n,t,e,i=0,r){if(n!==null){if(t[R]&2048&&!(i&2)){let s=SD(n,t,e,i,mn);if(s!==mn)return s}let o=rv(n,t,e,i,mn);if(o!==mn)return o}return nv(t,e,i,r)}function rv(n,t,e,i,r){let o=ID(e);if(typeof o=="function"){if(!lu(t,n,i))return i&1?tv(r,e,i):nv(t,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Ea(e);else return s}finally{du()}}else if(typeof o=="number"){let s=null,a=ev(n,t),c=Mr,l=i&1?t[ut][Xe]:null;for((a===-1||i&4)&&(c=a===-1?nf(n,t):t[a+8],c===Mr||!gg(i,!1)?a=-1:(s=t[A],a=Qa(c),t=Za(c,t)));a!==-1;){let d=t[A];if(hg(o,a,d.data)){let u=CD(a,t,e,s,i,l);if(u!==mn)return u}c=t[a+8],c!==Mr&&gg(i,t[A].data[a+8]===l)&&hg(o,a,t)?(s=d,a=Qa(c),t=Za(c,t)):a=-1}}return r}function CD(n,t,e,i,r,o){let s=t[A],a=s.data[n+8],c=i==null?Tn(a)&&Su:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Wa(a,s,e,c,l);return d!==null?Lo(t,s,d,a,r):mn}function Wa(n,t,e,i,r){let o=n.providerIndexes,s=t.data,a=o&1048575,c=n.directiveStart,l=n.directiveEnd,d=o>>20,u=i?a:a+d,h=r?a+d:l;for(let f=u;f<h;f++){let v=s[f];if(f<c&&e===v||f>=c&&v.type===e)return f}if(r){let f=s[c];if(f&&dn(f)&&f.type===e)return c}return null}function Lo(n,t,e,i,r){let o=n[e],s=t.data;if(o instanceof $i){let a=o;if(a.resolving)throw Pd("");let c=Ya(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,u=a.injectImpl?st(a.injectImpl):null,h=lu(n,i,0);try{o=n[e]=a.factory(void 0,r,s,n,i),t.firstCreatePass&&e>=i.directiveStart&&mD(e,s[e],t)}finally{u!==null&&st(u),Ya(c),a.resolving=!1,du()}}return o}function ID(n){if(typeof n=="string")return n.charCodeAt(0)||0;let t=n.hasOwnProperty(Ni)?n[Ni]:void 0;return typeof t=="number"?t>=0?t&Xg:xD:t}function hg(n,t,e){let i=1<<n;return!!(e[t+(n>>Jg)]&i)}function gg(n,t){return!(n&2)&&!(n&1&&t)}var ai=class{_tNode;_lView;constructor(t,e){this._tNode=t,this._lView=e}get(t,e,i){return iv(this._tNode,this._lView,t,Mi(i),e)}};function xD(){return new ai(Be(),L())}function qi(n){return $o(()=>{let t=n.prototype.constructor,e=t[go]||Tu(t),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let o=r[go]||Tu(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Tu(n){return Td(n)?()=>{let t=Tu(ze(n));return t&&t()}:Zn(n)}function SD(n,t,e,i,r){let o=n,s=t;for(;o!==null&&s!==null&&s[R]&2048&&!Dr(s);){let a=rv(o,s,e,i|2,mn);if(a!==mn)return a;let c=o.parent;if(!c){let l=s[zd];if(l){let d=l.get(e,mn,i&-5);if(d!==mn)return d}c=ov(s),s=s[ki]}o=c}return r}function ov(n){let t=n[A],e=t.type;return e===2?t.declTNode:e===1?n[Xe]:null}function rf(n){return DD(Be(),n)}function Y(n){return{token:n.token,providedIn:n.autoProvided===!1?null:"root",factory:n.factory,value:void 0}}function MD(){return Fr(Be(),L())}function Fr(n,t){return new re(kt(n,t))}var re=(()=>{class n{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=MD}return n})();function sv(n){return n instanceof re?n.nativeElement:n}function TD(){return this._results[Symbol.iterator]()}var Vo=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Q}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,e){return this._results.reduce(t,e)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,e){this.dirty=!1;let i=Sh(t);(this._changesDetected=!xh(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=TD};function av(n){return(n.flags&128)===128}var of=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(of||{}),cv=new Map,AD=0;function ND(){return AD++}function kD(n){cv.set(n[Sn],n)}function Au(n){cv.delete(n[Sn])}var vg="__ngContext__";function Nr(n,t){Mn(t)?(n[vg]=t[Sn],kD(t)):n[vg]=t}function lv(n){return uv(n[wr])}function dv(n){return uv(n[Tt])}function uv(n){for(;n!==null&&!At(n);)n=n[Tt];return n}var Nu;function sf(n){Nu=n}function fv(){if(Nu!==void 0)return Nu;if(typeof document<"u")return document;throw new _(210,!1)}var mv=!1,pv=new w("",{factory:()=>mv});var yg=new WeakMap;function RD(n,t){if(n==null||typeof n!="object")return;let e=yg.get(n);e||(e=new WeakSet,yg.set(n,e)),e.add(t)}var FD=(n,t,e,i)=>{};function OD(n,t,e,i){FD(n,t,e,i)}function dc(n){return(n.flags&32)===32}var PD=()=>null;function hv(n,t,e=!1){return PD(n,t,e)}function gv(n,t){let e=n.contentQueries;if(e!==null){let i=F(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=n.data[s];So(o),a.contentQueries(2,t[s],s)}}}finally{F(i)}}}function ku(n,t,e){So(0);let i=F(null);try{t(n,e)}finally{F(i)}}function vv(n,t,e){if(Gd(t)){let i=F(null);try{let r=t.directiveStart,o=t.directiveEnd;for(let s=r;s<o;s++){let a=n.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{F(i)}}}var Kt=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(Kt||{});var Ha;function LD(){if(Ha===void 0&&(Ha=null,an.trustedTypes))try{Ha=an.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch(n){}return Ha}function uc(n){return LD()?.createHTML(n)||n}var Nn=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${ga})`}},Ru=class extends Nn{getTypeName(){return"HTML"}},Fu=class extends Nn{getTypeName(){return"Style"}},Ou=class extends Nn{getTypeName(){return"Script"}},Pu=class extends Nn{getTypeName(){return"URL"}},Lu=class extends Nn{getTypeName(){return"ResourceURL"}};function Rn(n){return n instanceof Nn?n.changingThisBreaksApplicationSecurity:n}function Ki(n,t){let e=yv(n);if(e!=null&&e!==t){if(e==="ResourceURL"&&t==="URL")return!0;throw new Error(`Required a safe ${t}, got a ${e} (see ${ga})`)}return e===t}function yv(n){return n instanceof Nn&&n.getTypeName()||null}function af(n){return new Ru(n)}function cf(n){return new Fu(n)}function lf(n){return new Ou(n)}function df(n){return new Pu(n)}function uf(n){return new Lu(n)}function VD(n){let t=new ju(n);return jD()?new Vu(t):t}var Vu=class{inertDocumentHelper;constructor(t){this.inertDocumentHelper=t}getInertBodyElement(t){t="<body><remove></remove>"+t;try{let e=new window.DOMParser().parseFromString(uc(t),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(t):(e.firstChild?.remove(),e)}catch(e){return null}}},ju=class{defaultDoc;inertDocument;constructor(t){this.defaultDoc=t,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(t){let e=this.inertDocument.createElement("template");return e.innerHTML=uc(t),e}};function jD(){try{return!!new window.DOMParser().parseFromString(uc(""),"text/html")}catch(n){return!1}}var BD=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function fc(n){return n=String(n),n.match(BD)?n:"unsafe:"+n}function Fn(n){let t={};for(let e of n.split(","))t[e]=!0;return t}function zo(...n){let t={};for(let e of n)for(let i in e)e.hasOwnProperty(i)&&(t[i]=!0);return t}var bv=Fn("area,br,col,hr,img,wbr"),_v=Fn("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Ev=Fn("rp,rt"),HD=zo(Ev,_v),UD=zo(_v,Fn("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),$D=zo(Ev,Fn("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),bg=zo(bv,UD,$D,HD),wv=Fn("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),zD=Fn("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),GD=Fn("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),WD=zo(wv,zD,GD),qD=Fn("script,style,template"),Bu=class{sanitizedSomething=!1;buf=[];sanitizeChildren(t){let e=t.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=ZD(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=QD(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(t){let e=_g(t).toLowerCase();if(!bg.hasOwnProperty(e))return this.sanitizedSomething=!0,!qD.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=t.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!WD.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;wv[a]&&(c=fc(c)),this.buf.push(" ",s,'="',Eg(c),'"')}return this.buf.push(">"),!0}endElement(t){let e=_g(t).toLowerCase();bg.hasOwnProperty(e)&&!bv.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(t){this.buf.push(Eg(t))}};function KD(n,t){return(n.compareDocumentPosition(t)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function QD(n){let t=n.nextSibling;if(t&&n!==t.previousSibling)throw Dv(t);return t}function ZD(n){let t=n.firstChild;if(t&&KD(n,t))throw Dv(t);return t}function _g(n){let t=n.nodeName;return typeof t=="string"?t:"FORM"}function Dv(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var YD=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,XD=/([^\#-~ |!])/g;function Eg(n){return n.replace(/&/g,"&amp;").replace(YD,function(t){let e=t.charCodeAt(0),i=t.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(XD,function(t){return"&#"+t.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Ua;function ff(n,t){let e=null;try{Ua=Ua||VD(n);let i=t?String(t):"";e=Ua.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Ua.getInertBodyElement(i)}while(i!==o);let a=new Bu().sanitizeChildren(wg(e)||e);return uc(a)}finally{if(e){let i=wg(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function wg(n){return"content"in n&&JD(n)?n.content:null}function JD(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}function eC(n,t){return n.createText(t)}function tC(n,t,e){n.setValue(t,e)}function Cv(n,t,e){return n.createElement(t,e)}function Ja(n,t,e,i,r){n.insertBefore(t,e,i,r)}function Iv(n,t,e){n.appendChild(t,e)}function Dg(n,t,e,i,r){i!==null?Ja(n,t,e,i,r):Iv(n,t,e)}function nC(n,t,e,i){n.removeChild(null,t,e,i)}function iC(n,t,e){n.setAttribute(t,"style",e)}function rC(n,t,e){e===""?n.removeAttribute(t,"class"):n.setAttribute(t,"class",e)}function xv(n,t,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&vD(n,t,i),r!==null&&rC(n,t,r),o!==null&&iC(n,t,o)}var ct=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n[n.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",n})(ct||{});function oC(n,t,e){let i=n.length;for(;;){let r=n.indexOf(t,e);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let o=t.length;if(r+o===i||n.charCodeAt(r+o)<=32)return r}e=r+1}}var Sv="ng-template";function sC(n,t,e,i){let r=0;if(i){for(;r<t.length&&typeof t[r]=="string";r+=2)if(t[r]==="class"&&oC(t[r+1].toLowerCase(),e,0)!==-1)return!0}else if(mf(n))return!1;if(r=t.indexOf(1,r),r>-1){let o;for(;++r<t.length&&typeof(o=t[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function mf(n){return n.type===4&&n.value!==Sv}function aC(n,t,e){let i=n.type===4&&!e?Sv:n.value;return t===i}function cC(n,t,e){let i=4,r=n.attrs,o=r!==null?uC(r):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!Wt(i)&&!Wt(c))return!1;if(s&&Wt(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!aC(n,c,e)||c===""&&t.length===1){if(Wt(i))return!1;s=!0}}else if(i&8){if(r===null||!sC(n,r,c,e)){if(Wt(i))return!1;s=!0}}else{let l=t[++a],d=lC(c,r,mf(n),e);if(d===-1){if(Wt(i))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&l!==u){if(Wt(i))return!1;s=!0}}}}return Wt(i)||s}function Wt(n){return(n&1)===0}function lC(n,t,e,i){if(t===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<t.length;){let s=t[r];if(s===n)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=t[++r];for(;typeof a=="string";)a=t[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return fC(t,n)}function Mv(n,t,e=!1){for(let i=0;i<t.length;i++)if(cC(n,t[i],e))return!0;return!1}function dC(n){let t=n.attrs;if(t!=null){let e=t.indexOf(5);if((e&1)===0)return t[e+1]}return null}function uC(n){for(let t=0;t<n.length;t++){let e=n[t];if(Zg(e))return t}return n.length}function fC(n,t){let e=n.indexOf(4);if(e>-1)for(e++;e<n.length;){let i=n[e];if(typeof i=="number")return-1;if(i===t)return e;e++}return-1}function mC(n,t){e:for(let e=0;e<t.length;e++){let i=t[e];if(n.length===i.length){for(let r=0;r<n.length;r++)if(n[r]!==i[r])continue e;return!0}}return!1}function Cg(n,t){return n?":not("+t.trim()+")":t}function pC(n){let t=n[0],e=1,i=2,r="",o=!1;for(;e<n.length;){let s=n[e];if(typeof s=="string")if(i&2){let a=n[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!Wt(s)&&(t+=Cg(o,r),r=""),i=s,o=o||!Wt(i);e++}return r!==""&&(t+=Cg(o,r)),t}function hC(n){return n.map(pC).join(",")}function gC(n){let t=[],e=[],i=1,r=2;for(;i<n.length;){let o=n[i];if(typeof o=="string")r===2?o!==""&&t.push(o,n[++i]):r===8&&e.push(o);else{if(!Wt(r))break;r=o}i++}return e.length&&t.push(1,...e),t}var ft={},pn=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(pn||{}),vC;function pf(n,t){return vC(n,t)}var k2=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Hu=new WeakMap,Fo=new WeakSet;function yC(n,t){let e=Hu.get(n);if(!e||e.length===0)return;let i=t.parentNode,r=t.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===t?(e.splice(o,1),Fo.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function bC(n,t){let e=Hu.get(n);e?e.includes(t)||e.push(t):Hu.set(n,[t])}var zi=new Set,mc=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(mc||{}),On=new w(""),Ig=new Set;function di(n){Ig.has(n)||(Ig.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var hf=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=P({token:n,providedIn:"root",factory:()=>new n})}return n})(),gf=[0,1,2,3],Tv=(()=>{class n{ngZone=m(O);scheduler=m(sn);errorHandler=m(dt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(On,{optional:!0})}execute(){let e=this.sequences.size>0;e&&se(te.AfterRenderHooksStart),this.executing=!0;for(let i of gf)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&se(te.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Fi]??=[]).push(e),Li(i),i[R]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(mc.AFTER_NEXT_RENDER,e):e()}static \u0275prov=P({token:n,providedIn:"root",factory:()=>new n})}return n})(),ec=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,e,i,r,o,s=null){this.impl=t,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[Fi];t&&(this.view[Fi]=t.filter(e=>e!==this))}};var Av=new w("",{factory:()=>{let n=m(Ge),t=new Set;return n.onDestroy(()=>t.clear()),{queue:t,isScheduled:!1,scheduler:null,injector:n}}});function Nv(n,t,e){let i=n.get(Av);if(Array.isArray(t))for(let r of t)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(t),e?.detachedLeaveAnimationFns?.push(t);i.scheduler&&i.scheduler(n)}function _C(n,t){let e=n.get(Av);if(Array.isArray(t))for(let i of t)e.queue.delete(i);else e.queue.delete(t)}function EC(n,t){for(let[e,i]of t)Nv(n,i.animateFns)}function xg(n,t,e,i){let r=n?.[ii]?.enter;t!==null&&r&&r.has(e.index)&&EC(i,r)}function Sg(n,t,e,i){try{e.get(wo)}catch(s){return i(!1)}let r=n?.[ii];r?.enter?.has(t.index)&&_C(e,r.enter.get(t.index).animateFns);let o=wC(n,t,r);if(o.size===0){let s=!1;if(n){let a=[];pc(n,t,a),s=a.length>0}if(!s)return i(!1)}n&&zi.add(n[Sn]),Nv(e,()=>DC(n,t,r||void 0,o,i),r||void 0)}function wC(n,t,e){let i=new Map,r=e?.leave;if(r&&r.has(t.index)&&i.set(t.index,r.get(t.index)),n&&r)for(let[o,s]of r){if(i.has(o))continue;let c=n[A].data[o].parent;for(;c;){if(c===t){i.set(o,s);break}c=c.parent}}return i}function DC(n,t,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(n&&pc(n,t,o),o.length>0){let s=e||n?.[ii];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),IC(n,s.running,r)}else Promise.allSettled(o).then(()=>{n&&zi.delete(n[Sn]),r(!0)})}else n&&zi.delete(n[Sn]),r(!1)}function pc(n,t,e){if(t.type&12){let r=n[t.index];if(At(r))for(let o=qe;o<r.length;o++){let s=r[o];s[A].type===2&&CC(s,e)}}let i=t.child;for(;i;)pc(n,i,e),i=i.next}function CC(n,t){let e=n[ii];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();t.push(s)}let i=n[A].firstChild;for(;i;)pc(n,i,t),i=i.next}function IC(n,t,e){t.then(()=>{n[ii]?.running===t&&(n[ii].running=void 0,zi.delete(n[Sn])),e(!0)})}function Sr(n,t,e,i,r,o,s,a){if(r!=null){let c,l=!1;At(r)?c=r:Mn(r)&&(l=!0,r=r[Gt]);let d=Nt(r);n===0&&i!==null?(xg(a,i,o,e),s==null?Iv(t,i,d):Ja(t,i,d,s||null,!0)):n===1&&i!==null?(xg(a,i,o,e),Ja(t,i,d,s||null,!0),yC(o,d)):n===2?(a?.[ii]?.leave?.has(o.index)&&bC(o,d),Fo.delete(d),Sg(a,o,e,u=>{if(Fo.has(d)){Fo.delete(d);return}nC(t,d,l,u)})):n===3&&(Fo.delete(d),Sg(a,o,e,()=>{t.destroyNode(d)})),c!=null&&OC(t,n,e,c,o,i,s)}}function xC(n,t){kv(n,t),t[Gt]=null,t[Xe]=null}function SC(n,t,e,i,r,o){i[Gt]=r,i[Xe]=t,hc(n,i,e,1,r,o)}function kv(n,t){t[cn].changeDetectionScheduler?.notify(9),hc(n,t,t[_e],2,null,null)}function MC(n){let t=n[wr];if(!t)return yu(n[A],n);for(;t;){let e=null;if(Mn(t))e=t[wr];else{let i=t[qe];i&&(e=i)}if(!e){for(;t&&!t[Tt]&&t!==n;)Mn(t)&&yu(t[A],t),t=t[je];t===null&&(t=n),Mn(t)&&yu(t[A],t),e=t&&t[Tt]}t=e}}function vf(n,t){let e=n[Oi],i=e.indexOf(t);e.splice(i,1)}function yf(n,t){if(Pi(t))return;let e=t[_e];e.destroyNode&&hc(n,t,e,3,null,null),MC(t)}function yu(n,t){if(Pi(t))return;let e=F(null);try{t[R]&=-129,t[R]|=256,t[Et]&&Wn(t[Et]),AC(n,t),TC(n,t),t[A].type===1&&t[_e].destroy();let i=t[ni];if(i!==null&&At(t[je])){i!==t[je]&&vf(i,t);let r=t[ln];r!==null&&r.detachView(n)}Au(t)}finally{F(e)}}function TC(n,t){let e=n.cleanup,i=t[Er];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(t[Er]=null);let r=t[Cn];if(r!==null){t[Cn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=t[Yn];if(o!==null){t[Yn]=null;for(let s of o)s.destroy()}}function AC(n,t){let e;if(n!=null&&(e=n.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=t[e[i]];if(!(r instanceof $i)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];se(te.LifecycleHookStart,a,c);try{c.call(a)}finally{se(te.LifecycleHookEnd,a,c)}}else{se(te.LifecycleHookStart,r,o);try{o.call(r)}finally{se(te.LifecycleHookEnd,r,o)}}}}}function Rv(n,t,e){return NC(n,t.parent,e)}function NC(n,t,e){let i=t;for(;i!==null&&i.type&168;)t=i,i=t.parent;if(i===null)return e[Gt];if(Tn(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===Kt.None||r===Kt.Emulated)return null}return kt(i,e)}function Fv(n,t,e){return RC(n,t,e)}function kC(n,t,e){return n.type&40?kt(n,e):null}var RC=kC,Mg;function bf(n,t,e,i){let r=Rv(n,i,t),o=t[_e],s=i.parent||t[Xe],a=Fv(s,i,t);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)Dg(o,r,e[c],a,!1);else Dg(o,r,e,a,!1);Mg!==void 0&&Mg(o,i,t,e,r)}function Oo(n,t){if(t!==null){let e=t.type;if(e&3)return kt(t,n);if(e&4)return Uu(-1,n[t.index]);if(e&8){let i=t.child;if(i!==null)return Oo(n,i);{let r=n[t.index];return At(r)?Uu(-1,r):Nt(r)}}else{if(e&128)return Oo(n,t.next);if(e&32)return pf(t,n)()||Nt(n[t.index]);{let i=Ov(n,t);if(i!==null){if(Array.isArray(i))return i[0];let r=In(n[ut]);return Oo(r,i)}else return Oo(n,t.next)}}}return null}function Ov(n,t){if(t!==null){let i=n[ut][Xe],r=t.projection;return i.projection[r]}return null}function Uu(n,t){let e=qe+n+1;if(e<t.length){let i=t[e],r=i[A].firstChild;if(r!==null)return Oo(i,r)}return t[ri]}function _f(n,t,e,i,r,o,s){for(;e!=null;){let a=i[xn];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&t===0&&(c&&Nr(Nt(c),i),e.flags|=2),!dc(e))if(l&8)_f(n,t,e.child,i,r,o,!1),Sr(t,n,a,r,c,e,o,i);else if(l&32){let d=pf(e,i),u;for(;u=d();)Sr(t,n,a,r,u,e,o,i);Sr(t,n,a,r,c,e,o,i)}else l&16?Pv(n,t,i,e,r,o):Sr(t,n,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function hc(n,t,e,i,r,o){_f(e,i,n.firstChild,t,r,o,!1)}function FC(n,t,e){let i=t[_e],r=Rv(n,e,t),o=e.parent||t[Xe],s=Fv(o,e,t);Pv(i,0,t,e,r,s)}function Pv(n,t,e,i,r,o){let s=e[ut],c=s[Xe].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Sr(t,n,e[xn],r,d,i,o,e)}else{let l=c,d=s[je];av(i)&&(l.flags|=128),_f(n,t,l,d,r,o,!0)}}function OC(n,t,e,i,r,o,s){let a=i[ri],c=Nt(i);a!==c&&Sr(t,n,e,o,a,r,s);for(let l=qe;l<i.length;l++){let d=i[l];hc(d[A],d,n,t,o,a)}}function PC(n,t,e,i,r){if(t)r?n.addClass(e,i):n.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:pn.DashCase;r==null?n.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=pn.Important),n.setStyle(e,i,r,o))}}function Ef(n,t,e,i,r,o,s,a,c,l,d){let u=Me+i,h=u+r,f=LC(u,h),v=typeof l=="function"?l():l;return f[A]={type:n,blueprint:f,template:e,queries:null,viewQuery:a,declTNode:t,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:v,incompleteFirstPass:!1,ssrId:d}}function LC(n,t){let e=[];for(let i=0;i<t;i++)e.push(i<n?null:ft);return e}function VC(n){let t=n.tView;return t===null||t.incompleteFirstPass?n.tView=Ef(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):t}function wf(n,t,e,i,r,o,s,a,c,l,d){let u=t.blueprint.slice();return u[Gt]=r,u[R]=i|4|128|8|64|1024,(l!==null||n&&n[R]&2048)&&(u[R]|=2048),Zd(u),u[je]=u[ki]=n,u[We]=e,u[cn]=s||n&&n[cn],u[_e]=a||n&&n[_e],u[xn]=c||n&&n[xn]||null,u[Xe]=o,u[Sn]=ND(),u[_r]=d,u[zd]=l,u[ut]=t.type==2?n[ut]:u,u}function jC(n,t,e){let i=kt(t,n),r=VC(e),o=n[cn].rendererFactory,s=Df(n,wf(n,r,null,Lv(e),i,t,null,o.createRenderer(i,e),null,null,null));return n[t.index]=s}function Lv(n){let t=16;return n.signals?t=4096:n.onPush&&(t=64),t}function Vv(n,t,e,i){if(e===0)return-1;let r=t.length;for(let o=0;o<e;o++)t.push(i),n.blueprint.push(i),n.data.push(null);return r}function Df(n,t){return n[wr]?n[$d][Tt]=t:n[wr]=t,n[$d]=t,t}function b(n=1){jv(Te(),L(),An()+n,!1)}function jv(n,t,e,i){if(!i)if((t[R]&3)===3){let o=n.preOrderCheckHooks;o!==null&&za(t,o,e)}else{let o=n.preOrderHooks;o!==null&&Ga(t,o,0,e)}oi(e)}var gc=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(gc||{});function Gi(n,t,e,i){let r=F(null);try{let[o,s,a]=n.inputs[e],c=null;(s&gc.SignalBased)!==0&&(c=t[o][Pe]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(t,i)),n.setInput!==null?n.setInput(t,c,i,e,o):Gg(t,c,o,i)}finally{F(r)}}function Bv(n,t,e,i,r){let o=An(),s=i&2;try{oi(-1),s&&t.length>Me&&jv(n,t,Me,!1);let a=s?te.TemplateUpdateStart:te.TemplateCreateStart;se(a,r,e),e(i,r)}finally{oi(o);let a=s?te.TemplateUpdateEnd:te.TemplateCreateEnd;se(a,r,e)}}function Cf(n,t,e){WC(n,t,e),(e.flags&64)===64&&qC(n,t,e)}function vc(n,t,e=kt){let i=t.localNames;if(i!==null){let r=t.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(t,n):n[s];n[r++]=a}}}function BC(n,t,e,i){let o=i.get(pv,mv)||e===Kt.ShadowDom||e===Kt.ExperimentalIsolatedShadowDom,s=n.selectRootElement(t,o);if(s.tagName.toLowerCase()==="script")throw new _(905,!1);return HC(s),s}function HC(n){UC(n)}var UC=()=>null;function $C(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function zC(n,t,e,i,r,o){let s=t[A];if(If(n,s,t,e,i)){Tn(n)&&GC(t,n.index);return}n.type&3&&(e=$C(e)),Hv(n,t,e,i,r,o)}function Hv(n,t,e,i,r,o){if(n.type&3){let s=kt(n,t);i=o!=null?o(i,n.value||"",e):i,r.setProperty(s,e,i)}else n.type&12}function GC(n,t){let e=Rt(t,n);e[R]&16||(e[R]|=64)}function WC(n,t,e){let i=e.directiveStart,r=e.directiveEnd;Tn(e)&&jC(t,e,n.data[i+e.componentOffset]),n.firstCreatePass||Xa(e,t);let o=e.initialInputs;for(let s=i;s<r;s++){let a=n.data[s],c=Lo(t,n,s,e);if(Nr(c,t),o!==null&&YC(t,s-i,c,a,e,o),dn(a)){let l=Rt(e.index,t);l[We]=Lo(t,n,s,e)}}}function qC(n,t,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Zh();try{oi(o);for(let a=i;a<r;a++){let c=n.data[a],l=t[a];ka(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&KC(c,l)}}finally{oi(-1),ka(s)}}function KC(n,t){n.hostBindings!==null&&n.hostBindings(1,t)}function Uv(n,t){let e=n.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Mv(t,o.selectors,!1)&&(i??=[],dn(o)?i.unshift(o):i.push(o))}return i}function QC(n,t,e,i,r,o){let s=kt(n,t);ZC(t[_e],s,o,n.value,e,i,r)}function ZC(n,t,e,i,r,o,s){if(o==null)s?.(o,i||"",r),n.removeAttribute(t,r,e);else{let a=s==null?_o(o):s(o,i||"",r);n.setAttribute(t,r,a,e)}}function YC(n,t,e,i,r,o){let s=o[t];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Gi(i,e,c,l)}}function $v(n,t,e,i,r){let o=Me+e,s=t[A],a=r(s,t,n,i,e);t[o]=a,Cr(n,!0);let c=n.type===2;return c?(xv(t[_e],a,n),(Hh()===0||Io(n))&&Nr(a,t),Uh()):Nr(a,t),Pa()&&(!c||!dc(n))&&bf(s,t,a,n),n}function zv(n){let t=n;return su()?au():(t=t.parent,Cr(t,!1)),t}function XC(n,t){let e=n[xn];if(!e)return;let i;try{i=e.get(si,null)}catch(r){i=null}i?.(t)}function If(n,t,e,i,r){let o=n.inputs?.[i],s=n.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=t.data[l];Gi(u,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=t.data[c];Gi(d,l,i,r),a=!0}return a}function JC(n,t,e,i,r,o){let s=null,a=null,c=null,l=!1,d=n.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&n.hostDirectiveInputs?.hasOwnProperty(r)){let u=n.hostDirectiveInputs[r];for(let h=0;h<u.length;h+=2){let f=u[h];if(f>=a&&f<=c){let v=t.data[f],E=u[h+1];Gi(v,e[f],E,o),l=!0}else if(f>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(Gi(i,e[s],r,o),l=!0),l}function eI(n,t){let e=Rt(t,n),i=e[A];tI(i,e);let r=e[Gt];r!==null&&e[_r]===null&&(e[_r]=hv(r,e[xn])),se(te.ComponentStart);try{xf(i,e,e[We])}finally{se(te.ComponentEnd,e[We])}}function tI(n,t){for(let e=t.length;e<n.blueprint.length;e++)t.push(n.blueprint[e])}function xf(n,t,e){Fa(t);try{let i=n.viewQuery;i!==null&&ku(1,i,e);let r=n.template;r!==null&&Bv(n,t,r,1,e),n.firstCreatePass&&(n.firstCreatePass=!1),t[ln]?.finishViewCreation(n),n.staticContentQueries&&gv(n,t),n.staticViewQueries&&ku(2,n.viewQuery,e);let o=n.components;o!==null&&nI(t,o)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{t[R]&=-5,Oa()}}function nI(n,t){for(let e=0;e<t.length;e++)eI(n,t[e])}function Sf(n,t,e,i){let r=F(null);try{let o=t.tView,a=n[R]&4096?4096:16,c=wf(n,o,e,a,null,t,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[t.index];c[ni]=l;let d=n[ln];return d!==null&&(c[ln]=d.createEmbeddedView(o)),xf(o,c,e),c}finally{F(r)}}function tc(n,t){return!t||t.firstChild===null||av(n)}function jo(n,t,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=t[e.index];o!==null&&i.push(Nt(o)),At(o)&&Gv(o,i);let s=e.type;if(s&8)jo(n,t,e.child,i);else if(s&32){let a=pf(e,t),c;for(;c=a();)i.push(c)}else if(s&16){let a=Ov(t,e);if(Array.isArray(a))i.push(...a);else{let c=In(t[ut]);jo(c[A],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Gv(n,t){for(let e=qe;e<n.length;e++){let i=n[e],r=i[A].firstChild;r!==null&&jo(i[A],i,r,t)}n[ri]!==n[Gt]&&t.push(n[ri])}function Wv(n){if(n[Fi]!==null){for(let t of n[Fi])t.impl.addSequence(t);n[Fi].length=0}}var qv=[];function iI(n){return n[Et]??rI(n)}function rI(n){let t=qv.pop()??Object.create(sI);return t.lView=n,t}function oI(n){n.lView[Et]!==n&&(n.lView=null,qv.push(n))}var sI=ee(I({},gi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Li(n.lView)},consumerOnSignalRead(){this.lView[Et]=this}});function aI(n){let t=n[Et]??Object.create(cI);return t.lView=n,t}var cI=ee(I({},gi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let t=In(n.lView);for(;t&&!Kv(t[A]);)t=In(t);t&&Yd(t)},consumerOnSignalRead(){this.lView[Et]=this}});function Kv(n){return n.type!==2}function Qv(n){if(n[Yn]===null)return;let t=!0;for(;t;){let e=!1;for(let i of n[Yn])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));t=e&&!!(n[R]&8192)}}var lI=100;function Zv(n,t=0){let i=n[cn].rendererFactory,r=!1;r||i.begin?.();try{dI(n,t)}finally{r||i.end?.()}}function dI(n,t){let e=cu();try{vo(!0),$u(n,t);let i=0;for(;xo(n);){if(i===lI)throw new _(103,!1);i++,$u(n,1)}}finally{vo(e)}}function uI(n,t,e,i){if(Pi(t))return;let r=t[R],o=!1,s=!1;Fa(t);let a=!0,c=null,l=null;o||(Kv(n)?(l=iI(t),c=Gn(l)):js()===null?(a=!1,l=aI(t),c=Gn(l)):t[Et]&&(Wn(t[Et]),t[Et]=null));try{Zd(t),qh(n.bindingStartIndex),e!==null&&Bv(n,t,e,2,i);let d=(r&3)===3;if(!o)if(d){let f=n.preOrderCheckHooks;f!==null&&za(t,f,null)}else{let f=n.preOrderHooks;f!==null&&Ga(t,f,0,null),gu(t,0)}if(s||fI(t),Qv(t),Yv(t,0),n.contentQueries!==null&&gv(n,t),!o)if(d){let f=n.contentCheckHooks;f!==null&&za(t,f)}else{let f=n.contentHooks;f!==null&&Ga(t,f,1),gu(t,1)}pI(n,t);let u=n.components;u!==null&&Jv(t,u,0);let h=n.viewQuery;if(h!==null&&ku(2,h,i),!o)if(d){let f=n.viewCheckHooks;f!==null&&za(t,f)}else{let f=n.viewHooks;f!==null&&Ga(t,f,2),gu(t,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),t[xa]){for(let f of t[xa])f();t[xa]=null}o||(Wv(t),t[R]&=-73)}catch(d){throw o||Li(t),d}finally{l!==null&&(yi(l,c),a&&oI(l)),Oa()}}function Yv(n,t){for(let e=lv(n);e!==null;e=dv(e))for(let i=qe;i<e.length;i++){let r=e[i];Xv(r,t)}}function fI(n){for(let t=lv(n);t!==null;t=dv(t)){if(!(t[R]&2))continue;let e=t[Oi];for(let i=0;i<e.length;i++){let r=e[i];Yd(r)}}}function mI(n,t,e){se(te.ComponentStart);let i=Rt(t,n);try{Xv(i,e)}finally{se(te.ComponentEnd,i[We])}}function Xv(n,t){Ma(n)&&$u(n,t)}function $u(n,t){let i=n[A],r=n[R],o=n[Et],s=!!(t===0&&r&16);if(s||=!!(r&64&&t===0),s||=!!(r&1024),s||=!!(o?.dirty&&sr(o)),s||=!1,o&&(o.dirty=!1),n[R]&=-9217,s)uI(i,n,i.template,n[We]);else if(r&8192){let a=F(null);try{Qv(n),Yv(n,1);let c=i.components;c!==null&&Jv(n,c,1),Wv(n)}finally{F(a)}}}function Jv(n,t,e){for(let i=0;i<t.length;i++)mI(n,t[i],e)}function pI(n,t){let e=n.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)oi(~r);else{let o=r,s=e[++i],a=e[++i];Qh(s,o);let c=t[o];se(te.HostBindingsUpdateStart,c);try{a(2,c)}finally{se(te.HostBindingsUpdateEnd,c)}}}}finally{oi(-1)}}function Mf(n,t){let e=cu()?64:1088;for(n[cn].changeDetectionScheduler?.notify(t);n;){n[R]|=e;let i=In(n);if(Dr(n)&&!i)return n;n=i}return null}function ey(n,t,e,i){return[n,!0,0,t,null,i,null,e,null,null]}function hI(n,t){let e=qe+t;if(e<n.length)return n[e]}function Tf(n,t,e,i=!0){let r=t[A];if(vI(r,t,n,e),i){let s=Uu(e,n),a=t[_e],c=a.parentNode(n[ri]);c!==null&&SC(r,n[Xe],a,t,c,s)}let o=t[_r];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function gI(n,t){let e=nc(n,t);return e!==void 0&&yf(e[A],e),e}function nc(n,t){if(n.length<=qe)return;let e=qe+t,i=n[e];if(i){let r=i[ni];r!==null&&r!==n&&vf(r,i),t>0&&(n[e-1][Tt]=i[Tt]);let o=Eo(n,qe+t);xC(i[A],i);let s=o[ln];s!==null&&s.detachView(o[A]),i[je]=null,i[Tt]=null,i[R]&=-129}return i}function vI(n,t,e,i){let r=qe+i,o=e.length;i>0&&(e[r-1][Tt]=t),i<o-qe?(t[Tt]=e[r],Vd(e,qe+i,t)):(e.push(t),t[Tt]=null),t[je]=e;let s=t[ni];s!==null&&e!==s&&ty(s,t);let a=t[ln];a!==null&&a.insertView(n),Ta(t),t[R]|=128}function ty(n,t){let e=n[Oi],i=t[je];if(Mn(i))n[R]|=2;else{let r=i[je][ut];t[ut]!==r&&(n[R]|=2)}e===null?n[Oi]=[t]:e.push(t)}var ci=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,e=t[A];return jo(e,t,e.firstChild,[])}constructor(t,e){this._lView=t,this._cdRefInjectingView=e}get context(){return this._lView[We]}set context(t){this._lView[We]=t}get destroyed(){return Pi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[je];if(At(t)){let e=t[Co],i=e?e.indexOf(this):-1;i>-1&&(nc(t,i),Eo(e,i))}this._attachedToViewContainer=!1}yf(this._lView[A],this._lView)}onDestroy(t){Aa(this._lView,t)}markForCheck(){Mf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[R]&=-129}reattach(){Ta(this._lView),this._lView[R]|=128}detectChanges(){this._lView[R]|=1024,Zv(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new _(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=Dr(this._lView),e=this._lView[ni];e!==null&&!t&&vf(e,this._lView),kv(this._lView[A],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new _(902,!1);this._appRef=t;let e=Dr(this._lView),i=this._lView[ni];i!==null&&!e&&ty(i,this._lView),Ta(this._lView)}};var li=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=yI;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Sf(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new ci(o)}}return n})();function yI(){return yc(Be(),L())}function yc(n,t){return n.type&4?new li(t,n,Fr(n,t)):null}function Or(n,t,e,i,r){let o=n.data[t];if(o===null)o=bI(n,t,e,i,r),Kh()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=$h();o.injectorIndex=s===null?-1:s.injectorIndex}return Cr(o,!0),o}function bI(n,t,e,i,r){let o=ou(),s=su(),a=s?o:o&&o.parent,c=n.data[t]=EI(n,a,e,t,i,r);return _I(n,c,o,s),c}function _I(n,t,e,i){n.firstChild===null&&(n.firstChild=t),e!==null&&(i?e.child==null&&t.parent!==null&&(e.child=t):e.next===null&&(e.next=t,t.prev=e))}function EI(n,t,e,i,r,o){let s=t?t.injectorIndex:-1,a=0;return nu()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:uu(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var wI=()=>null,DI=()=>null;function zu(n,t){return wI(n,t)}function CI(n,t,e){return DI(n,t,e)}var ny=class{},at=class{},mt=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>II()}return n})();function II(){let n=L(),t=Be(),e=Rt(t.index,n);return(Mn(e)?e:n)[_e]}var iy=(()=>{class n{static \u0275prov=P({token:n,providedIn:"root",factory:()=>null})}return n})();function ry(n){return n.debugInfo?.className||n.type.name||null}var qa={},ic=class{injector;parentInjector;constructor(t,e){this.injector=t,this.parentInjector=e}get(t,e,i){let r=this.injector.get(t,qa,i);return r!==qa||e===qa?r:this.parentInjector.get(t,e,i)}};function Af(n){return sy(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function oy(n,t){if(Array.isArray(n))for(let e=0;e<n.length;e++)t(n[e]);else{let e=n[Symbol.iterator](),i;for(;!(i=e.next()).done;)t(i.value)}}function sy(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function xI(n,t,e){return n[t]=e}function kn(n,t,e){if(e===ft)return!1;let i=n[t];return Object.is(i,e)?!1:(n[t]=e,!0)}function ay(n,t,e,i){let r=kn(n,t,e);return kn(n,t+1,i)||r}function Tr(n,t,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&RD(r,o);let s=Tn(n)?Rt(n.index,t):t;Mf(s,5);let a=t[We],c=Tg(t,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=Tg(t,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Tg(n,t,e,i){let r=F(null);try{return se(te.OutputStart,t,e),e(i)!==!1}catch(o){return XC(n,o),!1}finally{se(te.OutputEnd,t,e),F(r)}}function cy(n,t,e,i,r,o,s,a){let c=Io(n),l=!1,d=null;if(!i&&c&&(d=MI(t,e,o,n.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=kt(n,e),h=i?i(u):u;OD(e,h,o,a),i||(a.__ngNativeEl__=u);let f=r.listen(h,o,a);if(!SI(o)){let v=i?E=>i(Nt(E[n.index])):n.index;ly(v,t,e,o,a,f,!1)}}return l}function SI(n){return n.startsWith("animation")||n.startsWith("transition")}function MI(n,t,e,i){let r=n.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=t[Er],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function ly(n,t,e,i,r,o,s){let a=t.firstCreatePass?Jd(t):null,c=Xd(e),l=c.length;c.push(r,o),a&&a.push(i,n,l,(l+1)*(s?-1:1))}function Ag(n,t,e,i,r){let o=null,s=null,a=null,c=!1,l=n.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&n.hostDirectiveOutputs?.hasOwnProperty(i)){let d=n.hostDirectiveOutputs[i];for(let u=0;u<d.length;u+=2){let h=d[u];if(h>=s&&h<=a)c=!0,rc(n,t,h,d[u+1],i,r);else if(h>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,rc(n,t,o,i,i,r)),c}function rc(n,t,e,i,r,o){let s=t[e],a=t[A],l=a.data[e].outputs[i],u=s[l].subscribe(o);ly(n.index,a,t,r,o,u,!0)}function Nf(){TI()}function TI(){let n=L(),t=Te(),e=Be();if(t.firstCreatePass&&NI(t,e),e.controlDirectiveIndex===-1)return;di("NgSignalForms");let i=n[e.controlDirectiveIndex];t.data[e.controlDirectiveIndex].controlDef.create(i,new oc(n,t,e))}function kf(){AI()}function AI(){let n=L(),t=Te(),e=Mo();if(e.controlDirectiveIndex===-1)return;let i=t.data[e.controlDirectiveIndex].controlDef,r=n[e.controlDirectiveIndex];i.update(r,new oc(n,t,e))}var oc=class{lView;tView;tNode;hasPassThrough;constructor(t,e,i){this.lView=t,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return kt(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(t,e){let i=this.tView.data[this.tNode.customControlIndex];Ag(this.tNode,this.lView,i,t,Tr(this.tNode,this.lView,e))}listenToCustomControlModel(t){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];Ag(this.tNode,this.lView,i,e,Tr(this.tNode,this.lView,t))}listenToDom(t,e){cy(this.tNode,this.tView,this.lView,void 0,this.lView[_e],t,e,Tr(this.tNode,this.lView,e))}setInputOnDirectives(t,e){let i=this.tNode.inputs?.[t],r=this.tNode.hostDirectiveInputs?.[t];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];Gi(a,c,t,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];Gi(l,d,c,e),o=!0}return o}setCustomControlModelInput(t){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";JC(this.tNode,this.tView,this.lView,e,i,t)}customControlHasInput(t){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[t]===!0}_buildCustomControlInputCache(t){let e={};for(let i in t.inputs)e[i]=!0;if(t.hostDirectives!==null){let i=[...t.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=Ng(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=Ng(o.directive);s!==null&&i.push(...s)}}}return e}};function Ng(n){return typeof n=="function"&&"\u0275dir"in n?n.\u0275dir.hostDirectives??null:null}function NI(n,t,e){for(let r=t.directiveStart;r<t.directiveEnd;r++)if(n.data[r].controlDef){t.controlDirectiveIndex=r;break}if(t.controlDirectiveIndex===-1)return;let i=n.data[t.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(t.inputs?.[i.passThroughInput]?.length??0)>1){t.flags|=4096;return}kI(n,t)}function kI(n,t){for(let e=t.directiveStart;e<t.directiveEnd;e++){let i=n.data[e];if(!(t.directiveToIndex&&!t.directiveToIndex.has(i.type))){if(kg(i,"value")){t.flags|=1024,t.customControlIndex=e;return}if(kg(i,"checked")){t.flags|=2048,t.customControlIndex=e;return}}}if(t.hostDirectiveInputs!==null&&t.hostDirectiveOutputs!==null&&t.directiveToIndex!==null){let e=(i,r)=>{let o=t.hostDirectiveInputs[i],s=t.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let u of t.directiveToIndex.values()){if(!Array.isArray(u))continue;let[h,f,v]=u;if(c>=f&&c<=v)return t.flags|=r,t.customControlIndex=h,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function kg(n,t){return RI(n,t)&&FI(n,t+"Change")}function RI(n,t){return t in n.inputs}function FI(n,t){return t in n.outputs}var Gu=Symbol("BINDING");var Qi=new w("");function sc(n,t,e){let i=e?n.styles:null,r=e?n.classes:null,o=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")o=a;else if(o==1)r=ya(r,a);else if(o==2){let c=a,l=t[++s];i=ya(i,c+": "+l+";")}}e?n.styles=i:n.stylesWithoutHost=i,e?n.classes=r:n.classesWithoutHost=r}function $(n,t=0){let e=L();if(e===null)return M(n,t);let i=Be();return iv(i,e,ze(n),t)}function dy(n,t,e,i,r){let o=i===null?null:{"":-1},s=r(n,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}LI(n,t,e,a,o,c,l)}o!==null&&i!==null&&OI(e,i,o)}function OI(n,t,e){let i=n.localNames=[];for(let r=0;r<t.length;r+=2){let o=e[t[r+1]];if(o==null)throw new _(-301,!1);i.push(t[r],o)}}function PI(n,t,e){t.componentOffset=e,(n.components??=[]).push(t.index)}function LI(n,t,e,i,r,o,s){let a=i.length,c=null;for(let h=0;h<a;h++){let f=i[h];c===null&&dn(f)&&(c=f,PI(n,e,h)),Mu(Xa(e,t),n,f.type)}$I(e,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let l=!1,d=!1,u=Vv(n,t,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let f=i[h];if(e.mergedAttrs=Ar(e.mergedAttrs,f.hostAttrs),jI(n,e,t,u,f),UI(u,f,r),s!==null&&s.has(f)){let[E,D]=s.get(f);e.directiveToIndex.set(f.type,[u,E+e.directiveStart,D+e.directiveStart])}else(o===null||!o.has(f))&&e.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let v=f.type.prototype;!l&&(v.ngOnChanges||v.ngOnInit||v.ngDoCheck)&&((n.preOrderHooks??=[]).push(e.index),l=!0),!d&&(v.ngOnChanges||v.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}VI(n,e,o)}function VI(n,t,e){for(let i=t.directiveStart;i<t.directiveEnd;i++){let r=n.data[i];if(e===null||!e.has(r))Rg(0,t,r,i),Rg(1,t,r,i),Og(t,i,!1);else{let o=e.get(r);Fg(0,t,o,i),Fg(1,t,o,i),Og(t,i,!0)}}}function Rg(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;n===0?s=t.inputs??={}:s=t.outputs??={},s[o]??=[],s[o].push(i),uy(t,o)}}function Fg(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;n===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),uy(t,s)}}function uy(n,t){t==="class"?n.flags|=8:t==="style"&&(n.flags|=16)}function Og(n,t,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=n;if(i===null||!e&&r===null||e&&o===null||mf(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===t){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===t){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function jI(n,t,e,i,r){n.data[i]=r;let o=r.factory||(r.factory=Zn(r.type,!0)),s=new $i(o,dn(r),$,null);n.blueprint[i]=s,e[i]=s,BI(n,t,i,Vv(n,e,r.hostVars,ft),r)}function BI(n,t,e,i,r){let o=r.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~t.index;HI(s)!=a&&s.push(a),s.push(e,i,o)}}function HI(n){let t=n.length;for(;t>0;){let e=n[--t];if(typeof e=="number"&&e<0)return e}return 0}function UI(n,t,e){if(e){if(t.exportAs)for(let i=0;i<t.exportAs.length;i++)e[t.exportAs[i]]=n;dn(t)&&(e[""]=n)}}function $I(n,t,e){n.flags|=1,n.directiveStart=t,n.directiveEnd=t+e,n.providerIndexes=t}function fy(n,t,e,i,r,o,s,a){let c=t[A],l=c.consts,d=un(l,s),u=Or(c,n,e,i,d);return o&&dy(c,t,u,un(l,a),r),u.mergedAttrs=Ar(u.mergedAttrs,u.attrs),u.attrs!==null&&sc(u,u.attrs,!1),u.mergedAttrs!==null&&sc(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function my(n,t){Kg(n,t),Gd(t)&&n.queries.elementEnd(t)}function zI(n,t,e,i,r,o){let s=t.consts,a=un(s,r),c=Or(t,n,e,i,a);if(c.mergedAttrs=Ar(c.mergedAttrs,c.attrs),o!=null){let l=un(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&sc(c,c.attrs,!1),c.mergedAttrs!==null&&sc(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}var py=typeof ShadowRoot<"u",GI=typeof Document<"u";function WI(n){return Object.keys(n).map(t=>{let[e,i,r]=n[t],o={propName:e,templateName:t,isSignal:(i&gc.SignalBased)!==0};return r&&(o.transform=r),o})}function qI(n){return Object.keys(n).map(t=>({propName:n[t],templateName:t}))}function KI(n,t,e){let i=t instanceof Ge?t:t?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new ic(e,i):e}function QI(n){let t=n.get(at,null);if(t===null)throw new _(407,!1);let e=n.get(iy,null),i=n.get(sn,null),r=n.get(On,null,{optional:!0});return{rendererFactory:t,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function ZI(n,t){let e=hy(n);return Cv(t,e,e==="svg"?Wd:e==="math"?Oh:null)}function hy(n){return(n.selectors[0][0]||"div").toLowerCase()}var kr=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=WI(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=qI(this.componentDef.outputs),this.cachedOutputs}constructor(t,e){this.componentDef=t,this.ngModule=e,this.componentType=t.type,this.selector=hC(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!e}create(t,e,i,r,o,s){se(te.DynamicComponentStart);let a=F(null);try{let c=this.componentDef,l=KI(c,r||this.ngModule,t),d=QI(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(ry(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{F(a)}}createComponentRef(t,e,i,r,o,s){let a=this.componentDef,c=YI(r,a,s,o),l=t.rendererFactory.createRenderer(null,a),d=r?BC(l,r,a.encapsulation,e):ZI(a,l),u=e.get(Qi,null),h=XI(d,()=>e.get(Z,null)??fv());u&&u.addHost(h);let f=s?.some(Pg)||o?.some(D=>typeof D!="function"&&D.bindings.some(Pg)),v=wf(null,c,null,512|Lv(a),null,null,t,l,e,null,hv(d,e,!0));u&&py&&h instanceof ShadowRoot&&Aa(v,()=>{u.removeHost(h)}),v[Me]=d,Fa(v);let E=null;try{let D=fy(Me,v,2,"#host",()=>c.directiveRegistry,!0,0);xv(l,d,D),Nr(d,v),Cf(c,v,D),vv(c,D,v),my(c,D),i!==void 0&&ex(D,this.ngContentSelectors,i),E=Rt(D.index,v),v[We]=E[We],xf(c,v,null)}catch(D){throw E!==null&&Au(E),Au(v),D}finally{se(te.DynamicComponentEnd),Oa()}return new ac(this.componentType,v,!!f)}};function YI(n,t,e,i){let r=n?["ng-version","22.0.1"]:gC(t.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Gu].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let u=i[d];if(typeof u!="function")for(let h of u.bindings){a+=h[Gu].requiredVars;let f=d+1;h.create&&(h.targetIdx=f,(o??=[]).push(h)),h.update&&(h.targetIdx=f,(s??=[]).push(h))}}let c=[t];if(i)for(let d of i){let u=typeof d=="function"?d:d.type,h=Fd(u);c.push(h)}return Ef(0,null,JI(o,s),1,a,c,null,null,null,[r],null)}function XI(n,t){let e=n.getRootNode?.();return GI&&e instanceof Document?e.head:e&&py&&e instanceof ShadowRoot?e:t().head}function JI(n,t){return!n&&!t?null:e=>{if(e&1&&n)for(let i of n)i.create();if(e&2&&t)for(let i of t)i.update()}}function Pg(n){let t=n[Gu].kind;return t==="input"||t==="twoWay"}var ac=class extends ny{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Sa(e[A],Me),this.location=Fr(this._tNode,e),this.instance=Rt(this._tNode.index,e)[We],this.hostView=this.changeDetectorRef=new ci(e,void 0),this.componentType=t}setInput(t,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),e))return;let r=this._rootLView,o=If(i,r[A],r,t,e);this.previousInputValues.set(t,e);let s=Rt(i.index,r);Mf(s,1)}get injector(){return new ai(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function ex(n,t,e){let i=n.projection=[];for(let r=0;r<t.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ui=(()=>{class n{static __NG_ELEMENT_ID__=tx}return n})();function tx(){let n=Be();return gy(n,L())}var Wu=class n extends ui{_lContainer;_hostTNode;_hostLView;constructor(t,e,i){super(),this._lContainer=t,this._hostTNode=e,this._hostLView=i}get element(){return Fr(this._hostTNode,this._hostLView)}get injector(){return new ai(this._hostTNode,this._hostLView)}get parentInjector(){let t=nf(this._hostTNode,this._hostLView);if(Yg(t)){let e=Za(t,this._hostLView),i=Qa(t),r=e[A].data[i+8];return new ai(r,e)}else return new ai(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let e=Lg(this._lContainer);return e!==null&&e[t]||null}get length(){return this._lContainer.length-qe}createEmbeddedView(t,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=zu(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,tc(this._hostTNode,s)),a}createComponent(t,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new kr(Jn(t)),u=i||this.parentInjector;if(!o&&d.ngModule==null){let x=this.parentInjector.get(Ge,null);x&&(o=x)}let h=Jn(d.componentType??{}),f=zu(this._lContainer,h?.id??null),v=f?.firstChild??null,E=d.create(u,r,v,o,s,a);return this.insertImpl(E.hostView,c,tc(this._hostTNode,f)),E}insert(t,e){return this.insertImpl(t,e,!0)}insertImpl(t,e,i){let r=t._lView;if(Lh(r)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=r[je],l=new n(c,c[Xe],c[je]);l.detach(l.indexOf(t))}}let o=this._adjustIndex(e),s=this._lContainer;return Tf(s,r,o,i),t.attachToViewContainerRef(),Vd(bu(s),o,t),t}move(t,e){return this.insert(t,e)}indexOf(t){let e=Lg(this._lContainer);return e!==null?e.indexOf(t):-1}remove(t){let e=this._adjustIndex(t,-1),i=nc(this._lContainer,e);i&&(Eo(bu(this._lContainer),e),yf(i[A],i))}detach(t){let e=this._adjustIndex(t,-1),i=nc(this._lContainer,e);return i&&Eo(bu(this._lContainer),e)!=null?new ci(i):null}_adjustIndex(t,e=0){return t??this.length+e}};function Lg(n){return n[Co]}function bu(n){return n[Co]||(n[Co]=[])}function gy(n,t){let e,i=t[n.index];return At(i)?e=i:(e=ey(i,t,null,n),t[n.index]=e,Df(t,e)),ix(e,t,n,i),new Wu(e,n,t)}function nx(n,t){let e=n[_e],i=e.createComment(""),r=kt(t,n),o=e.parentNode(r);return Ja(e,o,i,e.nextSibling(r),!1),i}var ix=sx,rx=()=>!1;function ox(n,t,e){return rx(n,t,e)}function sx(n,t,e,i){if(n[ri])return;let r;e.type&8?r=Nt(i):r=nx(t,e),n[ri]=r}var qu=class n{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Ku=class n{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let e=t.queries;if(e!==null){let i=t.contentQueries!==null?t.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let e=0;e<this.queries.length;e++)Ff(t,e).matches!==null&&this.queries[e].setDirty()}},cc=class{flags;read;predicate;constructor(t,e,i=null){this.flags=e,this.read=i,typeof t=="string"?this.predicate=ux(t):this.predicate=t}},Qu=class n{queries;constructor(t=[]){this.queries=t}elementStart(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(t,e)}elementEnd(t){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(t)}embeddedTView(t){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(t,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new n(e):null}template(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(t,e)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},Zu=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,e=-1){this.metadata=t,this._declarationNodeIndex=e}elementStart(t,e){this.isApplyingToNode(e)&&this.matchTNode(t,e)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,e){this.elementStart(t,e)}embeddedTView(t,e){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,e),new n(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=t.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(t,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(t,e,ax(e,o)),this.matchTNodeWithReadOption(t,e,Wa(e,t,o,!1,!1))}else i===li?e.type&4&&this.matchTNodeWithReadOption(t,e,-1):this.matchTNodeWithReadOption(t,e,Wa(e,t,i,!1,!1))}matchTNodeWithReadOption(t,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===re||r===ui||r===li&&e.type&4)this.addMatch(e.index,-2);else{let o=Wa(e,t,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(t,e){this.matches===null?this.matches=[t,e]:this.matches.push(t,e)}};function ax(n,t){let e=n.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===t)return e[i+1]}return null}function cx(n,t){return n.type&11?Fr(n,t):n.type&4?yc(n,t):null}function lx(n,t,e,i){return e===-1?cx(t,n):e===-2?dx(n,t,i):Lo(n,n[A],e,t)}function dx(n,t,e){if(e===re)return Fr(t,n);if(e===li)return yc(t,n);if(e===ui)return gy(t,n)}function vy(n,t,e,i){let r=t[ln].queries[i];if(r.matches===null){let o=n.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(lx(t,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function Yu(n,t,e,i){let r=n.queries.getByIndex(e),o=r.matches;if(o!==null){let s=vy(n,t,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=t[-c];for(let u=qe;u<d.length;u++){let h=d[u];h[ni]===h[je]&&Yu(h[A],h,l,i)}if(d[Oi]!==null){let u=d[Oi];for(let h=0;h<u.length;h++){let f=u[h];Yu(f[A],f,l,i)}}}}}return i}function Rf(n,t){return n[ln].queries[t].queryList}function yy(n,t,e){let i=new Vo((e&4)===4);return Bh(n,t,i,i.destroy),(t[ln]??=new Ku).queries.push(new qu(i))-1}function by(n,t,e){let i=Te();return i.firstCreatePass&&(Ey(i,new cc(n,t,e),-1),(t&2)===2&&(i.staticViewQueries=!0)),yy(i,L(),t)}function _y(n,t,e,i){let r=Te();if(r.firstCreatePass){let o=Be();Ey(r,new cc(t,e,i),o.index),fx(r,n),(e&2)===2&&(r.staticContentQueries=!0)}return yy(r,L(),e)}function ux(n){return n.split(",").map(t=>t.trim())}function Ey(n,t,e){n.queries===null&&(n.queries=new Qu),n.queries.track(new Zu(t,e))}function fx(n,t){let e=n.contentQueries||(n.contentQueries=[]),i=e.length?e[e.length-1]:-1;t!==i&&e.push(n.queries.length-1,t)}function Ff(n,t){return n.queries.getByIndex(t)}function wy(n,t){let e=n[A],i=Ff(e,t);return i.crossesNgTemplate?Yu(e,n,t,[]):vy(e,n,i,t)}function Dy(n,t,e){let i,r=so(()=>{i._dirtyCounter();let o=mx(i,n);if(t&&o===void 0)throw new _(-951,!1);return o});return i=r[Pe],i._dirtyCounter=xe(0),i._flatValue=void 0,r}function Of(n){return Dy(!0,!1,n)}function Pf(n){return Dy(!0,!0,n)}function Cy(n,t){let e=n[Pe];e._lView=L(),e._queryIndex=t,e._queryList=Rf(e._lView,t),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function mx(n,t){let e=n._lView,i=n._queryIndex;if(e===void 0||i===void 0||e[R]&4)return t?void 0:Ye;let r=Rf(e,i),o=wy(e,i);return r.reset(o,sv),t?r.first:r._changesDetected||n._flatValue===void 0?n._flatValue=r.toArray():n._flatValue}function Pr(n){return!!n&&typeof n.then=="function"}function Lf(n){return!!n&&typeof n.subscribe=="function"}var Bo=class{};var Ho=class extends Bo{injector;instance=null;constructor(t){super();let e=new Ai([...t.providers,{provide:Bo,useValue:this}],t.parent||yr(),t.debugName,new Set(["environment"]));this.injector=e,t.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function Iy(n,t,e=null){return new Ho({providers:n,parent:t,debugName:e,runEnvironmentInitializers:!0}).injector}var px=(()=>{class n{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Bd(!1,e.type),r=i.length>0?Iy([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=P({token:n,providedIn:"environment",factory:()=>new n(M(Ge))})}return n})();function le(n){return $o(()=>{let t=xy(n),e=ee(I({},t),{type:n.type,decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection!==of.Eager,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&n.dependencies||null,getStandaloneInjector:t.standalone?r=>r.get(px).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||Kt.Emulated,styles:n.styles||Ye,_:null,schemas:n.schemas||null,tView:null,id:""});t.standalone&&di("NgStandalone"),Sy(e);let i=n.dependencies;return e.directiveDefs=Vg(i,hx),e.pipeDefs=Vg(i,wh),e.id=yx(e),e})}function hx(n){return Jn(n)||Fd(n)}function K(n){return $o(()=>({type:n.type,bootstrap:n.bootstrap||Ye,declarations:n.declarations||Ye,imports:n.imports||Ye,exports:n.exports||Ye,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function gx(n,t){if(n==null)return ei;let e={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=gc.None,c=null),e[o]=[i,a,c],t[o]=s}return e}function vx(n){if(n==null)return ei;let t={};for(let e in n)n.hasOwnProperty(e)&&(t[n[e]]=e);return t}function G(n){return $o(()=>{let t=xy(n);return Sy(t),t})}function Vf(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function xy(n){let t={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:t,inputConfig:n.inputs||ei,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Ye,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:gx(n.inputs,t),outputs:vx(n.outputs),debugInfo:null}}function Sy(n){n.features?.forEach(t=>t(n))}function Vg(n,t){return n?()=>{let e=typeof n=="function"?n():n,i=[];for(let r of e){let o=t(r);o!==null&&i.push(o)}return i}:null}function yx(n){let t=0,e=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,e,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of i.join("|"))t=Math.imul(31,t)+o.charCodeAt(0)<<0;return t+=2147483648,"c"+t}var jf=new w("");function Bf(n){return ti([{provide:jf,multi:!0,useValue:n}])}var Hf=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=m(jf,{optional:!0})??[];injector=m(Ie);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=br(this.injector,r);if(Pr(o))e.push(o);else if(Lf(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();function Uf(n){return t=>{t.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:n}}}function bx(n){return Object.getPrototypeOf(n.prototype).constructor}function tt(n){let t=bx(n.type),e=!0,i=[n];for(;t;){let r;if(dn(n))r=t.\u0275cmp||t.\u0275dir;else{if(t.\u0275cmp)throw new _(903,!1);r=t.\u0275dir}if(r){if(e){i.push(r);let s=n;s.inputs=_u(n.inputs),s.declaredInputs=_u(n.declaredInputs),s.outputs=_u(n.outputs);let a=r.hostBindings;a&&Cx(n,a);let c=r.viewQuery,l=r.contentQueries;if(c&&wx(n,c),l&&Dx(n,l),_x(n,r),Eh(n.outputs,r.outputs),dn(r)&&r.data.animation){let d=n.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(n),a===tt&&(e=!1)}}t=Object.getPrototypeOf(t)}Ex(i)}function _x(n,t){for(let e in t.inputs){if(!t.inputs.hasOwnProperty(e)||n.inputs.hasOwnProperty(e))continue;let i=t.inputs[e];i!==void 0&&(n.inputs[e]=i,n.declaredInputs[e]=t.declaredInputs[e])}}function Ex(n){let t=0,e=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=t+=r.hostVars,r.hostAttrs=Ar(r.hostAttrs,e=Ar(e,r.hostAttrs))}}function _u(n){return n===ei?{}:n===Ye?[]:n}function wx(n,t){let e=n.viewQuery;e?n.viewQuery=(i,r)=>{t(i,r),e(i,r)}:n.viewQuery=t}function Dx(n,t){let e=n.contentQueries;e?n.contentQueries=(i,r,o)=>{t(i,r,o),e(i,r,o)}:n.contentQueries=t}function Cx(n,t){let e=n.hostBindings;e?n.hostBindings=(i,r)=>{t(i,r),e(i,r)}:n.hostBindings=t}function My(n,t,e,i,r,o,s,a){if(e.firstCreatePass){n.mergedAttrs=Ar(n.mergedAttrs,n.attrs);let d=n.tView=Ef(2,n,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,n),d.queries=e.queries.embeddedTView(n))}a&&(n.flags|=a),Cr(n,!1);let c=xx(e,t,n,i);Pa()&&bf(e,t,c,n),Nr(c,t);let l=ey(c,t,c,n);t[i+Me]=l,Df(t,l),ox(l,n,t)}function Ix(n,t,e,i,r,o,s,a,c,l,d){let u=e+Me,h;return t.firstCreatePass?(h=Or(t,u,4,s||null,a||null),tu()&&dy(t,n,h,un(t.consts,l),Uv),Kg(t,h)):h=t.data[u],My(h,n,t,e,i,r,o,c),Io(h)&&Cf(t,n,h),l!=null&&vc(n,h,d),h}function $f(n,t,e,i,r,o,s,a,c,l,d){let u=e+Me,h;if(t.firstCreatePass){if(h=Or(t,u,4,s||null,a||null),l!=null){let f=un(t.consts,l);h.localNames=[];for(let v=0;v<f.length;v+=2)h.localNames.push(f[v],-1)}}else h=t.data[u];return My(h,n,t,e,i,r,o,c),l!=null&&vc(n,h,d),h}function me(n,t,e,i,r,o,s,a){let c=L(),l=Te(),d=un(l.consts,o);return Ix(c,l,n,t,e,i,r,d,void 0,s,a),me}var xx=Sx;function Sx(n,t,e,i){return La(!0),t[_e].createComment("")}var zf=new w("");var Gf=new w("");function Ty(){Kl(()=>{let n="";throw new _(600,n)})}var Mx=10;var Qt=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(si);afterRenderManager=m(hf);zonelessEnabled=m(No);rootEffectScheduler=m(ja);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Q;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(Vi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ne(e=>!e))}constructor(){m(On,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=m(Ge);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=Ie.NULL){return this._injector.get(O).run(()=>{if(se(te.BootstrapComponentStart),!this._injector.get(Hf).done){let x="";throw new _(405,x)}let a=Jn(e),c=this._injector.get(Bo),l=new kr(a,c);this.componentTypes.push(e);let{hostElement:d,directives:u,bindings:h}=Tx(i),f=d||l.selector,v=l.create(r,[],f,c.injector,u,h),E=v.location.nativeElement,D=v.injector.get(zf,null);return D?.registerApplication(E),v.onDestroy(()=>{this.detachView(v.hostView),Po(this.components,v),D?.unregisterApplication(E)}),this._loadComponent(v),se(te.BootstrapComponentEnd,v),v})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){se(te.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(mc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw se(te.ChangeDetectionEnd),new _(101,!1);let e=F(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,F(e),this.afterTick.next(),se(te.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(at,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<Mx;){se(te.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{se(te.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!xo(r))continue;let o=i&&!this.zonelessEnabled?0:1;Zv(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>xo(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Po(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Gf,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Po(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new _(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();function Tx(n){return n===void 0||typeof n=="string"||n instanceof Element?{hostElement:n}:n}function Po(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function Se(n,t,e,i){let r=L(),o=Ir();if(kn(r,o,t)){let s=Te(),a=Mo();QC(a,r,n,t,e,i)}return Se}function Ee(n,t,e,i,r,o,s,a){di("NgControlFlow");let c=L(),l=Te(),d=un(l.consts,o);return $f(c,l,n,t,e,i,r,d,256,s,a),Wf}function Wf(n,t,e,i,r,o,s,a){di("NgControlFlow");let c=L(),l=Te(),d=un(l.consts,o);return $f(c,l,n,t,e,i,r,d,512,s,a),Wf}function we(n,t){di("NgControlFlow");let e=L(),i=Ir(),r=e[i]!==ft?e[i]:-1,o=r!==-1?jg(e,Me+r):void 0,s=0;if(kn(e,i,n)){let a=F(null);try{if(o!==void 0&&gI(o,s),n!==-1){let c=Me+n,l=jg(e,c),d=Ax(e[A],c),u=CI(l,d,e),h=Sf(e,d,t,{dehydratedView:u});Tf(l,h,s,tc(d,u))}}finally{F(a)}}else if(o!==void 0){let a=hI(o,s);a!==void 0&&(a[We]=t)}}function jg(n,t){return n[t]}function Ax(n,t){return Sa(n,t)}function N(n,t,e){let i=L(),r=Ir();if(kn(i,r,t)){let o=Te(),s=Mo();zC(s,i,n,t,i[_e],e)}return N}function Xu(n,t,e,i,r){If(t,n,e,r?"class":"style",i)}function g(n,t,e,i){let r=L(),o=r[A],s=n+Me,a=o.firstCreatePass?fy(s,r,2,t,Uv,tu(),e,i):o.data[s];if(Tn(a)){let c=r[cn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(ry(l),()=>(Bg(n,t,r,a,i),g))}}return Bg(n,t,r,a,i),g}function Bg(n,t,e,i,r){if($v(i,e,n,t,Ay),Io(i)){let o=e[A];Cf(o,e,i),vv(o,i,e)}r!=null&&vc(e,i)}function p(){let n=Te(),t=Be(),e=zv(t);return n.firstCreatePass&&my(n,e),iu(e)&&ru(),eu(),e.classesWithoutHost!=null&&hD(e)&&Xu(n,e,L(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&gD(e)&&Xu(n,e,L(),e.stylesWithoutHost,!1),p}function Re(n,t,e,i){return g(n,t,e,i),p(),Re}function Ot(n,t,e,i){let r=L(),o=r[A],s=n+Me,a=o.firstCreatePass?zI(s,o,2,t,e,i):o.data[s];return $v(a,r,n,t,Ay),i!=null&&vc(r,a),Ot}function Pt(){let n=Be(),t=zv(n);return iu(t)&&ru(),eu(),Pt}function hn(n,t,e,i){return Ot(n,t,e,i),Pt(),hn}var Ay=(n,t,e,i,r)=>(La(!0),Cv(t[_e],i,uu()));function wt(){return L()}function gn(n,t,e){let i=L(),r=Ir();if(kn(i,r,t)){let o=Te(),s=Mo();Hv(s,i,n,t,i[_e],e)}return gn}var Ro=void 0;function Nx(n){let t=Math.floor(Math.abs(n)),e=n.toString().replace(/^[^.]*\.?/,"").length;return t===1&&e===0?1:5}var kx=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Ro,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Ro,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Ro,Ro,Ro],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",Nx],Eu=Object.create(null);function Dt(n){let t=Rx(n),e=Hg(t);if(e)return e;let i=t.split("-")[0];if(e=Hg(i),e)return e;if(i==="en")return kx;throw new _(701,!1)}function Hg(n){return n in Eu||(Eu[n]=an.ng&&an.ng.common&&an.ng.common.locales&&an.ng.common.locales[n]),Eu[n]}var Fe={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function Rx(n){return n.toLowerCase().replace(/_/g,"-")}var Go="en-US";var Fx=Go;function Ny(n){typeof n=="string"&&(Fx=n.toLowerCase().replace(/_/g,"-"))}function ie(n,t,e){let i=L(),r=Te(),o=Be();return Ox(r,i,i[_e],o,n,t,e),ie}function Ox(n,t,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Tr(i,t,o),cy(i,n,t,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let h=d[u],f=d[u+1];c??=Tr(i,t,o),rc(i,t,h,f,r,c)}if(l&&l.length)for(let u of l)c??=Tr(i,t,o),rc(i,t,u,r,r,c)}}function H(n=1){return tg(n)}function Px(n,t){let e=null,i=dC(n);for(let r=0;r<t.length;r++){let o=t[r];if(o==="*"){e=r;continue}if(i===null?Mv(n,o,!0):mC(i,o))return r}return e}function Ke(n){let t=L()[ut][Xe];if(!t.projection){let e=n?n.length:1,i=t.projection=Mh(e,null),r=i.slice(),o=t.child;for(;o!==null;){if(o.type!==128){let s=n?Px(o,n):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ae(n,t=0,e,i,r,o){let s=L(),a=Te(),c=i?n+1:null;c!==null&&$f(s,a,c,i,r,o,null,e);let l=Or(a,Me+n,16,null,e||null);l.projection===null&&(l.projection=t),au();let u=!s[_r]||nu();s[ut][Xe].projection[l.projection]===null&&c!==null?Lx(s,a,c):u&&!dc(l)&&FC(a,s,l)}function Lx(n,t,e){let i=Me+e,r=t.data[i],o=n[i],s=zu(o,r.tView.ssrId),a=Sf(n,r,void 0,{dehydratedView:s});Tf(o,a,0,tc(r,s))}function Lr(n,t,e,i){return _y(n,t,e,i),Lr}function Pn(n,t,e){return by(n,t,e),Pn}function de(n){let t=L(),e=Te(),i=Ra();So(i+1);let r=Ff(e,i);if(n.dirty&&Ph(t)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let o=wy(t,i);n.reset(o,sv),n.notifyOnChanges()}return!0}return!1}function ue(){return Rf(L(),Ra())}function bc(n,t,e,i,r){return Cy(t,_y(n,e,i,r)),bc}function _c(n,t,e,i){return Cy(n,by(t,e,i)),_c}function Ec(n=1){So(Ra()+n)}function Vr(n){let t=zh();return Kd(t,Me+n)}function $a(n,t){return n<<17|t<<2}function Wi(n){return n>>17&32767}function Vx(n){return(n&2)==2}function jx(n,t){return n&131071|t<<17}function Ju(n){return n|2}function Rr(n){return(n&131068)>>2}function wu(n,t){return n&-131069|t<<2}function Bx(n){return(n&1)===1}function ef(n){return n|1}function Hx(n,t,e,i,r,o){let s=o?t.classBindings:t.styleBindings,a=Wi(s),c=Rr(s);n[i]=e;let l=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||gr(u,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let h=Wi(n[a+1]);n[i+1]=$a(h,a),h!==0&&(n[h+1]=wu(n[h+1],i)),n[a+1]=jx(n[a+1],i)}else n[i+1]=$a(a,0),a!==0&&(n[a+1]=wu(n[a+1],i)),a=i;else n[i+1]=$a(c,0),a===0?a=i:n[c+1]=wu(n[c+1],i),c=i;l&&(n[i+1]=Ju(n[i+1])),Ug(n,d,i,!0),Ug(n,d,i,!1),Ux(t,d,n,i,o),s=$a(a,c),o?t.classBindings=s:t.styleBindings=s}function Ux(n,t,e,i,r){let o=r?n.residualClasses:n.residualStyles;o!=null&&typeof t=="string"&&gr(o,t)>=0&&(e[i+1]=ef(e[i+1]))}function Ug(n,t,e,i){let r=n[e+1],o=t===null,s=i?Wi(r):Rr(r),a=!1;for(;s!==0&&(a===!1||o);){let c=n[s],l=n[s+1];$x(c,t)&&(a=!0,n[s+1]=i?ef(l):Ju(l)),s=i?Wi(l):Rr(l)}a&&(n[e+1]=i?Ju(r):ef(r))}function $x(n,t){return n===null||t==null||(Array.isArray(n)?n[1]:n)===t?!0:Array.isArray(n)&&typeof t=="string"?gr(n,t)>=0:!1}var qt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function zx(n){return n.substring(qt.key,qt.keyEnd)}function Gx(n){return Wx(n),ky(n,Ry(n,0,qt.textEnd))}function ky(n,t){let e=qt.textEnd;return e===t?-1:(t=qt.keyEnd=qx(n,qt.key=t,e),Ry(n,t,e))}function Wx(n){qt.key=0,qt.keyEnd=0,qt.value=0,qt.valueEnd=0,qt.textEnd=n.length}function Ry(n,t,e){for(;t<e&&n.charCodeAt(t)<=32;)t++;return t}function qx(n,t,e){for(;t<e&&n.charCodeAt(t)>32;)t++;return t}function Wo(n,t,e){return Fy(n,t,e,!1),Wo}function W(n,t){return Fy(n,t,null,!0),W}function pt(n){Qx(tS,Kx,n,!0)}function Kx(n,t){for(let e=Gx(t);e>=0;e=ky(t,e))Da(n,zx(t),!0)}function Fy(n,t,e,i){let r=L(),o=Te(),s=Na(2);if(o.firstUpdatePass&&Py(o,n,s,i),t!==ft&&kn(r,s,t)){let a=o.data[An()];Ly(o,a,r,r[_e],n,r[s+1]=iS(t,e),i,s)}}function Qx(n,t,e,i){let r=Te(),o=Na(2);r.firstUpdatePass&&Py(r,null,o,i);let s=L();if(e!==ft&&kn(s,o,e)){let a=r.data[An()];if(Vy(a,i)&&!Oy(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=ya(c,e||"")),Xu(r,a,s,e,i)}else nS(r,a,s,s[_e],s[o+1],s[o+1]=eS(n,t,e),i,o)}}function Oy(n,t){return t>=n.expandoStartIndex}function Py(n,t,e,i){let r=n.data;if(r[e+1]===null){let o=r[An()],s=Oy(n,e);Vy(o,i)&&t===null&&!s&&(t=!1),t=Zx(r,o,t,i),Hx(r,o,t,e,s,i)}}function Zx(n,t,e,i){let r=Yh(n),o=i?t.residualClasses:t.residualStyles;if(r===null)(i?t.classBindings:t.styleBindings)===0&&(e=Du(null,n,t,e,i),e=Uo(e,t.attrs,i),o=null);else{let s=t.directiveStylingLast;if(s===-1||n[s]!==r)if(e=Du(r,n,t,e,i),o===null){let c=Yx(n,t,i);c!==void 0&&Array.isArray(c)&&(c=Du(null,n,t,c[1],i),c=Uo(c,t.attrs,i),Xx(n,t,i,c))}else o=Jx(n,t,i)}return o!==void 0&&(i?t.residualClasses=o:t.residualStyles=o),e}function Yx(n,t,e){let i=e?t.classBindings:t.styleBindings;if(Rr(i)!==0)return n[Wi(i)]}function Xx(n,t,e,i){let r=e?t.classBindings:t.styleBindings;n[Wi(r)]=i}function Jx(n,t,e){let i,r=t.directiveEnd;for(let o=1+t.directiveStylingLast;o<r;o++){let s=n[o].hostAttrs;i=Uo(i,s,e)}return Uo(i,t.attrs,e)}function Du(n,t,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=t[a],i=Uo(i,o.hostAttrs,r),o!==n);)a++;return n!==null&&(e.directiveStylingLast=a),i}function Uo(n,t,e){let i=e?1:2,r=-1;if(t!==null)for(let o=0;o<t.length;o++){let s=t[o];typeof s=="number"?r=s:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Da(n,s,e?!0:t[++o]))}return n===void 0?null:n}function eS(n,t,e){if(e==null||e==="")return Ye;let i=[],r=Rn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)n(i,r[o],!0);else if(r instanceof Set)for(let o of r)n(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&n(i,o,r[o]);else typeof r=="string"&&t(i,r);return i}function tS(n,t,e){let i=String(t);i!==""&&!i.includes(" ")&&Da(n,i,e)}function nS(n,t,e,i,r,o,s,a){r===ft&&(r=Ye);let c=0,l=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let h=c<r.length?r[c+1]:void 0,f=l<o.length?o[l+1]:void 0,v=null,E;d===u?(c+=2,l+=2,h!==f&&(v=u,E=f)):u===null||d!==null&&d<u?(c+=2,v=d):(l+=2,v=u,E=f),v!==null&&Ly(n,t,e,i,v,E,s,a),d=c<r.length?r[c]:null,u=l<o.length?o[l]:null}}function Ly(n,t,e,i,r,o,s,a){if(!(t.type&3))return;let c=n.data,l=c[a+1],d=Bx(l)?$g(c,t,e,r,Rr(l),s):void 0;if(!lc(d)){lc(o)||Vx(l)&&(o=$g(c,null,e,r,a,s));let u=qd(An(),e);PC(i,s,u,r,o)}}function $g(n,t,e,i,r,o){let s=t===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),d=l?c[1]:c,u=d===null,h=e[r+1];h===ft&&(h=u?Ye:void 0);let f=u?Ca(h,i):d===i?h:void 0;if(l&&!lc(f)&&(f=Ca(c,i)),lc(f)&&(a=f,s))return a;let v=n[r+1];r=s?Wi(v):Rr(v)}if(t!==null){let c=o?t.residualClasses:t.residualStyles;c!=null&&(a=Ca(c,i))}return a}function lc(n){return n!==void 0}function iS(n,t){return n==null||n===""||(typeof t=="string"?n=n+t:typeof n=="object"&&(n=va(Rn(n)))),n}function Vy(n,t){return(n.flags&(t?8:16))!==0}function y(n,t=""){let e=L(),i=Te(),r=n+Me,o=i.firstCreatePass?Or(i,r,1,t,null):i.data[r],s=rS(i,e,o,t);e[r]=s,Pa()&&bf(i,e,s,o),Cr(o,!1)}var rS=(n,t,e,i)=>(La(!0),eC(t[_e],i));function oS(n,t,e,i=""){return kn(n,Ir(),e)?t+_o(e)+i:ft}function sS(n,t,e,i,r,o=""){let s=Wh(),a=ay(n,s,e,r);return Na(2),a?t+_o(e)+i+_o(r)+o:ft}function T(n){return He("",n),T}function He(n,t,e){let i=L(),r=oS(i,n,t,e);return r!==ft&&jy(i,An(),r),He}function wc(n,t,e,i,r){let o=L(),s=sS(o,n,t,e,i,r);return s!==ft&&jy(o,An(),s),wc}function jy(n,t,e){let i=qd(t,n);tC(n[_e],i,e)}function zg(n,t,e){let i=Te();i.firstCreatePass&&By(t,i.data,i.blueprint,dn(n),e)}function By(n,t,e,i,r){if(n=ze(n),Array.isArray(n))for(let o=0;o<n.length;o++)By(n[o],t,e,i,r);else{let o=Te(),s=L(),a=Be(),c=Ti(n)?n:ze(n.provide),l=Ud(n),d=a.providerIndexes&1048575,u=a.directiveStart,h=a.providerIndexes>>20;if(Ti(n)||!n.multi){let f=new $i(l,r,$,null),v=Iu(c,t,r?d:d+h,u);v===-1?(Mu(Xa(a,s),o,c),Cu(o,n,t.length),t.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(f),s.push(f)):(e[v]=f,s[v]=f)}else{let f=Iu(c,t,d+h,u),v=Iu(c,t,d,d+h),E=f>=0&&e[f],D=v>=0&&e[v];if(r&&!D||!r&&!E){Mu(Xa(a,s),o,c);let x=lS(r?cS:aS,e.length,r,i,l,n);!r&&D&&(e[v].providerFactory=x),Cu(o,n,t.length,0),t.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(x),s.push(x)}else{let x=Hy(e[r?v:f],l,!r&&i);Cu(o,n,f>-1?f:v,x)}!r&&i&&D&&e[v].componentProviders++}}}function Cu(n,t,e,i){let r=Ti(t),o=kh(t);if(r||o){let c=(o?ze(t.useClass):t).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&t.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function Hy(n,t,e){return e&&n.componentProviders++,n.multi.push(t)-1}function Iu(n,t,e,i){for(let r=e;r<i;r++)if(t[r]===n)return r;return-1}function aS(n,t,e,i,r){return tf(this.multi,[])}function cS(n,t,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Lo(i,i[A],this.providerFactory.index,r);s=c.slice(0,a),tf(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],tf(o,s);return s}function tf(n,t){for(let e=0;e<n.length;e++){let i=n[e];t.push(i())}return t}function lS(n,t,e,i,r,o){let s=new $i(n,e,$,null);return s.multi=[],s.index=t,s.componentProviders=0,Hy(s,r,i&&!e),s}function nt(n,t){return e=>{e.providersResolver=(i,r)=>zg(i,r?r(n):n,!1),t&&(e.viewProvidersResolver=(i,r)=>zg(i,r?r(t):t,!0))}}function dS(n,t){let e=n[t];return e===ft?void 0:e}function uS(n,t,e,i,r,o,s){let a=t+e;return ay(n,a,r,o)?xI(n,a+2,s?i.call(s,r,o):i(r,o)):dS(n,a+2)}function Dc(n,t){let e=Te(),i,r=n+Me;e.firstCreatePass?(i=fS(t,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Zn(i.type,!0)),s,a=st($);try{let c=Ya(!1),l=o();return Ya(c),Qd(e,L(),r,l),l}finally{st(a)}}function fS(n,t){if(t)for(let e=t.length-1;e>=0;e--){let i=t[e];if(n===i.name)return i}}function Cc(n,t,e,i){let r=n+Me,o=L(),s=Kd(o,r);return mS(o,r)?uS(o,Gh(),t,s.transform,e,i,s):s.transform(e,i)}function mS(n,t){return n[A].data[t].pure}function qf(n,t){return yc(n,t)}var Uy=(()=>{class n{applicationErrorHandler=m(si);appRef=m(Qt);taskService=m(Vi);ngZone=m(O);zonelessEnabled=m(No);tracing=m(On,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Ne;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(yo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(hu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?sg:fu;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(yo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();function $y(){return[{provide:sn,useExisting:Uy},{provide:O,useClass:bo},{provide:No,useValue:!0}]}function pS(){return typeof $localize<"u"&&$localize.locale||Go}var qo=new w("",{factory:()=>m(qo,{optional:!0,skipSelf:!0})||pS()});function Ue(n,t){return so(n,t?.equal)}function ht(n){return $p(n)}var Yy=Symbol("InputSignalNode#UNSET"),SS=ee(I({},ao),{transformFn:void 0,applyValueToInputSignal(n,t){cr(n,t)}});function Xy(n,t){let e=Object.create(SS);e.value=n,e.transformFn=t?.transform;function i(){if(vi(e),e.value===Yy){let r=null;throw new _(-950,r)}return e.value}return i[Pe]=e,i}var jr=class{attributeName;constructor(t){this.attributeName=t}__NG_ELEMENT_ID__=()=>rf(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},Jy=(()=>{let n=new w("");return n.__NG_ELEMENT_ID__=t=>{let e=Be();if(e===null)throw new _(-204,!1);if(e.type&2)return e.value;if(t&8)return null;throw new _(-204,!1)},n})();function zy(n,t){return Xy(n,t)}function MS(n){return Xy(Yy,n)}var eb=(zy.required=MS,zy);function Gy(n,t){return Of(t)}function TS(n,t){return Pf(t)}var Qo=(Gy.required=TS,Gy);function Wy(n,t){return Of(t)}function AS(n,t){return Pf(t)}var tb=(Wy.required=AS,Wy);var NS=1e4;var J$=NS-1e3;var Qf=class{supports(t){return Af(t)}create(t){return new Zf(t)}},kS=(n,t)=>t,Zf=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(t){this._trackByFn=t||kS}forEachItem(t){let e;for(e=this._itHead;e!==null;e=e._next)t(e)}forEachOperation(t){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<qy(i,r,o)?e:i,a=qy(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let h=0;h<l;h++){let f=h<o.length?o[h]:o[h]=0,v=f+h;d<=v&&v<l&&(o[h]=f+1)}let u=s.previousIndex;o[u]=d-l}}a!==c&&t(s,a,c)}}forEachPreviousItem(t){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)t(e)}forEachAddedItem(t){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)t(e)}forEachMovedItem(t){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)t(e)}forEachRemovedItem(t){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)t(e)}forEachIdentityChange(t){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)t(e)}diff(t){if(t==null&&(t=[]),!Af(t))throw new _(900,!1);return this.check(t)?this:null}onDestroy(){}check(t){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(t)){this.length=t.length;for(let a=0;a<this.length;a++)o=t[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,oy(t,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=t,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let t;for(t=this._previousItHead=this._itHead;t!==null;t=t._next)t._nextPrevious=t._next;for(t=this._additionsHead;t!==null;t=t._nextAdded)t.previousIndex=t.currentIndex;for(this._additionsHead=this._additionsTail=null,t=this._movesHead;t!==null;t=t._nextMoved)t.previousIndex=t.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(t,e,i,r){let o;return t===null?o=this._itTail:(o=t._prev,this._remove(t)),t=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._reinsertAfter(t,o,r)):(t=this._linkedRecords===null?null:this._linkedRecords.get(i,r),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._moveAfter(t,o,r)):t=this._addAfter(new Yf(e,i),o,r)),t}_verifyReinsertion(t,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?t=this._reinsertAfter(o,t._prev,r):t.currentIndex!=r&&(t.currentIndex=r,this._addToMoves(t,r)),t}_truncate(t){for(;t!==null;){let e=t._next;this._addToRemovals(this._unlink(t)),t=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(t,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(t);let r=t._prevRemoved,o=t._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(t,e,i),this._addToMoves(t,i),t}_moveAfter(t,e,i){return this._unlink(t),this._insertAfter(t,e,i),this._addToMoves(t,i),t}_addAfter(t,e,i){return this._insertAfter(t,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=t:this._additionsTail=this._additionsTail._nextAdded=t,t}_insertAfter(t,e,i){let r=e===null?this._itHead:e._next;return t._next=r,t._prev=e,r===null?this._itTail=t:r._prev=t,e===null?this._itHead=t:e._next=t,this._linkedRecords===null&&(this._linkedRecords=new xc),this._linkedRecords.put(t),t.currentIndex=i,t}_remove(t){return this._addToRemovals(this._unlink(t))}_unlink(t){this._linkedRecords!==null&&this._linkedRecords.remove(t);let e=t._prev,i=t._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,t}_addToMoves(t,e){return t.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=t:this._movesTail=this._movesTail._nextMoved=t),t}_addToRemovals(t){return this._unlinkedRecords===null&&(this._unlinkedRecords=new xc),this._unlinkedRecords.put(t),t.currentIndex=null,t._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=t,t._prevRemoved=null):(t._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=t),t}_addIdentityChange(t,e){return t.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=t:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=t,t}},Yf=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(t,e){this.item=t,this.trackById=e}},Xf=class{_head=null;_tail=null;add(t){this._head===null?(this._head=this._tail=t,t._nextDup=null,t._prevDup=null):(this._tail._nextDup=t,t._prevDup=this._tail,t._nextDup=null,this._tail=t)}get(t,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,t))return i;return null}remove(t){let e=t._prevDup,i=t._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},xc=class{map=new Map;put(t){let e=t.trackById,i=this.map.get(e);i||(i=new Xf,this.map.set(e,i)),i.add(t)}get(t,e){let i=t,r=this.map.get(i);return r?r.get(t,e):null}remove(t){let e=t.trackById;return this.map.get(e).remove(t)&&this.map.delete(e),t}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function qy(n,t,e){let i=n.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+t+r}function Ky(){return new tm([new Qf])}var tm=(()=>{class n{factories;static \u0275prov=P({token:n,providedIn:"root",factory:Ky});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new n(e)}static extend(e){return{provide:n,useFactory:()=>{let i=m(n,{optional:!0,skipSelf:!0});return n.create(e,i||Ky())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new _(901,!1)}}return n})();var vn=(()=>{class n{static __NG_ELEMENT_ID__=RS}return n})();function RS(n){return FS(Be(),L(),(n&16)===16)}function FS(n,t,e){if(Tn(n)&&!e){let i=Rt(n.index,t);return new ci(i,i)}else if(n.type&175){let i=t[ut];return new ci(i,t)}return null}var Jf=new w(""),OS=new w("");function Ko(n){return!n.moduleRef}function PS(n){let t=Ko(n)?n.r3Injector:n.moduleRef.injector,e=t.get(O);return e.run(()=>{Ko(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=t.get(si),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ko(n)){let o=()=>t.destroy(),s=n.platformInjector.get(Jf);s.add(o),t.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(Jf);s.add(o),n.moduleRef.onDestroy(()=>{Po(n.allPlatformModules,n.moduleRef),r.unsubscribe(),s.delete(o)})}return VS(i,e,()=>{let o=t.get(Vi),s=o.add(),a=t.get(Hf);return a.runInitializers(),a.donePromise.then(()=>{let c=t.get(qo,Go);if(Ny(c||Go),!t.get(OS,!0))return Ko(n)?t.get(Qt):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Ko(n)){let d=t.get(Qt);return n.rootComponent!==void 0&&d.bootstrap(n.rootComponent),d}else return LS?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var LS;function VS(n,t,e){try{let i=e();return Pr(i)?i.catch(r=>{throw t.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw t.runOutsideAngular(()=>n(i)),i}}var Ic=null;function jS(n=[],t){return Ie.create({name:t,providers:[{provide:Do,useValue:"platform"},{provide:Jf,useValue:new Set([()=>Ic=null])},...n]})}function BS(n=[]){if(Ic)return Ic;let t=jS(n);return Ic=t,Ty(),HS(t),t}function HS(n){let t=n.get(Va,null);br(n,()=>{t?.forEach(e=>e())})}function nb(n){let{rootComponent:t,appProviders:e,platformProviders:i,platformRef:r}=n;se(te.BootstrapApplicationStart);try{let o=r?.injector??BS(i),s=[$y(),cg,...e||[]],a=new Ho({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return PS({r3Injector:a.injector,platformInjector:o,rootComponent:t})}catch(o){return Promise.reject(o)}finally{se(te.BootstrapApplicationEnd)}}function pe(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function Br(n,t=NaN){return!isNaN(parseFloat(n))&&!isNaN(Number(n))?Number(n):t}var Kf=Symbol("NOT_SET"),ib=new Set,US=ee(I({},ao),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Kf,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(n){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Kf&&!sr(this))return this.signal;try{for(let r of this.cleanup??ib)r()}finally{this.cleanup?.clear()}let t=[];n!==void 0&&t.push(n),t.push(this.registerCleanupFn);let e=Gn(this),i;try{i=this.userFn.apply(null,t)}finally{yi(this,e)}return(this.value===Kf||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),em=class extends ec{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,e,i,r,o,s=null){super(t,[void 0,void 0,void 0,void 0],i,!1,o.get(fn),s),this.scheduler=r;for(let a of gf){let c=e[a];if(c===void 0)continue;let l=Object.create(US);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(vi(l),l.value),l.signal[Pe]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let e of t.cleanup??ib)e()}finally{Wn(t)}}};function rb(n,t){let e=t?.injector??m(Ie),i=e.get(sn),r=e.get(hf),o=e.get(On,null,{optional:!0});r.impl??=e.get(Tv);let s=n;typeof s=="function"&&(s={mixedReadWrite:n});let a=e.get(Ao,null,{optional:!0}),c=new em(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function ob(n,t){let e=Jn(n),i=t.elementInjector||yr();return new kr(e).create(i,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}function sb(){return!1}function Zo(n,t){t=encodeURIComponent(t);for(let e of n.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===t)return decodeURIComponent(o)}return null}var ab=null;function Lt(){return ab}function nm(n){ab??=n}var Yo=class{},Sc=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:()=>m(cb),providedIn:"platform"})}return n})();var cb=(()=>{class n extends Sc{_location;_history;_doc=m(Z);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Lt().getBaseHref(this._doc)}onPopState(e){let i=Lt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Lt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:()=>new n,providedIn:"platform"})}return n})();var Vn=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let e=this.headers.get(t.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,e){return this.clone({name:t,value:e,op:"a"})}set(t,e){return this.clone({name:t,value:e,op:"s"})}delete(t,e){return this.clone({name:t,value:e,op:"d"})}maybeSetNormalizedName(t,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,t)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init(),Array.from(t.headers.keys()).forEach(e=>{this.headers.set(e,t.headers.get(e)),this.normalizedNames.set(e,t.normalizedNames.get(e))})}clone(t){let e=new n;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([t]),e}applyUpdate(t){let e=t.name.toLowerCase();switch(t.op){case"a":case"s":let i=t.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(t.name,e);let r=(t.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=t.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(t,e){let i=t.toLowerCase();this.maybeSetNormalizedName(t,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(t,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=t.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(t,r)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>t(this.normalizedNames.get(e),this.headers.get(e)))}};var Tc=class{map=new Map;set(t,e){return this.map.set(t,e),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},Ac=class{encodeKey(t){return lb(t)}encodeValue(t){return lb(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function zS(n,t){let e=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[t.decodeKey(r),""]:[t.decodeKey(r.slice(0,o)),t.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var GS=/%(\d[a-f0-9])/gi,WS={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function lb(n){return encodeURIComponent(n).replace(GS,(t,e)=>WS[e]??t)}function Mc(n){return`${n}`}var Ln=class n{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new Ac,t.fromString){if(t.fromObject)throw new _(2805,!1);this.map=zS(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(e=>{let i=t.fromObject[e],r=Array.isArray(i)?i.map(Mc):[Mc(i)];this.map.set(e,r)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let e=this.map.get(t);return e?e[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,e){return this.clone({param:t,value:e,op:"a"})}appendAll(t){let e=[];return Object.keys(t).forEach(i=>{let r=t[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(t,e){return this.clone({param:t,value:e,op:"s"})}delete(t,e){return this.clone({param:t,value:e,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let e=this.encoder.encodeKey(t);return this.map.get(t).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let e=new n({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(t),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(t=>this.map.set(t,this.cloneFrom.map.get(t))),this.updates.forEach(t=>{switch(t.op){case"a":case"s":let e=(t.op==="a"?this.map.get(t.param):void 0)||[];e.push(Mc(t.value)),this.map.set(t.param,e);break;case"d":if(t.value!==void 0){let i=this.map.get(t.param)||[],r=i.indexOf(Mc(t.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(t.param,i):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null)}};function qS(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function db(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function ub(n){return typeof Blob<"u"&&n instanceof Blob}function fb(n){return typeof FormData<"u"&&n instanceof FormData}function KS(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var im="Content-Type",mb="Accept",hb="text/plain",gb="application/json",QS=`${gb}, ${hb}, */*`,Hr=class n{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,e,i,r){this.url=e,this.method=t.toUpperCase();let o;if(qS(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new _(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Vn,this.context??=new Tc,!this.params)this.params=new Ln,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),u=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+u+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||db(this.body)||ub(this.body)||fb(this.body)||KS(this.body)?this.body:this.body instanceof Ln?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||fb(this.body)?null:ub(this.body)?this.body.type||null:db(this.body)?null:typeof this.body=="string"?hb:this.body instanceof Ln?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?gb:null}clone(t={}){let e=t.method||this.method,i=t.url||this.url,r=t.responseType||this.responseType,o=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,c=t.mode||this.mode,l=t.redirect||this.redirect,d=t.credentials||this.credentials,u=t.referrer??this.referrer,h=t.integrity||this.integrity,f=t.referrerPolicy||this.referrerPolicy,v=t.transferCache??this.transferCache,E=t.timeout??this.timeout,D=t.body!==void 0?t.body:this.body,x=t.withCredentials??this.withCredentials,X=t.reportProgress??this.reportProgress,ye=t.reportUploadProgress??this.reportUploadProgress,be=t.reportDownloadProgress??this.reportDownloadProgress,Ct=t.headers||this.headers,Le=t.params||this.params,ke=t.context??this.context;return t.setHeaders!==void 0&&(Ct=Object.keys(t.setHeaders).reduce((Ae,ot)=>Ae.set(ot,t.setHeaders[ot]),Ct)),t.setParams&&(Le=Object.keys(t.setParams).reduce((Ae,ot)=>Ae.set(ot,t.setParams[ot]),Le)),new n(e,i,D,{params:Le,headers:Ct,context:ke,reportProgress:X,reportUploadProgress:ye,reportDownloadProgress:be,responseType:r,withCredentials:x,transferCache:v,keepalive:o,cache:a,priority:s,timeout:E,mode:c,redirect:l,credentials:d,referrer:u,integrity:h,referrerPolicy:f})}},Ur=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(Ur||{}),$r=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,e=200,i="OK"){this.headers=t.headers||new Vn,this.status=t.status!==void 0?t.status:e,this.statusText=t.statusText||i,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},Nc=class n extends $r{constructor(t={}){super(t)}type=Ur.ResponseHeader;clone(t={}){return new n({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},Xo=class n extends $r{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=Ur.Response;clone(t={}){return new n({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},Zi=class extends $r{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},ZS=200;var YS=/^\)\]\}',?\n/,y3=1024*1024,vb=new w("",{factory:()=>null}),kc=(()=>{class n{fetchImpl=m(om,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=m(O);destroyRef=m(fn);maxResponseSize=m(vb);handle(e){return new q(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(sm,s=>i.error(new Zi({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return bn(this,null,function*(){let o=this.createRequestInit(e),s;try{let D=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,I({signal:i},o)));XS(D),r.next({type:Ur.Sent}),s=yield D}catch(D){r.error(new Zi({error:D,status:D.status??0,statusText:D.statusText,url:e.urlWithParams,headers:D.headers}));return}let a=new Vn(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,u=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new Nc({headers:a,status:d,statusText:c,url:l})),s.body){let D=s.headers.get("content-length"),x=D!==null?Number(D):NaN;this.maxResponseSize!==null&&Number.isFinite(x)&&x>this.maxResponseSize&&pb(this.maxResponseSize);let X=[],ye=s.body.getReader(),be=0,Ct,Le,ke=typeof Zone<"u"&&Zone.current,Ae=!1;if(yield this.ngZone.runOutsideAngular(()=>bn(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield ye.cancel(),Ae=!0;break}let{done:It,value:or}=yield ye.read();if(It)break;if(X.push(or),be+=or.length,this.maxResponseSize!==null&&be>this.maxResponseSize&&(yield ye.cancel(),pb(this.maxResponseSize)),h){Le=e.responseType==="text"?(Le??"")+(Ct??=new TextDecoder).decode(or,{stream:!0}):void 0;let nn=()=>r.next({type:Ur.DownloadProgress,total:Number.isFinite(x)?x:void 0,loaded:be,partialText:Le});ke?ke.run(nn):nn()}}})),Ae){r.complete();return}let ot=this.concatChunks(X,be);try{let It=s.headers.get(im)??"";u=this.parseBody(e,ot,It,d)}catch(It){r.error(new Zi({error:It,headers:new Vn(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=u?ZS:0);let f=d>=200&&d<300,v=s.redirected,E=s.type;f?(r.next(new Xo({body:u,headers:a,status:d,statusText:c,url:l,redirected:v,responseType:E})),r.complete()):r.error(new Zi({error:u,headers:a,status:d,statusText:c,url:l,redirected:v,responseType:E}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(YS,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new _(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(mb)||(i[mb]=QS),!e.headers.has(im)){let o=e.detectContentTypeHeader();o!==null&&(i[im]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})(),om=class{};function sm(){}function XS(n){n.then(sm,sm)}function pb(n){throw new _(2825,!1)}function JS(n,t){return t(n)}function eM(n,t,e){return(i,r)=>br(e,()=>t(i,o=>n(o,r)))}var yb=new w("",{factory:()=>[]}),bb=new w(""),_b=new w("",{factory:()=>!0});var am=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=M(kc),r},providedIn:"root"})}return n})();var Rc=(()=>{class n{backend;injector;chain=null;pendingTasks=m(Ba);contributeToStability=m(_b);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(yb),...this.injector.get(bb,[])]));this.chain=i.reduceRight((r,o)=>eM(r,o,this.injector),JS)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(fo(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(M(am),M(Ge))};static \u0275prov=P({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),cm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=M(Rc),r},providedIn:"root"})}return n})();function rm(n,t){return I({body:t},n)}var zr=(()=>{class n{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Hr)o=e;else{let c;r.headers instanceof Vn?c=r.headers:c=new Vn(r.headers);let l;r.params&&(r.params instanceof Ln?l=r.params:l=new Ln({fromObject:r.params})),o=new Hr(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=lt(o).pipe(dd(c=>this.handler.handle(c)));if(e instanceof Hr||r.observe==="events")return s;let a=s.pipe(Mt(c=>c instanceof Xo));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new _(2806,!1);return c.body}));case"blob":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new _(2807,!1);return c.body}));case"text":return a.pipe(ne(c=>{if(c.body!==null&&typeof c.body!="string")throw new _(2808,!1);return c.body}));default:return a.pipe(ne(c=>c.body))}case"response":return a;default:throw new _(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Ln().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,rm(r,i))}post(e,i,r={}){return this.request("POST",e,rm(r,i))}put(e,i,r={}){return this.request("PUT",e,rm(r,i))}static \u0275fac=function(i){return new(i||n)(M(cm))};static \u0275prov=P({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var tM=new w("",{factory:()=>!0}),nM="XSRF-TOKEN",iM=new w("",{factory:()=>nM}),rM="X-XSRF-TOKEN",oM=new w("",{factory:()=>rM}),sM=(()=>{class n{cookieName=m(iM);doc=m(Z);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Zo(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})(),Eb=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=M(sM),r},providedIn:"root"})}return n})();function aM(n,t){if(!m(tM)||n.method==="GET"||n.method==="HEAD")return t(n);try{let r=m(Sc).href,{origin:o}=new URL(r),{origin:s}=new URL(n.url,o);if(o!==s)return t(n)}catch(r){return t(n)}let e=m(Eb).getToken(),i=m(oM);return e!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,e)})),t(n)}function lm(...n){let t=[zr,kc,Rc,{provide:cm,useExisting:Rc},{provide:am,useFactory:()=>m(kc)},{provide:yb,useValue:aM,multi:!0}];for(let e of n)t.push(...e.\u0275providers);return ti(t)}var it=(function(n){return n[n.Format=0]="Format",n[n.Standalone=1]="Standalone",n})(it||{}),fe=(function(n){return n[n.Narrow=0]="Narrow",n[n.Abbreviated=1]="Abbreviated",n[n.Wide=2]="Wide",n[n.Short=3]="Short",n})(fe||{}),gt=(function(n){return n[n.Short=0]="Short",n[n.Medium=1]="Medium",n[n.Long=2]="Long",n[n.Full=3]="Full",n})(gt||{}),Bn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function xb(n){return Dt(n)[Fe.LocaleId]}function Sb(n,t,e){let i=Dt(n),r=[i[Fe.DayPeriodsFormat],i[Fe.DayPeriodsStandalone]],o=Vt(r,t);return Vt(o,e)}function Mb(n,t,e){let i=Dt(n),r=[i[Fe.DaysFormat],i[Fe.DaysStandalone]],o=Vt(r,t);return Vt(o,e)}function Tb(n,t,e){let i=Dt(n),r=[i[Fe.MonthsFormat],i[Fe.MonthsStandalone]],o=Vt(r,t);return Vt(o,e)}function Ab(n,t){let i=Dt(n)[Fe.Eras];return Vt(i,t)}function Jo(n,t){let e=Dt(n);return Vt(e[Fe.DateFormat],t)}function es(n,t){let e=Dt(n);return Vt(e[Fe.TimeFormat],t)}function ts(n,t){let i=Dt(n)[Fe.DateTimeFormat];return Vt(i,t)}function ns(n,t){let e=Dt(n),i=e[Fe.NumberSymbols][t];if(typeof i>"u"){if(t===Bn.CurrencyDecimal)return e[Fe.NumberSymbols][Bn.Decimal];if(t===Bn.CurrencyGroup)return e[Fe.NumberSymbols][Bn.Group]}return i}function Nb(n){if(!n[Fe.ExtraData])throw new _(2303,!1)}function kb(n){let t=Dt(n);return Nb(t),(t[Fe.ExtraData][2]||[]).map(i=>typeof i=="string"?dm(i):[dm(i[0]),dm(i[1])])}function Rb(n,t,e){let i=Dt(n);Nb(i);let r=[i[Fe.ExtraData][0],i[Fe.ExtraData][1]],o=Vt(r,t)||[];return Vt(o,e)||[]}function Vt(n,t){for(let e=t;e>-1;e--)if(typeof n[e]<"u")return n[e];throw new _(2304,!1)}function dm(n){let[t,e]=n.split(":");return{hours:+t,minutes:+e}}var cM=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Fc={},lM=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,dM=256;function Fb(n,t,e,i){let r=_M(n);uM(t),t=jn(e,t)||t;let s=[],a;for(;t;)if(a=lM.exec(t),a){s=s.concat(a.slice(1));let d=s.pop();if(!d)break;t=d}else{s.push(t);break}let c=r.getTimezoneOffset();i&&(c=Pb(i,c),r=bM(r,i));let l="";return s.forEach(d=>{let u=vM(d);l+=u?u(r,e,c):d==="''"?"'":d.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function uM(n){if(n.length>dM)throw new _(2300,!1)}function jc(n,t,e){let i=new Date(0);return i.setFullYear(n,t,e),i.setHours(0,0,0),i}function jn(n,t){let e=xb(n);if(Fc[e]??={},Fc[e][t])return Fc[e][t];let i="";switch(t){case"shortDate":i=Jo(n,gt.Short);break;case"mediumDate":i=Jo(n,gt.Medium);break;case"longDate":i=Jo(n,gt.Long);break;case"fullDate":i=Jo(n,gt.Full);break;case"shortTime":i=es(n,gt.Short);break;case"mediumTime":i=es(n,gt.Medium);break;case"longTime":i=es(n,gt.Long);break;case"fullTime":i=es(n,gt.Full);break;case"short":let r=jn(n,"shortTime"),o=jn(n,"shortDate");i=Oc(ts(n,gt.Short),[r,o]);break;case"medium":let s=jn(n,"mediumTime"),a=jn(n,"mediumDate");i=Oc(ts(n,gt.Medium),[s,a]);break;case"long":let c=jn(n,"longTime"),l=jn(n,"longDate");i=Oc(ts(n,gt.Long),[c,l]);break;case"full":let d=jn(n,"fullTime"),u=jn(n,"fullDate");i=Oc(ts(n,gt.Full),[d,u]);break}return i&&(Fc[e][t]=i),i}function Oc(n,t){return t&&(n=n.replace(/\{([^}]+)}/g,function(e,i){return Object.hasOwn(t,i)?t[i]:e})),n}function Zt(n,t,e="-",i,r){let o="";(n<0||r&&n<=0)&&(r?n=-n+1:(n=-n,o=e));let s=String(n);for(;s.length<t;)s="0"+s;return i&&(s=s.slice(s.length-t)),o+s}function fM(n,t){return Zt(n,3).substring(0,t)}function Oe(n,t,e=0,i=!1,r=!1){return function(o,s){let a=mM(n,o);if((e>0||a>-e)&&(a+=e),n===3)a===0&&e===-12&&(a=12);else if(n===6)return fM(a,t);let c=ns(s,Bn.MinusSign);return Zt(a,t,c,i,r)}}function mM(n,t){switch(n){case 0:return t.getFullYear();case 1:return t.getMonth();case 2:return t.getDate();case 3:return t.getHours();case 4:return t.getMinutes();case 5:return t.getSeconds();case 6:return t.getMilliseconds();case 7:return t.getDay();default:throw new _(2301,!1)}}function ge(n,t,e=it.Format,i=!1){return function(r,o){return pM(r,o,n,t,e,i)}}function pM(n,t,e,i,r,o){switch(e){case 2:return Tb(t,r,i)[n.getMonth()];case 1:return Mb(t,r,i)[n.getDay()];case 0:let s=n.getHours(),a=n.getMinutes();if(o){let l=kb(t),d=Rb(t,r,i),u=l.findIndex(h=>{if(Array.isArray(h)){let[f,v]=h,E=s>=f.hours&&a>=f.minutes,D=s<v.hours||s===v.hours&&a<v.minutes;if(f.hours<v.hours){if(E&&D)return!0}else if(E||D)return!0}else if(h.hours===s&&h.minutes===a)return!0;return!1});if(u!==-1)return d[u]}return Sb(t,r,i)[s<12?0:1];case 3:return Ab(t,i)[n.getFullYear()<=0?0:1];default:let c=e;throw new _(2302,!1)}}function Pc(n){return function(t,e,i){let r=-1*i,o=ns(e,Bn.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(n){case 0:return(r>=0?"+":"")+Zt(s,2,o)+Zt(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+Zt(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+Zt(s,2,o)+":"+Zt(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+Zt(s,2,o)+":"+Zt(Math.abs(r%60),2,o);default:throw new _(2310,!1)}}}var hM=0,Vc=4;function gM(n){let t=jc(n,hM,1).getDay();return jc(n,0,1+(t<=Vc?Vc:Vc+7)-t)}function Ob(n){let t=n.getDay(),e=t===0?-3:Vc-t;return jc(n.getFullYear(),n.getMonth(),n.getDate()+e)}function um(n,t=!1){return function(e,i){let r;if(t){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,s=e.getDate();r=1+Math.floor((s+o)/7)}else{let o=Ob(e),s=gM(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return Zt(r,n,ns(i,Bn.MinusSign))}}function Lc(n,t=!1){return function(e,i){let o=Ob(e).getFullYear();return Zt(o,n,ns(i,Bn.MinusSign),t)}}var fm={};function vM(n){if(fm[n])return fm[n];let t;switch(n){case"G":case"GG":case"GGG":t=ge(3,fe.Abbreviated);break;case"GGGG":t=ge(3,fe.Wide);break;case"GGGGG":t=ge(3,fe.Narrow);break;case"y":t=Oe(0,1,0,!1,!0);break;case"yy":t=Oe(0,2,0,!0,!0);break;case"yyy":t=Oe(0,3,0,!1,!0);break;case"yyyy":t=Oe(0,4,0,!1,!0);break;case"Y":t=Lc(1);break;case"YY":t=Lc(2,!0);break;case"YYY":t=Lc(3);break;case"YYYY":t=Lc(4);break;case"M":case"L":t=Oe(1,1,1);break;case"MM":case"LL":t=Oe(1,2,1);break;case"MMM":t=ge(2,fe.Abbreviated);break;case"MMMM":t=ge(2,fe.Wide);break;case"MMMMM":t=ge(2,fe.Narrow);break;case"LLL":t=ge(2,fe.Abbreviated,it.Standalone);break;case"LLLL":t=ge(2,fe.Wide,it.Standalone);break;case"LLLLL":t=ge(2,fe.Narrow,it.Standalone);break;case"w":t=um(1);break;case"ww":t=um(2);break;case"W":t=um(1,!0);break;case"d":t=Oe(2,1);break;case"dd":t=Oe(2,2);break;case"c":case"cc":t=Oe(7,1);break;case"ccc":t=ge(1,fe.Abbreviated,it.Standalone);break;case"cccc":t=ge(1,fe.Wide,it.Standalone);break;case"ccccc":t=ge(1,fe.Narrow,it.Standalone);break;case"cccccc":t=ge(1,fe.Short,it.Standalone);break;case"E":case"EE":case"EEE":t=ge(1,fe.Abbreviated);break;case"EEEE":t=ge(1,fe.Wide);break;case"EEEEE":t=ge(1,fe.Narrow);break;case"EEEEEE":t=ge(1,fe.Short);break;case"a":case"aa":case"aaa":t=ge(0,fe.Abbreviated);break;case"aaaa":t=ge(0,fe.Wide);break;case"aaaaa":t=ge(0,fe.Narrow);break;case"b":case"bb":case"bbb":t=ge(0,fe.Abbreviated,it.Standalone,!0);break;case"bbbb":t=ge(0,fe.Wide,it.Standalone,!0);break;case"bbbbb":t=ge(0,fe.Narrow,it.Standalone,!0);break;case"B":case"BB":case"BBB":t=ge(0,fe.Abbreviated,it.Format,!0);break;case"BBBB":t=ge(0,fe.Wide,it.Format,!0);break;case"BBBBB":t=ge(0,fe.Narrow,it.Format,!0);break;case"h":t=Oe(3,1,-12);break;case"hh":t=Oe(3,2,-12);break;case"H":t=Oe(3,1);break;case"HH":t=Oe(3,2);break;case"m":t=Oe(4,1);break;case"mm":t=Oe(4,2);break;case"s":t=Oe(5,1);break;case"ss":t=Oe(5,2);break;case"S":t=Oe(6,1);break;case"SS":t=Oe(6,2);break;case"SSS":t=Oe(6,3);break;case"Z":case"ZZ":case"ZZZ":t=Pc(0);break;case"ZZZZZ":t=Pc(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":t=Pc(1);break;case"OOOO":case"ZZZZ":case"zzzz":t=Pc(2);break;default:return null}return fm[n]=t,t}function Pb(n,t){n=n.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+n)/6e4;return isNaN(e)?t:e}function yM(n,t){return n=new Date(n.getTime()),n.setMinutes(n.getMinutes()+t),n}function bM(n,t,e){let r=n.getTimezoneOffset(),o=Pb(t,r);return yM(n,-1*(o-r))}function _M(n){if(Db(n))return n;if(typeof n=="number"&&!isNaN(n))return new Date(n);if(typeof n=="string"){if(n=n.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(n)){let[r,o=1,s=1]=n.split("-").map(a=>+a);return jc(r,o-1,s)}let e=parseFloat(n);if(!isNaN(n-e))return new Date(e);let i;if(i=n.match(cM))return EM(i)}let t=new Date(n);if(!Db(t))throw new _(2311,!1);return t}function EM(n){let t=new Date(0),e=0,i=0,r=n[8]?t.setUTCFullYear:t.setFullYear,o=n[8]?t.setUTCHours:t.setHours;n[9]&&(e=Number(n[9]+n[10]),i=Number(n[9]+n[11])),r.call(t,Number(n[1]),Number(n[2])-1,Number(n[3]));let s=Number(n[4]||0)-e,a=Number(n[5]||0)-i,c=Number(n[6]||0),l=Math.floor(parseFloat("0."+(n[7]||0))*1e3);return o.call(t,s,a,c,l),t}function Db(n){return n instanceof Date&&!isNaN(n.valueOf())}var Bc=class{$implicit;ngForOf;index;count;constructor(t,e,i,r){this.$implicit=t,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Uc=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Bc(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),Cb(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);Cb(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||n)($(ui),$(li),$(tm))};static \u0275dir=G({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function Cb(n,t){n.context.$implicit=t.item}var mm=(()=>{class n{_viewContainer;_context=new Hc;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){Ib(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){Ib(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||n)($(ui),$(li))};static \u0275dir=G({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),Hc=class{$implicit=null;ngIf=null};function Ib(n,t){if(n&&!n.createEmbeddedView)throw new _(2020,!1)}var pm=(()=>{class n{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(Ie);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||n)($(ui))};static \u0275dir=G({type:n,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ft]})}return n})();function wM(n,t){return new _(2100,!1)}var DM="mediumDate",Lb=new w(""),Vb=new w(""),hm=(()=>{class n{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let s=i??this.defaultOptions?.dateFormat??DM,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return Fb(e,s,o||this.locale,a)}catch(s){throw wM(n,s.message)}}static \u0275fac=function(i){return new(i||n)($(qo,16),$(Lb,24),$(Vb,24))};static \u0275pipe=Vf({name:"date",type:n,pure:!0})}return n})();var $c=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({})}return n})();var gm="browser";function jb(n){return n===gm}var is=class{_doc;constructor(t){this._doc=t}manager},zc=(()=>{class n extends is{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||n)(M(Z))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),qc=new w(""),_m=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof zc));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof zc);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new _(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||n)(M(qc),M(O))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),vm="ng-app-id";function Bb(n){for(let t of n)t.remove()}function Hb(n,t){let e=t.createElement("style");return e.textContent=n,e}function IM(n,t,e,i){let r=n.head?.querySelectorAll(`style[${vm}="${t}"],link[${vm}="${t}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(vm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function bm(n,t){let e=t.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",n),e}var Em=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,IM(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,Hb);i?.forEach(r=>this.addUsage(r,this.external,bm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(Bb(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])Bb(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,Hb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,bm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||n)(M(Z),M(ji),M(Hi,8),M(Bi))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),ym={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},wm=/%COMP%/g;var $b="%COMP%",xM=`_nghost-${$b}`,SM=`_ngcontent-${$b}`,MM=!0,TM=new w("",{factory:()=>MM});function AM(n){return SM.replace(wm,n)}function NM(n){return xM.replace(wm,n)}function zb(n,t){return t.map(e=>e.replace(wm,n))}var ss=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new rs(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Wc?r.applyToHost(e):r instanceof os&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case Kt.Emulated:o=new Wc(c,l,i,this.appId,d,s,a,u);break;case Kt.ShadowDom:return new Gc(c,e,i,s,a,this.nonce,u,l);case Kt.ExperimentalIsolatedShadowDom:return new Gc(c,e,i,s,a,this.nonce,u);default:o=new os(c,l,i,d,s,a,u);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||n)(M(_m),M(Qi),M(ji),M(TM),M(Z),M(O),M(Hi),M(On,8))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),rs=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,e,i,r){this.eventManager=t,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(t,e){return e?this.doc.createElementNS(ym[e]||e,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,e){(Ub(t)?t.content:t).appendChild(e)}insertBefore(t,e,i){t&&(Ub(t)?t.content:t).insertBefore(e,i)}removeChild(t,e){e.remove()}selectRootElement(t,e){let i=typeof t=="string"?this.doc.querySelector(t):t;if(!i)throw new _(-5104,!1);return e||(i.textContent=""),i}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,e,i,r){if(r){e=r+":"+e;let o=ym[r];o?t.setAttributeNS(o,e,i):t.setAttribute(e,i)}else t.setAttribute(e,i)}removeAttribute(t,e,i){if(i){let r=ym[i];r?t.removeAttributeNS(r,e):t.removeAttribute(`${i}:${e}`)}else t.removeAttribute(e)}addClass(t,e){t.classList.add(e)}removeClass(t,e){t.classList.remove(e)}setStyle(t,e,i,r){r&(pn.DashCase|pn.Important)?t.style.setProperty(e,i,r&pn.Important?"important":""):t.style[e]=i}removeStyle(t,e,i){i&pn.DashCase?t.style.removeProperty(e):t.style[e]=""}setProperty(t,e,i){t!=null&&(t[e]=i)}setValue(t,e){t.nodeValue=e}listen(t,e,i,r){if(typeof t=="string"&&(t=Lt().getGlobalEventTarget(this.doc,t),!t))throw new _(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(t,e,o)),this.eventManager.addEventListener(t,e,o,r)}decoratePreventDefault(t){return e=>{if(e==="__ngUnwrap__")return t;t(e)===!1&&e.preventDefault()}}};function Ub(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Gc=class extends rs{hostEl;sharedStylesHost;shadowRoot;constructor(t,e,i,r,o,s,a,c){super(t,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=zb(i.id,l);for(let u of l){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=u,this.shadowRoot.appendChild(h)}let d=i.getExternalStyles?.();if(d)for(let u of d){let h=bm(u,r);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,e){return super.appendChild(this.nodeOrShadowRoot(t),e)}insertBefore(t,e,i){return super.insertBefore(this.nodeOrShadowRoot(t),e,i)}removeChild(t,e){return super.removeChild(null,e)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},os=class extends rs{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,e,i,r,o,s,a,c){super(t,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?zb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&zi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Wc=class extends os{contentAttr;hostAttr;constructor(t,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(t,e,i,o,s,a,c,l),this.contentAttr=AM(l),this.hostAttr=NM(l)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,e){let i=super.createElement(t,e);return super.setAttribute(i,this.contentAttr,""),i}};var Kc=class n extends Yo{supportsDOMEvents=!0;static makeCurrent(){nm(new n)}onAndCancel(t,e,i,r){return t.addEventListener(e,i,r),()=>{t.removeEventListener(e,i,r)}}dispatchEvent(t,e){t.dispatchEvent(e)}remove(t){t.remove()}createElement(t,e){return e=e||this.getDefaultDocument(),e.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,e){return e==="window"?window:e==="document"?t:e==="body"?t.body:null}getBaseHref(t){let e=kM();return e==null?null:RM(e)}resetBaseElement(){as=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return Zo(document.cookie,t)}},as=null;function kM(){return as=as||document.head.querySelector("base"),as?as.getAttribute("href"):null}function RM(n){return new URL(n,document.baseURI).pathname}var Gb=["alt","control","meta","shift"],FM={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},OM={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Wb=(()=>{class n extends is{constructor(e){super(e)}supports(e){return n.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=n.parseEventName(i),a=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Lt().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=n._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),Gb.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=FM[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Gb.forEach(s=>{if(s!==r){let a=OM[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{n.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||n)(M(Z))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})();function Dm(n,t,e){return bn(this,null,function*(){let i=I({rootComponent:n},PM(t,e));return nb(i)})}function PM(n,t){return{platformRef:t?.platformRef,appProviders:[...HM,...n?.providers??[]],platformProviders:BM}}function LM(){Kc.makeCurrent()}function VM(){return new dt}function jM(){return sf(document),document}var BM=[{provide:Bi,useValue:gm},{provide:Va,useValue:LM,multi:!0},{provide:Z,useFactory:jM}];var HM=[{provide:Do,useValue:"root"},{provide:dt,useFactory:VM},{provide:qc,useClass:zc,multi:!0},{provide:qc,useClass:Wb,multi:!0},ss,{provide:Qi,useClass:Em},{provide:Em,useExisting:Qi},_m,{provide:at,useExisting:ss},[]];var Cm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=M(UM),r},providedIn:"root"})}return n})(),UM=(()=>{class n extends Cm{_doc=m(Z);sanitize(e,i){if(i==null)return null;switch(e){case ct.NONE:return i;case ct.HTML:return Ki(i,"HTML")?Rn(i):ff(this._doc,String(i)).toString();case ct.STYLE:return Ki(i,"Style")?Rn(i):i;case ct.SCRIPT:if(Ki(i,"Script"))return Rn(i);throw new _(5200,!1);case ct.URL:return Ki(i,"URL")?Rn(i):fc(String(i));case ct.RESOURCE_URL:if(Ki(i,"ResourceURL"))return Rn(i);throw new _(5201,!1);default:throw new _(5202,!1)}}bypassSecurityTrustHtml(e){return af(e)}bypassSecurityTrustStyle(e){return cf(e)}bypassSecurityTrustScript(e){return lf(e)}bypassSecurityTrustUrl(e){return df(e)}bypassSecurityTrustResourceUrl(e){return uf(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var U=(function(n){return n[n.State=0]="State",n[n.Transition=1]="Transition",n[n.Sequence=2]="Sequence",n[n.Group=3]="Group",n[n.Animate=4]="Animate",n[n.Keyframes=5]="Keyframes",n[n.Style=6]="Style",n[n.Trigger=7]="Trigger",n[n.Reference=8]="Reference",n[n.AnimateChild=9]="AnimateChild",n[n.AnimateRef=10]="AnimateRef",n[n.Query=11]="Query",n[n.Stagger=12]="Stagger",n})(U||{}),Yt="*";function qb(n,t=null){return{type:U.Sequence,steps:n,options:t}}function Im(n){return{type:U.Style,styles:n,offset:null}}var Hn=class{_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_originalOnDoneFns=[];_originalOnStartFns=[];_started=!1;_destroyed=!1;_finished=!1;_position=0;parentPlayer=null;totalTime;constructor(t=0,e=0){this.totalTime=t+e}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}init(){}play(){this.hasStarted()||(this._onStart(),this.triggerMicrotask()),this._started=!0}triggerMicrotask(){queueMicrotask(()=>this._onFinish())}_onStart(){this._onStartFns.forEach(t=>t()),this._onStartFns=[]}pause(){}restart(){}finish(){this._onFinish()}destroy(){this._destroyed||(this._destroyed=!0,this.hasStarted()||this._onStart(),this.finish(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this._started=!1,this._finished=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}setPosition(t){this._position=this.totalTime?t*this.totalTime:1}getPosition(){return this.totalTime?this._position/this.totalTime:1}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Gr=class{_onDoneFns=[];_onStartFns=[];_finished=!1;_started=!1;_destroyed=!1;_onDestroyFns=[];parentPlayer=null;totalTime=0;players;constructor(t){this.players=t;let e=0,i=0,r=0,o=this.players.length;o==0?queueMicrotask(()=>this._onFinish()):this.players.forEach(s=>{s.onDone(()=>{++e==o&&this._onFinish()}),s.onDestroy(()=>{++i==o&&this._onDestroy()}),s.onStart(()=>{++r==o&&this._onStart()})}),this.totalTime=this.players.reduce((s,a)=>Math.max(s,a.totalTime),0)}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this.players.forEach(t=>t.init())}onStart(t){this._onStartFns.push(t)}_onStart(){this.hasStarted()||(this._started=!0,this._onStartFns.forEach(t=>t()),this._onStartFns=[])}onDone(t){this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}play(){this.parentPlayer||this.init(),this._onStart(),this.players.forEach(t=>t.play())}pause(){this.players.forEach(t=>t.pause())}restart(){this.players.forEach(t=>t.restart())}finish(){this._onFinish(),this.players.forEach(t=>t.finish())}destroy(){this._onDestroy()}_onDestroy(){this._destroyed||(this._destroyed=!0,this._onFinish(),this.players.forEach(t=>t.destroy()),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this.players.forEach(t=>t.reset()),this._destroyed=!1,this._finished=!1,this._started=!1}setPosition(t){let e=t*this.totalTime;this.players.forEach(i=>{let r=i.totalTime?Math.min(1,e/i.totalTime):1;i.setPosition(r)})}getPosition(){let t=this.players.reduce((e,i)=>e===null||i.totalTime>e.totalTime?i:e,null);return t!=null?t.getPosition():0}beforeDestroy(){this.players.forEach(t=>{t.beforeDestroy&&t.beforeDestroy()})}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},cs="!";function Kb(n){return new _(3e3,!1)}function zM(){return new _(3100,!1)}function GM(){return new _(3101,!1)}function WM(n){return new _(3001,!1)}function qM(n){return new _(3003,!1)}function KM(n){return new _(3004,!1)}function Zb(n,t){return new _(3005,!1)}function Yb(){return new _(3006,!1)}function Xb(){return new _(3007,!1)}function Jb(n,t){return new _(3008,!1)}function e_(n){return new _(3002,!1)}function t_(n,t,e,i,r){return new _(3010,!1)}function n_(){return new _(3011,!1)}function i_(){return new _(3012,!1)}function r_(){return new _(3200,!1)}function o_(){return new _(3202,!1)}function s_(){return new _(3013,!1)}function a_(n){return new _(3014,!1)}function c_(n){return new _(3015,!1)}function l_(n){return new _(3016,!1)}function d_(n,t){return new _(3404,!1)}function QM(n){return new _(3502,!1)}function u_(n){return new _(3503,!1)}function f_(){return new _(3300,!1)}function m_(n){return new _(3504,!1)}function p_(n){return new _(3301,!1)}function h_(n,t){return new _(3302,!1)}function g_(n){return new _(3303,!1)}function v_(n,t){return new _(3400,!1)}function y_(n){return new _(3401,!1)}function b_(n){return new _(3402,!1)}function __(n,t){return new _(3505,!1)}function Un(n){switch(n.length){case 0:return new Hn;case 1:return n[0];default:return new Gr(n)}}function Tm(n,t,e=new Map,i=new Map){let r=[],o=[],s=-1,a=null;if(t.forEach(c=>{let l=c.get("offset"),d=l==s,u=d&&a||new Map;c.forEach((h,f)=>{let v=f,E=h;if(f!=="offset")switch(v=n.normalizePropertyName(v,r),E){case cs:E=e.get(f);break;case Yt:E=i.get(f);break;default:E=n.normalizeStyleValue(f,v,E,r);break}u.set(v,E)}),d||o.push(u),a=u,s=l}),r.length)throw QM(r);return o}function Qc(n,t,e,i){switch(t){case"start":n.onStart(()=>i(e&&xm(e,"start",n)));break;case"done":n.onDone(()=>i(e&&xm(e,"done",n)));break;case"destroy":n.onDestroy(()=>i(e&&xm(e,"destroy",n)));break}}function xm(n,t,e){let i=e.totalTime,r=!!e.disabled,o=Zc(n.element,n.triggerName,n.fromState,n.toState,t||n.phaseName,i??n.totalTime,r),s=n._data;return s!=null&&(o._data=s),o}function Zc(n,t,e,i,r="",o=0,s){return{element:n,triggerName:t,fromState:e,toState:i,phaseName:r,totalTime:o,disabled:!!s}}function vt(n,t,e){let i=n.get(t);return i||n.set(t,i=e),i}function Am(n){let t=n.indexOf(":"),e=n.substring(1,t),i=n.slice(t+1);return[e,i]}var ZM=typeof document>"u"?null:document.documentElement;function Yc(n){let t=n.parentNode||n.host||null;return t===ZM?null:t}function YM(n){return n.substring(1,6)=="ebkit"}var Yi=null,Qb=!1;function E_(n){Yi||(Yi=XM()||{},Qb=Yi.style?"WebkitAppearance"in Yi.style:!1);let t=!0;return Yi.style&&!YM(n)&&(t=n in Yi.style,!t&&Qb&&(t="Webkit"+n.charAt(0).toUpperCase()+n.slice(1)in Yi.style)),t}function XM(){return typeof document<"u"?document.body:null}function Nm(n,t){for(;t;){if(t===n)return!0;t=Yc(t)}return!1}function km(n,t,e){if(e)return Array.from(n.querySelectorAll(t));let i=n.querySelector(t);return i?[i]:[]}var JM=1e3,Rm="{{",eT="}}",Fm="ng-enter",Xc="ng-leave",ls="ng-trigger",ds=".ng-trigger",Om="ng-animating",Jc=".ng-animating";function yn(n){if(typeof n=="number")return n;let t=n.match(/^(-?[\.\d]+)(m?s)/);return!t||t.length<2?0:Sm(parseFloat(t[1]),t[2])}function Sm(n,t){return t==="s"?n*JM:n}function us(n,t,e){return n.hasOwnProperty("duration")?n:nT(n,t,e)}var tT=/^(-?[\.\d]+)(m?s)(?:\s+(-?[\.\d]+)(m?s))?(?:\s+([-a-z]+(?:\(.+?\))?))?$/i;function nT(n,t,e){let i,r=0,o="";if(typeof n=="string"){let s=n.match(tT);if(s===null)return t.push(Kb(n)),{duration:0,delay:0,easing:""};i=Sm(parseFloat(s[1]),s[2]);let a=s[3];a!=null&&(r=Sm(parseFloat(a),s[4]));let c=s[5];c&&(o=c)}else i=n;if(!e){let s=!1,a=t.length;i<0&&(t.push(zM()),s=!0),r<0&&(t.push(GM()),s=!0),s&&t.splice(a,0,Kb(n))}return{duration:i,delay:r,easing:o}}function w_(n){return n.length?n[0]instanceof Map?n:n.map(t=>new Map(Object.entries(t))):[]}function Xt(n,t,e){t.forEach((i,r)=>{let o=el(r);e&&!e.has(r)&&e.set(r,n.style[o]),n.style[o]=i})}function fi(n,t){t.forEach((e,i)=>{let r=el(i);n.style[r]=""})}function Wr(n){return Array.isArray(n)?n.length==1?n[0]:qb(n):n}function D_(n,t,e){let i=t.params||{},r=Pm(n);r.length&&r.forEach(o=>{i.hasOwnProperty(o)||e.push(WM(o))})}var Mm=new RegExp(`${Rm}\\s*(.+?)\\s*${eT}`,"g");function Pm(n){let t=[];if(typeof n=="string"){let e;for(;e=Mm.exec(n);)t.push(e[1]);Mm.lastIndex=0}return t}function qr(n,t,e){let i=`${n}`,r=i.replace(Mm,(o,s)=>{let a=t[s];return a==null&&(e.push(qM(s)),a=""),a.toString()});return r==i?n:r}var iT=/-+([a-z0-9])/g;function el(n){return n.replace(iT,(...t)=>t[1].toUpperCase())}function C_(n,t){return n===0||t===0}function I_(n,t,e){if(e.size&&t.length){let i=t[0],r=[];if(e.forEach((o,s)=>{i.has(s)||r.push(s),i.set(s,o)}),r.length)for(let o=1;o<t.length;o++){let s=t[o];r.forEach(a=>s.set(a,tl(n,a)))}}return t}function yt(n,t,e){switch(t.type){case U.Trigger:return n.visitTrigger(t,e);case U.State:return n.visitState(t,e);case U.Transition:return n.visitTransition(t,e);case U.Sequence:return n.visitSequence(t,e);case U.Group:return n.visitGroup(t,e);case U.Animate:return n.visitAnimate(t,e);case U.Keyframes:return n.visitKeyframes(t,e);case U.Style:return n.visitStyle(t,e);case U.Reference:return n.visitReference(t,e);case U.AnimateChild:return n.visitAnimateChild(t,e);case U.AnimateRef:return n.visitAnimateRef(t,e);case U.Query:return n.visitQuery(t,e);case U.Stagger:return n.visitStagger(t,e);default:throw KM(t.type)}}function tl(n,t){return window.getComputedStyle(n)[t]}var Jm=(()=>{class n{validateStyleProperty(e){return E_(e)}containsElement(e,i){return Nm(e,i)}getParentElement(e){return Yc(e)}query(e,i,r){return km(e,i,r)}computeStyle(e,i,r){return r||""}animate(e,i,r,o,s,a=[],c){return new Hn(r,o)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),Ji=class{static NOOP=new Jm},er=class{};var rT=new Set(["width","height","minWidth","minHeight","maxWidth","maxHeight","left","top","bottom","right","fontSize","outlineWidth","outlineOffset","paddingTop","paddingLeft","paddingBottom","paddingRight","marginTop","marginLeft","marginBottom","marginRight","borderRadius","borderWidth","borderTopWidth","borderLeftWidth","borderRightWidth","borderBottomWidth","textIndent","perspective"]),sl=class extends er{normalizePropertyName(t,e){return el(t)}normalizeStyleValue(t,e,i,r){let o="",s=i.toString().trim();if(rT.has(e)&&i!==0&&i!=="0")if(typeof i=="number")o="px";else{let a=i.match(/^[+-]?[\d\.]+([a-z]*)$/);a&&a[1].length==0&&r.push(Zb(t,i))}return s+o}};var al="*";function oT(n,t){let e=[];return typeof n=="string"?n.split(/\s*,\s*/).forEach(i=>sT(i,e,t)):e.push(n),e}function sT(n,t,e){if(n[0]==":"){let c=aT(n,e);if(typeof c=="function"){t.push(c);return}n=c}let i=n.match(/^(\*|[-\w]+)\s*(<?[=-]>)\s*(\*|[-\w]+)$/);if(i==null||i.length<4)return e.push(c_(n)),t;let r=i[1],o=i[2],s=i[3];t.push(x_(r,s));let a=r==al&&s==al;o[0]=="<"&&!a&&t.push(x_(s,r))}function aT(n,t){switch(n){case":enter":return"void => *";case":leave":return"* => void";case":increment":return(e,i)=>parseFloat(i)>parseFloat(e);case":decrement":return(e,i)=>parseFloat(i)<parseFloat(e);default:return t.push(l_(n)),"* => *"}}var nl=new Set(["true","1"]),il=new Set(["false","0"]);function x_(n,t){let e=nl.has(n)||il.has(n),i=nl.has(t)||il.has(t);return(r,o)=>{let s=n==al||n==r,a=t==al||t==o;return!s&&e&&typeof r=="boolean"&&(s=r?nl.has(n):il.has(n)),!a&&i&&typeof o=="boolean"&&(a=o?nl.has(t):il.has(t)),s&&a}}var P_=":self",cT=new RegExp(`s*${P_}s*,?`,"g");function L_(n,t,e,i){return new Um(n).build(t,e,i)}var S_="",Um=class{_driver;constructor(t){this._driver=t}build(t,e,i){let r=new $m(e);return this._resetContextStyleTimingState(r),yt(this,Wr(t),r)}_resetContextStyleTimingState(t){t.currentQuerySelector=S_,t.collectedStyles=new Map,t.collectedStyles.set(S_,new Map),t.currentTime=0}visitTrigger(t,e){let i=e.queryCount=0,r=e.depCount=0,o=[],s=[];return t.name.charAt(0)=="@"&&e.errors.push(Yb()),t.definitions.forEach(a=>{if(this._resetContextStyleTimingState(e),a.type==U.State){let c=a,l=c.name;l.toString().split(/\s*,\s*/).forEach(d=>{c.name=d,o.push(this.visitState(c,e))}),c.name=l}else if(a.type==U.Transition){let c=this.visitTransition(a,e);i+=c.queryCount,r+=c.depCount,s.push(c)}else e.errors.push(Xb())}),{type:U.Trigger,name:t.name,states:o,transitions:s,queryCount:i,depCount:r,options:null}}visitState(t,e){let i=this.visitStyle(t.styles,e),r=t.options&&t.options.params||null;if(i.containsDynamicStyles){let o=new Set,s=r||{};i.styles.forEach(a=>{a instanceof Map&&a.forEach(c=>{Pm(c).forEach(l=>{s.hasOwnProperty(l)||o.add(l)})})}),o.size&&e.errors.push(Jb(t.name,[...o.values()]))}return{type:U.State,name:t.name,style:i,options:r?{params:r}:null}}visitTransition(t,e){e.queryCount=0,e.depCount=0;let i=yt(this,Wr(t.animation),e),r=oT(t.expr,e.errors);return{type:U.Transition,matchers:r,animation:i,queryCount:e.queryCount,depCount:e.depCount,options:Xi(t.options)}}visitSequence(t,e){return{type:U.Sequence,steps:t.steps.map(i=>yt(this,i,e)),options:Xi(t.options)}}visitGroup(t,e){let i=e.currentTime,r=0,o=t.steps.map(s=>{e.currentTime=i;let a=yt(this,s,e);return r=Math.max(r,e.currentTime),a});return e.currentTime=r,{type:U.Group,steps:o,options:Xi(t.options)}}visitAnimate(t,e){let i=fT(t.timings,e.errors);e.currentAnimateTimings=i;let r,o=t.styles?t.styles:Im({});if(o.type==U.Keyframes)r=this.visitKeyframes(o,e);else{let s=t.styles,a=!1;if(!s){a=!0;let l={};i.easing&&(l.easing=i.easing),s=Im(l)}e.currentTime+=i.duration+i.delay;let c=this.visitStyle(s,e);c.isEmptyStep=a,r=c}return e.currentAnimateTimings=null,{type:U.Animate,timings:i,style:r,options:null}}visitStyle(t,e){let i=this._makeStyleAst(t,e);return this._validateStyleAst(i,e),i}_makeStyleAst(t,e){let i=[],r=Array.isArray(t.styles)?t.styles:[t.styles];for(let a of r)typeof a=="string"?a===Yt?i.push(a):e.errors.push(e_(a)):i.push(new Map(Object.entries(a)));let o=!1,s=null;return i.forEach(a=>{if(a instanceof Map&&(a.has("easing")&&(s=a.get("easing"),a.delete("easing")),!o)){for(let c of a.values())if(c.toString().indexOf(Rm)>=0){o=!0;break}}}),{type:U.Style,styles:i,easing:s,offset:t.offset,containsDynamicStyles:o,options:null}}_validateStyleAst(t,e){let i=e.currentAnimateTimings,r=e.currentTime,o=e.currentTime;i&&o>0&&(o-=i.duration+i.delay),t.styles.forEach(s=>{typeof s!="string"&&s.forEach((a,c)=>{let l=e.collectedStyles.get(e.currentQuerySelector),d=l.get(c),u=!0;d&&(o!=r&&o>=d.startTime&&r<=d.endTime&&(e.errors.push(t_(c,d.startTime,d.endTime,o,r)),u=!1),o=d.startTime),u&&l.set(c,{startTime:o,endTime:r}),e.options&&D_(a,e.options,e.errors)})})}visitKeyframes(t,e){let i={type:U.Keyframes,styles:[],options:null};if(!e.currentAnimateTimings)return e.errors.push(n_()),i;let r=1,o=0,s=[],a=!1,c=!1,l=0,d=t.steps.map(x=>{let X=this._makeStyleAst(x,e),ye=X.offset!=null?X.offset:uT(X.styles),be=0;return ye!=null&&(o++,be=X.offset=ye),c=c||be<0||be>1,a=a||be<l,l=be,s.push(be),X});c&&e.errors.push(i_()),a&&e.errors.push(r_());let u=t.steps.length,h=0;o>0&&o<u?e.errors.push(o_()):o==0&&(h=r/(u-1));let f=u-1,v=e.currentTime,E=e.currentAnimateTimings,D=E.duration;return d.forEach((x,X)=>{let ye=h>0?X==f?1:h*X:s[X],be=ye*D;e.currentTime=v+E.delay+be,E.duration=be,this._validateStyleAst(x,e),x.offset=ye,i.styles.push(x)}),i}visitReference(t,e){return{type:U.Reference,animation:yt(this,Wr(t.animation),e),options:Xi(t.options)}}visitAnimateChild(t,e){return e.depCount++,{type:U.AnimateChild,options:Xi(t.options)}}visitAnimateRef(t,e){return{type:U.AnimateRef,animation:this.visitReference(t.animation,e),options:Xi(t.options)}}visitQuery(t,e){let i=e.currentQuerySelector,r=t.options||{};e.queryCount++,e.currentQuery=t;let[o,s]=lT(t.selector);e.currentQuerySelector=i.length?i+" "+o:o,vt(e.collectedStyles,e.currentQuerySelector,new Map);let a=yt(this,Wr(t.animation),e);return e.currentQuery=null,e.currentQuerySelector=i,{type:U.Query,selector:o,limit:r.limit||0,optional:!!r.optional,includeSelf:s,animation:a,originalSelector:t.selector,options:Xi(t.options)}}visitStagger(t,e){e.currentQuery||e.errors.push(s_());let i=t.timings==="full"?{duration:0,delay:0,easing:"full"}:us(t.timings,e.errors,!0);return{type:U.Stagger,animation:yt(this,Wr(t.animation),e),timings:i,options:null}}};function lT(n){let t=!!n.split(/\s*,\s*/).find(e=>e==P_);return t&&(n=n.replace(cT,"")),n=n.replace(/@\*/g,ds).replace(/@\w+/g,e=>ds+"-"+e.slice(1)).replace(/:animating/g,Jc),[n,t]}function dT(n){return n?I({},n):null}var $m=class{errors;queryCount=0;depCount=0;currentTransition=null;currentQuery=null;currentQuerySelector=null;currentAnimateTimings=null;currentTime=0;collectedStyles=new Map;options=null;unsupportedCSSPropertiesFound=new Set;constructor(t){this.errors=t}};function uT(n){if(typeof n=="string")return null;let t=null;if(Array.isArray(n))n.forEach(e=>{if(e instanceof Map&&e.has("offset")){let i=e;t=parseFloat(i.get("offset")),i.delete("offset")}});else if(n instanceof Map&&n.has("offset")){let e=n;t=parseFloat(e.get("offset")),e.delete("offset")}return t}function fT(n,t){if(n.hasOwnProperty("duration"))return n;if(typeof n=="number"){let o=us(n,t).duration;return Lm(o,0,"")}let e=n;if(e.split(/\s+/).some(o=>o.charAt(0)=="{"&&o.charAt(1)=="{")){let o=Lm(0,0,"");return o.dynamic=!0,o.strValue=e,o}let r=us(e,t);return Lm(r.duration,r.delay,r.easing)}function Xi(n){return n?(n=I({},n),n.params&&(n.params=dT(n.params))):n={},n}function Lm(n,t,e){return{duration:n,delay:t,easing:e}}function ep(n,t,e,i,r,o,s=null,a=!1){return{type:1,element:n,keyframes:t,preStyleProps:e,postStyleProps:i,duration:r,delay:o,totalTime:r+o,easing:s,subTimeline:a}}var ms=class{_map=new Map;get(t){return this._map.get(t)||[]}append(t,e){let i=this._map.get(t);i||this._map.set(t,i=[]),i.push(...e)}has(t){return this._map.has(t)}clear(){this._map.clear()}},mT=1,pT=":enter",hT=new RegExp(pT,"g"),gT=":leave",vT=new RegExp(gT,"g");function V_(n,t,e,i,r,o=new Map,s=new Map,a,c,l=[]){return new zm().buildKeyframes(n,t,e,i,r,o,s,a,c,l)}var zm=class{buildKeyframes(t,e,i,r,o,s,a,c,l,d=[]){l=l||new ms;let u=new Gm(t,e,l,r,o,d,[]);u.options=c;let h=c.delay?yn(c.delay):0;u.currentTimeline.delayNextStep(h),u.currentTimeline.setStyles([s],null,u.errors,c),yt(this,i,u);let f=u.timelines.filter(v=>v.containsAnimation());if(f.length&&a.size){let v;for(let E=f.length-1;E>=0;E--){let D=f[E];if(D.element===e){v=D;break}}v&&!v.allowOnlyTimelineStyles()&&v.setStyles([a],null,u.errors,c)}return f.length?f.map(v=>v.buildKeyframes()):[ep(e,[],[],[],0,h,"",!1)]}visitTrigger(t,e){}visitState(t,e){}visitTransition(t,e){}visitAnimateChild(t,e){let i=e.subInstructions.get(e.element);if(i){let r=e.createSubContext(t.options),o=e.currentTimeline.currentTime,s=this._visitSubInstructions(i,r,r.options);o!=s&&e.transformIntoNewTimeline(s)}e.previousNode=t}visitAnimateRef(t,e){let i=e.createSubContext(t.options);i.transformIntoNewTimeline(),this._applyAnimationRefDelays([t.options,t.animation.options],e,i),this.visitReference(t.animation,i),e.transformIntoNewTimeline(i.currentTimeline.currentTime),e.previousNode=t}_applyAnimationRefDelays(t,e,i){for(let r of t){let o=r?.delay;if(o){let s=typeof o=="number"?o:yn(qr(o,r?.params??{},e.errors));i.delayNextStep(s)}}}_visitSubInstructions(t,e,i){let o=e.currentTimeline.currentTime,s=i.duration!=null?yn(i.duration):null,a=i.delay!=null?yn(i.delay):null;return s!==0&&t.forEach(c=>{let l=e.appendInstructionToTimeline(c,s,a);o=Math.max(o,l.duration+l.delay)}),o}visitReference(t,e){e.updateOptions(t.options,!0),yt(this,t.animation,e),e.previousNode=t}visitSequence(t,e){let i=e.subContextCount,r=e,o=t.options;if(o&&(o.params||o.delay)&&(r=e.createSubContext(o),r.transformIntoNewTimeline(),o.delay!=null)){r.previousNode.type==U.Style&&(r.currentTimeline.snapshotCurrentStyles(),r.previousNode=cl);let s=yn(o.delay);r.delayNextStep(s)}t.steps.length&&(t.steps.forEach(s=>yt(this,s,r)),r.currentTimeline.applyStylesToKeyframe(),r.subContextCount>i&&r.transformIntoNewTimeline()),e.previousNode=t}visitGroup(t,e){let i=[],r=e.currentTimeline.currentTime,o=t.options&&t.options.delay?yn(t.options.delay):0;t.steps.forEach(s=>{let a=e.createSubContext(t.options);o&&a.delayNextStep(o),yt(this,s,a),r=Math.max(r,a.currentTimeline.currentTime),i.push(a.currentTimeline)}),i.forEach(s=>e.currentTimeline.mergeTimelineCollectedStyles(s)),e.transformIntoNewTimeline(r),e.previousNode=t}_visitTiming(t,e){if(t.dynamic){let i=t.strValue,r=e.params?qr(i,e.params,e.errors):i;return us(r,e.errors)}else return{duration:t.duration,delay:t.delay,easing:t.easing}}visitAnimate(t,e){let i=e.currentAnimateTimings=this._visitTiming(t.timings,e),r=e.currentTimeline;i.delay&&(e.incrementTime(i.delay),r.snapshotCurrentStyles());let o=t.style;o.type==U.Keyframes?this.visitKeyframes(o,e):(e.incrementTime(i.duration),this.visitStyle(o,e),r.applyStylesToKeyframe()),e.currentAnimateTimings=null,e.previousNode=t}visitStyle(t,e){let i=e.currentTimeline,r=e.currentAnimateTimings;!r&&i.hasCurrentStyleProperties()&&i.forwardFrame();let o=r&&r.easing||t.easing;t.isEmptyStep?i.applyEmptyStep(o):i.setStyles(t.styles,o,e.errors,e.options),e.previousNode=t}visitKeyframes(t,e){let i=e.currentAnimateTimings,r=e.currentTimeline.duration,o=i.duration,a=e.createSubContext().currentTimeline;a.easing=i.easing,t.styles.forEach(c=>{let l=c.offset||0;a.forwardTime(l*o),a.setStyles(c.styles,c.easing,e.errors,e.options),a.applyStylesToKeyframe()}),e.currentTimeline.mergeTimelineCollectedStyles(a),e.transformIntoNewTimeline(r+o),e.previousNode=t}visitQuery(t,e){let i=e.currentTimeline.currentTime,r=t.options||{},o=r.delay?yn(r.delay):0;o&&(e.previousNode.type===U.Style||i==0&&e.currentTimeline.hasCurrentStyleProperties())&&(e.currentTimeline.snapshotCurrentStyles(),e.previousNode=cl);let s=i,a=e.invokeQuery(t.selector,t.originalSelector,t.limit,t.includeSelf,!!r.optional,e.errors);e.currentQueryTotal=a.length;let c=null;a.forEach((l,d)=>{e.currentQueryIndex=d;let u=e.createSubContext(t.options,l);o&&u.delayNextStep(o),l===e.element&&(c=u.currentTimeline),yt(this,t.animation,u),u.currentTimeline.applyStylesToKeyframe();let h=u.currentTimeline.currentTime;s=Math.max(s,h)}),e.currentQueryIndex=0,e.currentQueryTotal=0,e.transformIntoNewTimeline(s),c&&(e.currentTimeline.mergeTimelineCollectedStyles(c),e.currentTimeline.snapshotCurrentStyles()),e.previousNode=t}visitStagger(t,e){let i=e.parentContext,r=e.currentTimeline,o=t.timings,s=Math.abs(o.duration),a=s*(e.currentQueryTotal-1),c=s*e.currentQueryIndex;switch(o.duration<0?"reverse":o.easing){case"reverse":c=a-c;break;case"full":c=i.currentStaggerTime;break}let d=e.currentTimeline;c&&d.delayNextStep(c);let u=d.currentTime;yt(this,t.animation,e),e.previousNode=t,i.currentStaggerTime=r.currentTime-u+(r.startTime-i.currentTimeline.startTime)}},cl={},Gm=class n{_driver;element;subInstructions;_enterClassName;_leaveClassName;errors;timelines;parentContext=null;currentTimeline;currentAnimateTimings=null;previousNode=cl;subContextCount=0;options={};currentQueryIndex=0;currentQueryTotal=0;currentStaggerTime=0;constructor(t,e,i,r,o,s,a,c){this._driver=t,this.element=e,this.subInstructions=i,this._enterClassName=r,this._leaveClassName=o,this.errors=s,this.timelines=a,this.currentTimeline=c||new ll(this._driver,e,0),a.push(this.currentTimeline)}get params(){return this.options.params}updateOptions(t,e){if(!t)return;let i=t,r=this.options;i.duration!=null&&(r.duration=yn(i.duration)),i.delay!=null&&(r.delay=yn(i.delay));let o=i.params;if(o){let s=r.params;s||(s=this.options.params={}),Object.keys(o).forEach(a=>{(!e||!s.hasOwnProperty(a))&&(s[a]=qr(o[a],s,this.errors))})}}_copyOptions(){let t={};if(this.options){let e=this.options.params;if(e){let i=t.params={};Object.keys(e).forEach(r=>{i[r]=e[r]})}}return t}createSubContext(t=null,e,i){let r=e||this.element,o=new n(this._driver,r,this.subInstructions,this._enterClassName,this._leaveClassName,this.errors,this.timelines,this.currentTimeline.fork(r,i||0));return o.previousNode=this.previousNode,o.currentAnimateTimings=this.currentAnimateTimings,o.options=this._copyOptions(),o.updateOptions(t),o.currentQueryIndex=this.currentQueryIndex,o.currentQueryTotal=this.currentQueryTotal,o.parentContext=this,this.subContextCount++,o}transformIntoNewTimeline(t){return this.previousNode=cl,this.currentTimeline=this.currentTimeline.fork(this.element,t),this.timelines.push(this.currentTimeline),this.currentTimeline}appendInstructionToTimeline(t,e,i){let r={duration:e??t.duration,delay:this.currentTimeline.currentTime+(i??0)+t.delay,easing:""},o=new Wm(this._driver,t.element,t.keyframes,t.preStyleProps,t.postStyleProps,r,t.stretchStartingKeyframe);return this.timelines.push(o),r}incrementTime(t){this.currentTimeline.forwardTime(this.currentTimeline.duration+t)}delayNextStep(t){t>0&&this.currentTimeline.delayNextStep(t)}invokeQuery(t,e,i,r,o,s){let a=[];if(r&&a.push(this.element),t.length>0){t=t.replace(hT,"."+this._enterClassName),t=t.replace(vT,"."+this._leaveClassName);let c=i!=1,l=this._driver.query(this.element,t,c);i!==0&&(l=i<0?l.slice(l.length+i,l.length):l.slice(0,i)),a.push(...l)}return!o&&a.length==0&&s.push(a_(e)),a}},ll=class n{_driver;element;startTime;_elementTimelineStylesLookup;duration=0;easing=null;_previousKeyframe=new Map;_currentKeyframe=new Map;_keyframes=new Map;_styleSummary=new Map;_localTimelineStyles=new Map;_globalTimelineStyles;_pendingStyles=new Map;_backFill=new Map;_currentEmptyStepKeyframe=null;constructor(t,e,i,r){this._driver=t,this.element=e,this.startTime=i,this._elementTimelineStylesLookup=r,this._elementTimelineStylesLookup||(this._elementTimelineStylesLookup=new Map),this._globalTimelineStyles=this._elementTimelineStylesLookup.get(e),this._globalTimelineStyles||(this._globalTimelineStyles=this._localTimelineStyles,this._elementTimelineStylesLookup.set(e,this._localTimelineStyles)),this._loadKeyframe()}containsAnimation(){switch(this._keyframes.size){case 0:return!1;case 1:return this.hasCurrentStyleProperties();default:return!0}}hasCurrentStyleProperties(){return this._currentKeyframe.size>0}get currentTime(){return this.startTime+this.duration}delayNextStep(t){let e=this._keyframes.size===1&&this._pendingStyles.size;this.duration||e?(this.forwardTime(this.currentTime+t),e&&this.snapshotCurrentStyles()):this.startTime+=t}fork(t,e){return this.applyStylesToKeyframe(),new n(this._driver,t,e||this.currentTime,this._elementTimelineStylesLookup)}_loadKeyframe(){this._currentKeyframe&&(this._previousKeyframe=this._currentKeyframe),this._currentKeyframe=this._keyframes.get(this.duration),this._currentKeyframe||(this._currentKeyframe=new Map,this._keyframes.set(this.duration,this._currentKeyframe))}forwardFrame(){this.duration+=mT,this._loadKeyframe()}forwardTime(t){this.applyStylesToKeyframe(),this.duration=t,this._loadKeyframe()}_updateStyle(t,e){this._localTimelineStyles.set(t,e),this._globalTimelineStyles.set(t,e),this._styleSummary.set(t,{time:this.currentTime,value:e})}allowOnlyTimelineStyles(){return this._currentEmptyStepKeyframe!==this._currentKeyframe}applyEmptyStep(t){t&&this._previousKeyframe.set("easing",t);for(let[e,i]of this._globalTimelineStyles)this._backFill.set(e,i||Yt),this._currentKeyframe.set(e,Yt);this._currentEmptyStepKeyframe=this._currentKeyframe}setStyles(t,e,i,r){e&&this._previousKeyframe.set("easing",e);let o=r&&r.params||{},s=yT(t,this._globalTimelineStyles);for(let[a,c]of s){let l=qr(c,o,i);this._pendingStyles.set(a,l),this._localTimelineStyles.has(a)||this._backFill.set(a,this._globalTimelineStyles.get(a)??Yt),this._updateStyle(a,l)}}applyStylesToKeyframe(){this._pendingStyles.size!=0&&(this._pendingStyles.forEach((t,e)=>{this._currentKeyframe.set(e,t)}),this._pendingStyles.clear(),this._localTimelineStyles.forEach((t,e)=>{this._currentKeyframe.has(e)||this._currentKeyframe.set(e,t)}))}snapshotCurrentStyles(){for(let[t,e]of this._localTimelineStyles)this._pendingStyles.set(t,e),this._updateStyle(t,e)}getFinalKeyframe(){return this._keyframes.get(this.duration)}get properties(){let t=[];for(let e in this._currentKeyframe)t.push(e);return t}mergeTimelineCollectedStyles(t){t._styleSummary.forEach((e,i)=>{let r=this._styleSummary.get(i);(!r||e.time>r.time)&&this._updateStyle(i,e.value)})}buildKeyframes(){this.applyStylesToKeyframe();let t=new Set,e=new Set,i=this._keyframes.size===1&&this.duration===0,r=[];this._keyframes.forEach((a,c)=>{let l=new Map([...this._backFill,...a]);l.forEach((d,u)=>{d===cs?t.add(u):d===Yt&&e.add(u)}),i||l.set("offset",c/this.duration),r.push(l)});let o=[...t.values()],s=[...e.values()];if(i){let a=r[0],c=new Map(a);a.set("offset",0),c.set("offset",1),r=[a,c]}return ep(this.element,r,o,s,this.duration,this.startTime,this.easing,!1)}},Wm=class extends ll{keyframes;preStyleProps;postStyleProps;_stretchStartingKeyframe;timings;constructor(t,e,i,r,o,s,a=!1){super(t,e,s.delay),this.keyframes=i,this.preStyleProps=r,this.postStyleProps=o,this._stretchStartingKeyframe=a,this.timings={duration:s.duration,delay:s.delay,easing:s.easing}}containsAnimation(){return this.keyframes.length>1}buildKeyframes(){let t=this.keyframes,{delay:e,duration:i,easing:r}=this.timings;if(this._stretchStartingKeyframe&&e){let o=[],s=i+e,a=e/s,c=new Map(t[0]);c.set("offset",0),o.push(c);let l=new Map(t[0]);l.set("offset",M_(a)),o.push(l);let d=t.length-1;for(let u=1;u<=d;u++){let h=new Map(t[u]),f=h.get("offset"),v=e+f*i;h.set("offset",M_(v/s)),o.push(h)}i=s,e=0,r="",t=o}return ep(this.element,t,this.preStyleProps,this.postStyleProps,i,e,r,!0)}};function M_(n,t=3){let e=Math.pow(10,t-1);return Math.round(n*e)/e}function yT(n,t){let e=new Map,i;return n.forEach(r=>{if(r==="*"){i??=t.keys();for(let o of i)e.set(o,Yt)}else for(let[o,s]of r)e.set(o,s)}),e}function T_(n,t,e,i,r,o,s,a,c,l,d,u,h){return{type:0,element:n,triggerName:t,isRemovalTransition:r,fromState:e,fromStyles:o,toState:i,toStyles:s,timelines:a,queriedElements:c,preStyleProps:l,postStyleProps:d,totalTime:u,errors:h}}var Vm={},dl=class{_triggerName;ast;_stateStyles;constructor(t,e,i){this._triggerName=t,this.ast=e,this._stateStyles=i}match(t,e,i,r){return bT(this.ast.matchers,t,e,i,r)}buildStyles(t,e,i){let r=this._stateStyles.get("*");return t!==void 0&&(r=this._stateStyles.get(t?.toString())||r),r?r.buildStyles(e,i):new Map}build(t,e,i,r,o,s,a,c,l,d){let u=[],h=this.ast.options&&this.ast.options.params||Vm,f=a&&a.params||Vm,v=this.buildStyles(i,f,u),E=c&&c.params||Vm,D=this.buildStyles(r,E,u),x=new Set,X=new Map,ye=new Map,be=r==="void",Ct={params:j_(E,h),delay:this.ast.options?.delay},Le=d?[]:V_(t,e,this.ast.animation,o,s,v,D,Ct,l,u),ke=0;return Le.forEach(Ae=>{ke=Math.max(Ae.duration+Ae.delay,ke)}),u.length?T_(e,this._triggerName,i,r,be,v,D,[],[],X,ye,ke,u):(Le.forEach(Ae=>{let ot=Ae.element,It=vt(X,ot,new Set);Ae.preStyleProps.forEach(nn=>It.add(nn));let or=vt(ye,ot,new Set);Ae.postStyleProps.forEach(nn=>or.add(nn)),ot!==e&&x.add(ot)}),T_(e,this._triggerName,i,r,be,v,D,Le,[...x.values()],X,ye,ke))}};function bT(n,t,e,i,r){return n.some(o=>o(t,e,i,r))}function j_(n,t){let e=I({},t);return Object.entries(n).forEach(([i,r])=>{r!=null&&(e[i]=r)}),e}var qm=class{styles;defaultParams;normalizer;constructor(t,e,i){this.styles=t,this.defaultParams=e,this.normalizer=i}buildStyles(t,e){let i=new Map,r=j_(t,this.defaultParams);return this.styles.styles.forEach(o=>{typeof o!="string"&&o.forEach((s,a)=>{s&&(s=qr(s,r,e));let c=this.normalizer.normalizePropertyName(a,e);s=this.normalizer.normalizeStyleValue(a,c,s,e),i.set(a,s)})}),i}};function _T(n,t,e){return new Km(n,t,e)}var Km=class{name;ast;_normalizer;transitionFactories=[];fallbackTransition;states=new Map;constructor(t,e,i){this.name=t,this.ast=e,this._normalizer=i,e.states.forEach(r=>{let o=r.options&&r.options.params||{};this.states.set(r.name,new qm(r.style,o,i))}),A_(this.states,"true","1"),A_(this.states,"false","0"),e.transitions.forEach(r=>{this.transitionFactories.push(new dl(t,r,this.states))}),this.fallbackTransition=ET(t,this.states)}get containsQueries(){return this.ast.queryCount>0}matchTransition(t,e,i,r){return this.transitionFactories.find(s=>s.match(t,e,i,r))||null}matchStyles(t,e,i){return this.fallbackTransition.buildStyles(t,e,i)}};function ET(n,t,e){let i=[(s,a)=>!0],r={type:U.Sequence,steps:[],options:null},o={type:U.Transition,animation:r,matchers:i,options:null,queryCount:0,depCount:0};return new dl(n,o,t)}function A_(n,t,e){n.has(t)?n.has(e)||n.set(e,n.get(t)):n.has(e)&&n.set(t,n.get(e))}var wT=new ms,Qm=class{bodyNode;_driver;_normalizer;_animations=new Map;_playersById=new Map;players=[];constructor(t,e,i){this.bodyNode=t,this._driver=e,this._normalizer=i}register(t,e){let i=[],r=[],o=L_(this._driver,e,i,r);if(i.length)throw u_(i);this._animations.set(t,o)}_buildPlayer(t,e,i){let r=t.element,o=Tm(this._normalizer,t.keyframes,e,i);return this._driver.animate(r,o,t.duration,t.delay,t.easing,[],!0)}create(t,e,i={}){let r=[],o=this._animations.get(t),s,a=new Map;if(o?(s=V_(this._driver,e,o,Fm,Xc,new Map,new Map,i,wT,r),s.forEach(d=>{let u=vt(a,d.element,new Map);d.postStyleProps.forEach(h=>u.set(h,null))})):(r.push(f_()),s=[]),r.length)throw m_(r);a.forEach((d,u)=>{d.forEach((h,f)=>{d.set(f,this._driver.computeStyle(u,f,Yt))})});let c=s.map(d=>{let u=a.get(d.element);return this._buildPlayer(d,new Map,u)}),l=Un(c);return this._playersById.set(t,l),l.onDestroy(()=>this.destroy(t)),this.players.push(l),l}destroy(t){let e=this._getPlayer(t);e.destroy(),this._playersById.delete(t);let i=this.players.indexOf(e);i>=0&&this.players.splice(i,1)}_getPlayer(t){let e=this._playersById.get(t);if(!e)throw p_(t);return e}listen(t,e,i,r){let o=Zc(e,"","","");return Qc(this._getPlayer(t),i,o,r),()=>{}}command(t,e,i,r){if(i=="register"){this.register(t,r[0]);return}if(i=="create"){let s=r[0]||{};this.create(t,e,s);return}let o=this._getPlayer(t);switch(i){case"play":o.play();break;case"pause":o.pause();break;case"reset":o.reset();break;case"restart":o.restart();break;case"finish":o.finish();break;case"init":o.init();break;case"setPosition":o.setPosition(parseFloat(r[0]));break;case"destroy":this.destroy(t);break}}},N_="ng-animate-queued",DT=".ng-animate-queued",jm="ng-animate-disabled",CT=".ng-animate-disabled",IT="ng-star-inserted",xT=".ng-star-inserted",ST=[],B_={namespaceId:"",setForRemoval:!1,setForMove:!1,hasAnimation:!1,removedBeforeQueried:!1},MT={namespaceId:"",setForMove:!1,setForRemoval:!1,hasAnimation:!1,removedBeforeQueried:!0},Jt="__ng_removed",ps=class{namespaceId;value;options;get params(){return this.options.params}constructor(t,e=""){this.namespaceId=e;let i=t&&t.hasOwnProperty("value"),r=i?t.value:t;if(this.value=AT(r),i){let o=t,{value:s}=o,a=Fs(o,["value"]);this.options=a}else this.options={};this.options.params||(this.options.params={})}absorbOptions(t){let e=t.params;if(e){let i=this.options.params;Object.keys(e).forEach(r=>{i[r]==null&&(i[r]=e[r])})}}},fs="void",Bm=new ps(fs),Zm=class{id;hostElement;_engine;players=[];_triggers=new Map;_queue=[];_elementListeners=new Map;_hostClassName;constructor(t,e,i){this.id=t,this.hostElement=e,this._engine=i,this._hostClassName="ng-tns-"+t,jt(e,this._hostClassName)}listen(t,e,i,r){if(!this._triggers.has(e))throw h_(i,e);if(i==null||i.length==0)throw g_(e);if(!NT(i))throw v_(i,e);let o=vt(this._elementListeners,t,[]),s={name:e,phase:i,callback:r};o.push(s);let a=vt(this._engine.statesByElement,t,new Map);return a.has(e)||(jt(t,ls),jt(t,ls+"-"+e),a.set(e,Bm)),()=>{this._engine.afterFlush(()=>{let c=o.indexOf(s);c>=0&&o.splice(c,1),this._triggers.has(e)||a.delete(e)})}}register(t,e){return this._triggers.has(t)?!1:(this._triggers.set(t,e),!0)}_getTrigger(t){let e=this._triggers.get(t);if(!e)throw y_(t);return e}trigger(t,e,i,r=!0){let o=this._getTrigger(e),s=new hs(this.id,e,t),a=this._engine.statesByElement.get(t);a||(jt(t,ls),jt(t,ls+"-"+e),this._engine.statesByElement.set(t,a=new Map));let c=a.get(e),l=new ps(i,this.id);if(!(i&&i.hasOwnProperty("value"))&&c&&l.absorbOptions(c.options),a.set(e,l),c||(c=Bm),!(l.value===fs)&&c.value===l.value){if(!FT(c.params,l.params)){let E=[],D=o.matchStyles(c.value,c.params,E),x=o.matchStyles(l.value,l.params,E);E.length?this._engine.reportError(E):this._engine.afterFlush(()=>{fi(t,D),Xt(t,x)})}return}let h=vt(this._engine.playersByElement,t,[]);h.forEach(E=>{E.namespaceId==this.id&&E.triggerName==e&&E.queued&&E.destroy()});let f=o.matchTransition(c.value,l.value,t,l.params),v=!1;if(!f){if(!r)return;f=o.fallbackTransition,v=!0}return this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:e,transition:f,fromState:c,toState:l,player:s,isFallbackTransition:v}),v||(jt(t,N_),s.onStart(()=>{Kr(t,N_)})),s.onDone(()=>{let E=this.players.indexOf(s);E>=0&&this.players.splice(E,1);let D=this._engine.playersByElement.get(t);if(D){let x=D.indexOf(s);x>=0&&D.splice(x,1)}}),this.players.push(s),h.push(s),s}deregister(t){this._triggers.delete(t),this._engine.statesByElement.forEach(e=>e.delete(t)),this._elementListeners.forEach((e,i)=>{this._elementListeners.set(i,e.filter(r=>r.name!=t))})}clearElementCache(t){this._engine.statesByElement.delete(t),this._elementListeners.delete(t);let e=this._engine.playersByElement.get(t);e&&(e.forEach(i=>i.destroy()),this._engine.playersByElement.delete(t))}_signalRemovalForInnerTriggers(t,e){let i=this._engine.driver.query(t,ds,!0);i.forEach(r=>{if(r[Jt])return;let o=this._engine.fetchNamespacesByElement(r);o.size?o.forEach(s=>s.triggerLeaveAnimation(r,e,!1,!0)):this.clearElementCache(r)}),this._engine.afterFlushAnimationsDone(()=>i.forEach(r=>this.clearElementCache(r)))}triggerLeaveAnimation(t,e,i,r){let o=this._engine.statesByElement.get(t),s=new Map;if(o){let a=[];if(o.forEach((c,l)=>{if(s.set(l,c.value),this._triggers.has(l)){let d=this.trigger(t,l,fs,r);d&&a.push(d)}}),a.length)return this._engine.markElementAsRemoved(this.id,t,!0,e,s),i&&Un(a).onDone(()=>this._engine.processLeaveNode(t)),!0}return!1}prepareLeaveAnimationListeners(t){let e=this._elementListeners.get(t),i=this._engine.statesByElement.get(t);if(e&&i){let r=new Set;e.forEach(o=>{let s=o.name;if(r.has(s))return;r.add(s);let c=this._triggers.get(s).fallbackTransition,l=i.get(s)||Bm,d=new ps(fs),u=new hs(this.id,s,t);this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:s,transition:c,fromState:l,toState:d,player:u,isFallbackTransition:!0})})}}removeNode(t,e){let i=this._engine;if(t.childElementCount&&this._signalRemovalForInnerTriggers(t,e),this.triggerLeaveAnimation(t,e,!0))return;let r=!1;if(i.totalAnimations){let o=i.players.length?i.playersByQueriedElement.get(t):[];if(o&&o.length)r=!0;else{let s=t;for(;s=s.parentNode;)if(i.statesByElement.get(s)){r=!0;break}}}if(this.prepareLeaveAnimationListeners(t),r)i.markElementAsRemoved(this.id,t,!1,e);else{let o=t[Jt];(!o||o===B_)&&(i.afterFlush(()=>this.clearElementCache(t)),i.destroyInnerAnimations(t),i._onRemovalComplete(t,e))}}insertNode(t,e){jt(t,this._hostClassName)}drainQueuedTransitions(t){let e=[];return this._queue.forEach(i=>{let r=i.player;if(r.destroyed)return;let o=i.element,s=this._elementListeners.get(o);s&&s.forEach(a=>{if(a.name==i.triggerName){let c=Zc(o,i.triggerName,i.fromState.value,i.toState.value);c._data=t,Qc(i.player,a.phase,c,a.callback)}}),r.markedForDestroy?this._engine.afterFlush(()=>{r.destroy()}):e.push(i)}),this._queue=[],e.sort((i,r)=>{let o=i.transition.ast.depCount,s=r.transition.ast.depCount;return o==0||s==0?o-s:this._engine.driver.containsElement(i.element,r.element)?1:-1})}destroy(t){this.players.forEach(e=>e.destroy()),this._signalRemovalForInnerTriggers(this.hostElement,t)}},Ym=class{bodyNode;driver;_normalizer;players=[];newHostElements=new Map;playersByElement=new Map;playersByQueriedElement=new Map;statesByElement=new Map;disabledNodes=new Set;totalAnimations=0;totalQueuedPlayers=0;_namespaceLookup={};_namespaceList=[];_flushFns=[];_whenQuietFns=[];namespacesByHostElement=new Map;collectedEnterElements=[];collectedLeaveElements=[];onRemovalComplete=(t,e)=>{};_onRemovalComplete(t,e){this.onRemovalComplete(t,e)}constructor(t,e,i){this.bodyNode=t,this.driver=e,this._normalizer=i}get queuedPlayers(){let t=[];return this._namespaceList.forEach(e=>{e.players.forEach(i=>{i.queued&&t.push(i)})}),t}createNamespace(t,e){let i=new Zm(t,e,this);return this.bodyNode&&this.driver.containsElement(this.bodyNode,e)?this._balanceNamespaceList(i,e):(this.newHostElements.set(e,i),this.collectEnterElement(e)),this._namespaceLookup[t]=i}_balanceNamespaceList(t,e){let i=this._namespaceList,r=this.namespacesByHostElement;if(i.length-1>=0){let s=!1,a=this.driver.getParentElement(e);for(;a;){let c=r.get(a);if(c){let l=i.indexOf(c);i.splice(l+1,0,t),s=!0;break}a=this.driver.getParentElement(a)}s||i.unshift(t)}else i.push(t);return r.set(e,t),t}register(t,e){let i=this._namespaceLookup[t];return i||(i=this.createNamespace(t,e)),i}registerTrigger(t,e,i){let r=this._namespaceLookup[t];r&&r.register(e,i)&&this.totalAnimations++}destroy(t,e){t&&(this.afterFlush(()=>{}),this.afterFlushAnimationsDone(()=>{let i=this._fetchNamespace(t);this.namespacesByHostElement.delete(i.hostElement);let r=this._namespaceList.indexOf(i);r>=0&&this._namespaceList.splice(r,1),i.destroy(e),delete this._namespaceLookup[t]}))}_fetchNamespace(t){return this._namespaceLookup[t]}fetchNamespacesByElement(t){let e=new Set,i=this.statesByElement.get(t);if(i){for(let r of i.values())if(r.namespaceId){let o=this._fetchNamespace(r.namespaceId);o&&e.add(o)}}return e}trigger(t,e,i,r){if(rl(e)){let o=this._fetchNamespace(t);if(o)return o.trigger(e,i,r),!0}return!1}insertNode(t,e,i,r){if(!rl(e))return;let o=e[Jt];if(o&&o.setForRemoval){o.setForRemoval=!1,o.setForMove=!0;let s=this.collectedLeaveElements.indexOf(e);s>=0&&this.collectedLeaveElements.splice(s,1)}if(t){let s=this._fetchNamespace(t);s&&s.insertNode(e,i)}r&&this.collectEnterElement(e)}collectEnterElement(t){this.collectedEnterElements.push(t)}markElementAsDisabled(t,e){e?this.disabledNodes.has(t)||(this.disabledNodes.add(t),jt(t,jm)):this.disabledNodes.has(t)&&(this.disabledNodes.delete(t),Kr(t,jm))}removeNode(t,e,i){if(rl(e)){let r=t?this._fetchNamespace(t):null;r?r.removeNode(e,i):this.markElementAsRemoved(t,e,!1,i);let o=this.namespacesByHostElement.get(e);o&&o.id!==t&&o.removeNode(e,i)}else this._onRemovalComplete(e,i)}markElementAsRemoved(t,e,i,r,o){this.collectedLeaveElements.push(e),e[Jt]={namespaceId:t,setForRemoval:r,hasAnimation:i,removedBeforeQueried:!1,previousTriggersValues:o}}listen(t,e,i,r,o){return rl(e)?this._fetchNamespace(t).listen(e,i,r,o):()=>{}}_buildInstruction(t,e,i,r,o){return t.transition.build(this.driver,t.element,t.fromState.value,t.toState.value,i,r,t.fromState.options,t.toState.options,e,o)}destroyInnerAnimations(t){let e=this.driver.query(t,ds,!0);e.forEach(i=>this.destroyActiveAnimationsForElement(i)),this.playersByQueriedElement.size!=0&&(e=this.driver.query(t,Jc,!0),e.forEach(i=>this.finishActiveQueriedAnimationOnElement(i)))}destroyActiveAnimationsForElement(t){let e=this.playersByElement.get(t);e&&e.forEach(i=>{i.queued?i.markedForDestroy=!0:i.destroy()})}finishActiveQueriedAnimationOnElement(t){let e=this.playersByQueriedElement.get(t);e&&e.forEach(i=>i.finish())}whenRenderingDone(){return new Promise(t=>{if(this.players.length)return Un(this.players).onDone(()=>t());t()})}processLeaveNode(t){let e=t[Jt];if(e&&e.setForRemoval){if(t[Jt]=B_,e.namespaceId){this.destroyInnerAnimations(t);let i=this._fetchNamespace(e.namespaceId);i&&i.clearElementCache(t)}this._onRemovalComplete(t,e.setForRemoval)}t.classList?.contains(jm)&&this.markElementAsDisabled(t,!1),this.driver.query(t,CT,!0).forEach(i=>{this.markElementAsDisabled(i,!1)})}flush(t=-1){let e=[];if(this.newHostElements.size&&(this.newHostElements.forEach((i,r)=>this._balanceNamespaceList(i,r)),this.newHostElements.clear()),this.totalAnimations&&this.collectedEnterElements.length)for(let i=0;i<this.collectedEnterElements.length;i++){let r=this.collectedEnterElements[i];jt(r,IT)}if(this._namespaceList.length&&(this.totalQueuedPlayers||this.collectedLeaveElements.length)){let i=[];try{e=this._flushAnimations(i,t)}finally{for(let r=0;r<i.length;r++)i[r]()}}else for(let i=0;i<this.collectedLeaveElements.length;i++){let r=this.collectedLeaveElements[i];this.processLeaveNode(r)}if(this.totalQueuedPlayers=0,this.collectedEnterElements.length=0,this.collectedLeaveElements.length=0,this._flushFns.forEach(i=>i()),this._flushFns=[],this._whenQuietFns.length){let i=this._whenQuietFns;this._whenQuietFns=[],e.length?Un(e).onDone(()=>{i.forEach(r=>r())}):i.forEach(r=>r())}}reportError(t){throw b_(t)}_flushAnimations(t,e){let i=new ms,r=[],o=new Map,s=[],a=new Map,c=new Map,l=new Map,d=new Set;this.disabledNodes.forEach(C=>{d.add(C);let S=this.driver.query(C,DT,!0);for(let k=0;k<S.length;k++)d.add(S[k])});let u=this.bodyNode,h=Array.from(this.statesByElement.keys()),f=F_(h,this.collectedEnterElements),v=new Map,E=0;f.forEach((C,S)=>{let k=Fm+E++;v.set(S,k),C.forEach(J=>jt(J,k))});let D=[],x=new Set,X=new Set;for(let C=0;C<this.collectedLeaveElements.length;C++){let S=this.collectedLeaveElements[C],k=S[Jt];k&&k.setForRemoval&&(D.push(S),x.add(S),k.hasAnimation?this.driver.query(S,xT,!0).forEach(J=>x.add(J)):X.add(S))}let ye=new Map,be=F_(h,Array.from(x));be.forEach((C,S)=>{let k=Xc+E++;ye.set(S,k),C.forEach(J=>jt(J,k))}),t.push(()=>{f.forEach((C,S)=>{let k=v.get(S);C.forEach(J=>Kr(J,k))}),be.forEach((C,S)=>{let k=ye.get(S);C.forEach(J=>Kr(J,k))}),D.forEach(C=>{this.processLeaveNode(C)})});let Ct=[],Le=[];for(let C=this._namespaceList.length-1;C>=0;C--)this._namespaceList[C].drainQueuedTransitions(e).forEach(k=>{let J=k.player,Ve=k.element;if(Ct.push(J),this.collectedEnterElements.length){let Qe=Ve[Jt];if(Qe&&Qe.setForMove){if(Qe.previousTriggersValues&&Qe.previousTriggersValues.has(k.triggerName)){let hi=Qe.previousTriggersValues.get(k.triggerName),xt=this.statesByElement.get(k.element);if(xt&&xt.has(k.triggerName)){let ks=xt.get(k.triggerName);ks.value=hi,xt.set(k.triggerName,ks)}}J.destroy();return}}let rn=!u||!this.driver.containsElement(u,Ve),bt=ye.get(Ve),zn=v.get(Ve),De=this._buildInstruction(k,i,zn,bt,rn);if(De.errors&&De.errors.length){Le.push(De);return}if(rn){J.onStart(()=>fi(Ve,De.fromStyles)),J.onDestroy(()=>Xt(Ve,De.toStyles)),r.push(J);return}if(k.isFallbackTransition){J.onStart(()=>fi(Ve,De.fromStyles)),J.onDestroy(()=>Xt(Ve,De.toStyles)),r.push(J);return}let Np=[];De.timelines.forEach(Qe=>{Qe.stretchStartingKeyframe=!0,this.disabledNodes.has(Qe.element)||Np.push(Qe)}),De.timelines=Np,i.append(Ve,De.timelines);let LE={instruction:De,player:J,element:Ve};s.push(LE),De.queriedElements.forEach(Qe=>vt(a,Qe,[]).push(J)),De.preStyleProps.forEach((Qe,hi)=>{if(Qe.size){let xt=c.get(hi);xt||c.set(hi,xt=new Set),Qe.forEach((ks,$l)=>xt.add($l))}}),De.postStyleProps.forEach((Qe,hi)=>{let xt=l.get(hi);xt||l.set(hi,xt=new Set),Qe.forEach((ks,$l)=>xt.add($l))})});if(Le.length){let C=[];Le.forEach(S=>{C.push(__(S.triggerName,S.errors))}),Ct.forEach(S=>S.destroy()),this.reportError(C)}let ke=new Map,Ae=new Map;s.forEach(C=>{let S=C.element;i.has(S)&&(Ae.set(S,S),this._beforeAnimationBuild(C.player.namespaceId,C.instruction,ke))}),r.forEach(C=>{let S=C.element;this._getPreviousPlayers(S,!1,C.namespaceId,C.triggerName,null).forEach(J=>{vt(ke,S,[]).push(J),J.destroy()})});let ot=D.filter(C=>O_(C,c,l)),It=new Map;R_(It,this.driver,X,l,Yt).forEach(C=>{O_(C,c,l)&&ot.push(C)});let nn=new Map;f.forEach((C,S)=>{R_(nn,this.driver,new Set(C),c,cs)}),ot.forEach(C=>{let S=It.get(C),k=nn.get(C);It.set(C,new Map([...S?.entries()??[],...k?.entries()??[]]))});let Ul=[],Tp=[],Ap={};s.forEach(C=>{let{element:S,player:k,instruction:J}=C;if(i.has(S)){if(d.has(S)){k.onDestroy(()=>Xt(S,J.toStyles)),k.disabled=!0,k.overrideTotalTime(J.totalTime),r.push(k);return}let Ve=Ap;if(Ae.size>1){let bt=S,zn=[];for(;bt=bt.parentNode;){let De=Ae.get(bt);if(De){Ve=De;break}zn.push(bt)}zn.forEach(De=>Ae.set(De,Ve))}let rn=this._buildAnimation(k.namespaceId,J,ke,o,nn,It);if(k.setRealPlayer(rn),Ve===Ap)Ul.push(k);else{let bt=this.playersByElement.get(Ve);bt&&bt.length&&(k.parentPlayer=Un(bt)),r.push(k)}}else fi(S,J.fromStyles),k.onDestroy(()=>Xt(S,J.toStyles)),Tp.push(k),d.has(S)&&r.push(k)}),Tp.forEach(C=>{let S=o.get(C.element);if(S&&S.length){let k=Un(S);C.setRealPlayer(k)}}),r.forEach(C=>{C.parentPlayer?C.syncPlayerEvents(C.parentPlayer):C.destroy()});for(let C=0;C<D.length;C++){let S=D[C],k=S[Jt];if(Kr(S,Xc),k&&k.hasAnimation)continue;let J=[];if(a.size){let rn=a.get(S);rn&&rn.length&&J.push(...rn);let bt=this.driver.query(S,Jc,!0);for(let zn=0;zn<bt.length;zn++){let De=a.get(bt[zn]);De&&De.length&&J.push(...De)}}let Ve=J.filter(rn=>!rn.destroyed);Ve.length?kT(this,S,Ve):this.processLeaveNode(S)}return D.length=0,Ul.forEach(C=>{this.players.push(C),C.onDone(()=>{C.destroy();let S=this.players.indexOf(C);this.players.splice(S,1)}),C.play()}),Ul}afterFlush(t){this._flushFns.push(t)}afterFlushAnimationsDone(t){this._whenQuietFns.push(t)}_getPreviousPlayers(t,e,i,r,o){let s=[];if(e){let a=this.playersByQueriedElement.get(t);a&&(s=a)}else{let a=this.playersByElement.get(t);if(a){let c=!o||o==fs;a.forEach(l=>{l.queued||!c&&l.triggerName!=r||s.push(l)})}}return(i||r)&&(s=s.filter(a=>!(i&&i!=a.namespaceId||r&&r!=a.triggerName))),s}_beforeAnimationBuild(t,e,i){let r=e.triggerName,o=e.element,s=e.isRemovalTransition?void 0:t,a=e.isRemovalTransition?void 0:r;for(let c of e.timelines){let l=c.element,d=l!==o,u=vt(i,l,[]);this._getPreviousPlayers(l,d,s,a,e.toState).forEach(f=>{let v=f.getRealPlayer();v.beforeDestroy&&v.beforeDestroy(),f.destroy(),u.push(f)})}fi(o,e.fromStyles)}_buildAnimation(t,e,i,r,o,s){let a=e.triggerName,c=e.element,l=[],d=new Set,u=new Set,h=e.timelines.map(v=>{let E=v.element;d.add(E);let D=E[Jt];if(D&&D.removedBeforeQueried)return new Hn(v.duration,v.delay);let x=E!==c,X=RT((i.get(E)||ST).map(ke=>ke.getRealPlayer())).filter(ke=>{let Ae=ke;return Ae.element?Ae.element===E:!1}),ye=o.get(E),be=s.get(E),Ct=Tm(this._normalizer,v.keyframes,ye,be),Le=this._buildPlayer(v,Ct,X);if(v.subTimeline&&r&&u.add(E),x){let ke=new hs(t,a,E);ke.setRealPlayer(Le),l.push(ke)}return Le});l.forEach(v=>{vt(this.playersByQueriedElement,v.element,[]).push(v),v.onDone(()=>TT(this.playersByQueriedElement,v.element,v))}),d.forEach(v=>jt(v,Om));let f=Un(h);return f.onDestroy(()=>{d.forEach(v=>Kr(v,Om)),Xt(c,e.toStyles)}),u.forEach(v=>{vt(r,v,[]).push(f)}),f}_buildPlayer(t,e,i){return e.length>0?this.driver.animate(t.element,e,t.duration,t.delay,t.easing,i):new Hn(t.duration,t.delay)}},hs=class{namespaceId;triggerName;element;_player=new Hn;_containsRealPlayer=!1;_queuedCallbacks=new Map;destroyed=!1;parentPlayer=null;markedForDestroy=!1;disabled=!1;queued=!0;totalTime=0;constructor(t,e,i){this.namespaceId=t,this.triggerName=e,this.element=i}setRealPlayer(t){this._containsRealPlayer||(this._player=t,this._queuedCallbacks.forEach((e,i)=>{e.forEach(r=>Qc(t,i,void 0,r))}),this._queuedCallbacks.clear(),this._containsRealPlayer=!0,this.overrideTotalTime(t.totalTime),this.queued=!1)}getRealPlayer(){return this._player}overrideTotalTime(t){this.totalTime=t}syncPlayerEvents(t){let e=this._player;e.triggerCallback&&t.onStart(()=>e.triggerCallback("start")),t.onDone(()=>this.finish()),t.onDestroy(()=>this.destroy())}_queueEvent(t,e){vt(this._queuedCallbacks,t,[]).push(e)}onDone(t){this.queued&&this._queueEvent("done",t),this._player.onDone(t)}onStart(t){this.queued&&this._queueEvent("start",t),this._player.onStart(t)}onDestroy(t){this.queued&&this._queueEvent("destroy",t),this._player.onDestroy(t)}init(){this._player.init()}hasStarted(){return this.queued?!1:this._player.hasStarted()}play(){!this.queued&&this._player.play()}pause(){!this.queued&&this._player.pause()}restart(){!this.queued&&this._player.restart()}finish(){this._player.finish()}destroy(){this.destroyed=!0,this._player.destroy()}reset(){!this.queued&&this._player.reset()}setPosition(t){this.queued||this._player.setPosition(t)}getPosition(){return this.queued?0:this._player.getPosition()}triggerCallback(t){let e=this._player;e.triggerCallback&&e.triggerCallback(t)}};function TT(n,t,e){let i=n.get(t);if(i){if(i.length){let r=i.indexOf(e);i.splice(r,1)}i.length==0&&n.delete(t)}return i}function AT(n){return n??null}function rl(n){return n&&n.nodeType===1}function NT(n){return n=="start"||n=="done"}function k_(n,t){let e=n.style.display;return n.style.display=t??"none",e}function R_(n,t,e,i,r){let o=[];e.forEach(c=>o.push(k_(c)));let s=[];i.forEach((c,l)=>{let d=new Map;c.forEach(u=>{let h=t.computeStyle(l,u,r);d.set(u,h),(!h||h.length==0)&&(l[Jt]=MT,s.push(l))}),n.set(l,d)});let a=0;return e.forEach(c=>k_(c,o[a++])),s}function F_(n,t){let e=new Map;if(n.forEach(a=>e.set(a,[])),t.length==0)return e;let i=1,r=new Set(t),o=new Map;function s(a){if(!a)return i;let c=o.get(a);if(c)return c;let l=a.parentNode;return e.has(l)?c=l:r.has(l)?c=i:c=s(l),o.set(a,c),c}return t.forEach(a=>{let c=s(a);c!==i&&e.get(c).push(a)}),e}function jt(n,t){n.classList?.add(t)}function Kr(n,t){n.classList?.remove(t)}function kT(n,t,e){Un(e).onDone(()=>n.processLeaveNode(t))}function RT(n){let t=[];return H_(n,t),t}function H_(n,t){for(let e=0;e<n.length;e++){let i=n[e];i instanceof Gr?H_(i.players,t):t.push(i)}}function FT(n,t){let e=Object.keys(n),i=Object.keys(t);if(e.length!=i.length)return!1;for(let r=0;r<e.length;r++){let o=e[r];if(!t.hasOwnProperty(o)||n[o]!==t[o])return!1}return!0}function O_(n,t,e){let i=e.get(n);if(!i)return!1;let r=t.get(n);return r?i.forEach(o=>r.add(o)):t.set(n,i),e.delete(n),!0}var Qr=class{_driver;_normalizer;_transitionEngine;_timelineEngine;_triggerCache={};onRemovalComplete=(t,e)=>{};constructor(t,e,i){this._driver=e,this._normalizer=i,this._transitionEngine=new Ym(t.body,e,i),this._timelineEngine=new Qm(t.body,e,i),this._transitionEngine.onRemovalComplete=(r,o)=>this.onRemovalComplete(r,o)}registerTrigger(t,e,i,r,o){let s=t+"-"+r,a=this._triggerCache[s];if(!a){let c=[],l=[],d=L_(this._driver,o,c,l);if(c.length)throw d_(r,c);a=_T(r,d,this._normalizer),this._triggerCache[s]=a}this._transitionEngine.registerTrigger(e,r,a)}register(t,e){this._transitionEngine.register(t,e)}destroy(t,e){this._transitionEngine.destroy(t,e)}onInsert(t,e,i,r){this._transitionEngine.insertNode(t,e,i,r)}onRemove(t,e,i){this._transitionEngine.removeNode(t,e,i)}disableAnimations(t,e){this._transitionEngine.markElementAsDisabled(t,e)}process(t,e,i,r){if(i.charAt(0)=="@"){let[o,s]=Am(i),a=r;this._timelineEngine.command(o,e,s,a)}else this._transitionEngine.trigger(t,e,i,r)}listen(t,e,i,r,o){if(i.charAt(0)=="@"){let[s,a]=Am(i);return this._timelineEngine.listen(s,e,a,o)}return this._transitionEngine.listen(t,e,i,r,o)}flush(t=-1){this._transitionEngine.flush(t)}get players(){return[...this._transitionEngine.players,...this._timelineEngine.players]}whenRenderingDone(){return this._transitionEngine.whenRenderingDone()}afterFlushAnimationsDone(t){this._transitionEngine.afterFlushAnimationsDone(t)}};function OT(n,t){let e=null,i=null;return Array.isArray(t)&&t.length?(e=Hm(t[0]),t.length>1&&(i=Hm(t[t.length-1]))):t instanceof Map&&(e=Hm(t)),e||i?new PT(n,e,i):null}var PT=(()=>{class n{_element;_startStyles;_endStyles;static initialStylesByElement=new WeakMap;_state=0;_initialStyles;constructor(e,i,r){this._element=e,this._startStyles=i,this._endStyles=r;let o=n.initialStylesByElement.get(e);o||n.initialStylesByElement.set(e,o=new Map),this._initialStyles=o}start(){this._state<1&&(this._startStyles&&Xt(this._element,this._startStyles,this._initialStyles),this._state=1)}finish(){this.start(),this._state<2&&(Xt(this._element,this._initialStyles),this._endStyles&&(Xt(this._element,this._endStyles),this._endStyles=null),this._state=1)}destroy(){this.finish(),this._state<3&&(n.initialStylesByElement.delete(this._element),this._startStyles&&(fi(this._element,this._startStyles),this._endStyles=null),this._endStyles&&(fi(this._element,this._endStyles),this._endStyles=null),Xt(this._element,this._initialStyles),this._state=3)}}return n})();function Hm(n){let t=null;return n.forEach((e,i)=>{LT(i)&&(t=t||new Map,t.set(i,e))}),t}function LT(n){return n==="display"||n==="position"}var ul=class{element;keyframes;options;_specialStyles;_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_duration;_delay;_initialized=!1;_finished=!1;_started=!1;_destroyed=!1;_finalKeyframe;_originalOnDoneFns=[];_originalOnStartFns=[];domPlayer=null;time=0;parentPlayer=null;currentSnapshot=new Map;constructor(t,e,i,r){this.element=t,this.keyframes=e,this.options=i,this._specialStyles=r,this._duration=i.duration,this._delay=i.delay||0,this.time=this._duration+this._delay}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this._buildPlayer()&&this._preparePlayerBeforeStart()}_buildPlayer(){if(this._initialized)return this.domPlayer;this._initialized=!0;let t=this.keyframes,e=this._triggerWebAnimation(this.element,t,this.options);if(!e)return this._onFinish(),null;this.domPlayer=e,this._finalKeyframe=t.length?t[t.length-1]:new Map;let i=()=>this._onFinish();return e.addEventListener("finish",i),this.onDestroy(()=>{e.removeEventListener("finish",i)}),e}_preparePlayerBeforeStart(){this._delay?this._resetDomPlayerState():this.domPlayer?.pause()}_convertKeyframesToObject(t){let e=[];return t.forEach(i=>{e.push(Object.fromEntries(i))}),e}_triggerWebAnimation(t,e,i){let r=this._convertKeyframesToObject(e);try{return t.animate(r,i)}catch(o){return null}}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}play(){let t=this._buildPlayer();t&&(this.hasStarted()||(this._onStartFns.forEach(e=>e()),this._onStartFns=[],this._started=!0,this._specialStyles&&this._specialStyles.start()),t.play())}pause(){this.init(),this.domPlayer?.pause()}finish(){this.init(),this.domPlayer&&(this._specialStyles&&this._specialStyles.finish(),this._onFinish(),this.domPlayer.finish())}reset(){this._resetDomPlayerState(),this._destroyed=!1,this._finished=!1,this._started=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}_resetDomPlayerState(){this.domPlayer?.cancel()}restart(){this.reset(),this.play()}hasStarted(){return this._started}destroy(){this._destroyed||(this._destroyed=!0,this._resetDomPlayerState(),this._onFinish(),this._specialStyles&&this._specialStyles.destroy(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}setPosition(t){this.domPlayer||this.init(),this.domPlayer&&(this.domPlayer.currentTime=t*this.time)}getPosition(){return this.domPlayer?+(this.domPlayer.currentTime??0)/this.time:this._initialized?1:0}get totalTime(){return this._delay+this._duration}beforeDestroy(){let t=new Map;this.hasStarted()&&this._finalKeyframe.forEach((i,r)=>{r!=="offset"&&t.set(r,this._finished?i:tl(this.element,r))}),this.currentSnapshot=t}triggerCallback(t){let e=t==="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},fl=class{validateStyleProperty(t){return!0}validateAnimatableStyleProperty(t){return!0}containsElement(t,e){return Nm(t,e)}getParentElement(t){return Yc(t)}query(t,e,i){return km(t,e,i)}computeStyle(t,e,i){return tl(t,e)}animate(t,e,i,r,o,s=[]){let a=r==0?"both":"forwards",c={duration:i,delay:r,fill:a};o&&(c.easing=o);let l=new Map,d=s.filter(f=>f instanceof ul);C_(i,r)&&d.forEach(f=>{f.currentSnapshot.forEach((v,E)=>l.set(E,v))});let u=w_(e).map(f=>new Map(f));u=I_(t,u,l);let h=OT(t,u);return new ul(t,u,c,h)}};var ol="@",U_="@.disabled",ml=class{namespaceId;delegate;engine;_onDestroy;\u0275type=0;constructor(t,e,i,r){this.namespaceId=t,this.delegate=e,this.engine=i,this._onDestroy=r}get data(){return this.delegate.data}destroyNode(t){this.delegate.destroyNode?.(t)}destroy(){this.engine.destroy(this.namespaceId,this.delegate),this.engine.afterFlushAnimationsDone(()=>{queueMicrotask(()=>{this.delegate.destroy()})}),this._onDestroy?.()}createElement(t,e){return this.delegate.createElement(t,e)}createComment(t){return this.delegate.createComment(t)}createText(t){return this.delegate.createText(t)}appendChild(t,e){this.delegate.appendChild(t,e),this.engine.onInsert(this.namespaceId,e,t,!1)}insertBefore(t,e,i,r=!0){this.delegate.insertBefore(t,e,i),this.engine.onInsert(this.namespaceId,e,t,r)}removeChild(t,e,i,r){if(r){this.delegate.removeChild(t,e,i,r);return}this.parentNode(e)&&this.engine.onRemove(this.namespaceId,e,this.delegate)}selectRootElement(t,e){return this.delegate.selectRootElement(t,e)}parentNode(t){return this.delegate.parentNode(t)}nextSibling(t){return this.delegate.nextSibling(t)}setAttribute(t,e,i,r){this.delegate.setAttribute(t,e,i,r)}removeAttribute(t,e,i){this.delegate.removeAttribute(t,e,i)}addClass(t,e){this.delegate.addClass(t,e)}removeClass(t,e){this.delegate.removeClass(t,e)}setStyle(t,e,i,r){this.delegate.setStyle(t,e,i,r)}removeStyle(t,e,i){this.delegate.removeStyle(t,e,i)}setProperty(t,e,i){e.charAt(0)==ol&&e==U_?this.disableAnimations(t,!!i):this.delegate.setProperty(t,e,i)}setValue(t,e){this.delegate.setValue(t,e)}listen(t,e,i,r){return this.delegate.listen(t,e,i,r)}disableAnimations(t,e){this.engine.disableAnimations(t,e)}},Xm=class extends ml{factory;constructor(t,e,i,r,o){super(e,i,r,o),this.factory=t,this.namespaceId=e}setProperty(t,e,i){e.charAt(0)==ol?e.charAt(1)=="."&&e==U_?(i=i===void 0?!0:!!i,this.disableAnimations(t,i)):this.engine.process(this.namespaceId,t,e.slice(1),i):this.delegate.setProperty(t,e,i)}listen(t,e,i,r){if(e.charAt(0)==ol){let o=VT(t),s=e.slice(1),a="";return s.charAt(0)!=ol&&([s,a]=jT(s)),this.engine.listen(this.namespaceId,o,s,a,c=>{let l=c._data||-1;this.factory.scheduleListenerCallback(l,i,c)})}return this.delegate.listen(t,e,i,r)}};function VT(n){switch(n){case"body":return document.body;case"document":return document;case"window":return window;default:return n}}function jT(n){let t=n.indexOf("."),e=n.substring(0,t),i=n.slice(t+1);return[e,i]}var pl=class{delegate;engine;_zone;_currentId=0;_microtaskId=1;_animationCallbacksBuffer=[];_rendererCache=new Map;_cdRecurDepth=0;constructor(t,e,i){this.delegate=t,this.engine=e,this._zone=i,e.onRemovalComplete=(r,o)=>{o?.removeChild(null,r)}}createRenderer(t,e){let r=this.delegate.createRenderer(t,e);if(!t||!e?.data?.animation){let l=this._rendererCache,d=l.get(r);if(!d){let u=()=>l.delete(r);d=new ml("",r,this.engine,u),l.set(r,d)}return d}let o=e.id,s=e.id+"-"+this._currentId;this._currentId++,this.engine.register(s,t);let a=l=>{Array.isArray(l)?l.forEach(a):this.engine.registerTrigger(o,s,t,l.name,l)};return e.data.animation.forEach(a),new Xm(this,s,r,this.engine)}begin(){this._cdRecurDepth++,this.delegate.begin&&this.delegate.begin()}_scheduleCountTask(){queueMicrotask(()=>{this._microtaskId++})}scheduleListenerCallback(t,e,i){if(t>=0&&t<this._microtaskId){this._zone.run(()=>e(i));return}let r=this._animationCallbacksBuffer;r.length==0&&queueMicrotask(()=>{this._zone.run(()=>{r.forEach(o=>{let[s,a]=o;s(a)}),this._animationCallbacksBuffer=[]})}),r.push([e,i])}end(){this._cdRecurDepth--,this._cdRecurDepth==0&&this._zone.runOutsideAngular(()=>{this._scheduleCountTask(),this.engine.flush(this._microtaskId)}),this.delegate.end&&this.delegate.end()}whenRenderingDone(){return this.engine.whenRenderingDone()}componentReplaced(t){this.engine.flush(),this.delegate.componentReplaced?.(t)}};var HT=(()=>{class n extends Qr{constructor(e,i,r){super(e,i,r)}ngOnDestroy(){this.flush()}static \u0275fac=function(i){return new(i||n)(M(Z),M(Ji),M(er))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})();function UT(){return new sl}function $T(){return new pl(m(ss),m(Qr),m(O))}var $_=[{provide:er,useFactory:UT},{provide:Qr,useClass:HT},{provide:at,useFactory:$T}],Z8=[{provide:Ji,useClass:Jm},{provide:xr,useValue:"NoopAnimations"},...$_],zT=[{provide:Ji,useFactory:()=>new fl},{provide:xr,useFactory:()=>"BrowserAnimations"},...$_];function z_(){return di("NgEagerAnimations"),[...zT]}var tp="Service workers are disabled or not supported by this browser",Zr=class{serviceWorker;worker;registration;events;constructor(t,e){if(this.serviceWorker=t,!t)this.worker=this.events=this.registration=new q(i=>i.error(new _(5601,!1)));else{let i=null,r=new Q;this.worker=new q(l=>(i!==null&&l.next(i),r.subscribe(d=>l.next(d))));let o=()=>{let{controller:l}=t;l!==null&&(i=l,r.next(i))};t.addEventListener("controllerchange",o),o(),this.registration=this.worker.pipe(Ii(()=>t.getRegistration().then(l=>{if(!l)throw new _(5601,!1);return l})));let s=new Q;this.events=s.asObservable();let a=l=>{let{data:d}=l;d?.type&&s.next(d)};t.addEventListener("message",a),e?.get(Qt,null,{optional:!0})?.onDestroy(()=>{t.removeEventListener("controllerchange",o),t.removeEventListener("message",a)})}}postMessage(t,e){return new Promise(i=>{this.worker.pipe(Dn(1)).subscribe(r=>{r.postMessage(I({action:t},e)),i()})})}postMessageWithOperation(t,e,i){let r=this.waitForOperationCompleted(i),o=this.postMessage(t,e);return Promise.all([o,r]).then(([,s])=>s)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(t){let e;return typeof t=="string"?e=i=>i.type===t:e=i=>t.includes(i.type),this.events.pipe(Mt(e))}nextEventOfType(t){return this.eventsOfType(t).pipe(Dn(1))}waitForOperationCompleted(t){return new Promise((e,i)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(Mt(r=>r.nonce===t),Dn(1),ne(r=>{if(r.result!==void 0)return r.result;throw new Error(r.error)})).subscribe({next:e,error:i})})}get isEnabled(){return!!this.serviceWorker}},W_=(()=>{class n{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new Q;constructor(e){if(this.sw=e,!e.isEnabled){this.messages=wn,this.notificationClicks=wn,this.notificationCloses=wn,this.pushSubscriptionChanges=wn,this.subscription=wn;return}this.messages=this.sw.eventsOfType("PUSH").pipe(ne(r=>r.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(ne(r=>r.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(ne(r=>r.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(ne(r=>r.data)),this.pushManager=this.sw.registration.pipe(ne(r=>r.pushManager));let i=this.pushManager.pipe(Ii(r=>r.getSubscription()));this.subscription=new q(r=>{let o=i.subscribe(r),s=this.subscriptionChanges.subscribe(r);return()=>{o.unsubscribe(),s.unsubscribe()}})}requestSubscription(e){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(tp));let i={userVisibleOnly:!0},r=this.decodeBase64(e.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),o=new Uint8Array(new ArrayBuffer(r.length));for(let s=0;s<r.length;s++)o[s]=r.charCodeAt(s);return i.applicationServerKey=o,new Promise((s,a)=>{this.pushManager.pipe(Ii(c=>c.subscribe(i)),Dn(1)).subscribe({next:c=>{this.subscriptionChanges.next(c),s(c)},error:a})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(tp));let e=i=>{if(i===null)throw new _(5602,!1);return i.unsubscribe().then(r=>{if(!r)throw new _(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((i,r)=>{this.subscription.pipe(Dn(1),Ii(e)).subscribe({next:i,error:r})})}decodeBase64(e){return atob(e)}static \u0275fac=function(i){return new(i||n)(M(Zr))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),q_=(()=>{class n{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(e){if(this.sw=e,!e.isEnabled){this.versionUpdates=wn,this.unrecoverable=wn;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(tp));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let e=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:e},e).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new _(5601,!1));let e=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:e},e)}static \u0275fac=function(i){return new(i||n)(M(Zr))};static \u0275prov=P({token:n,factory:n.\u0275fac})}return n})(),K_=new w("");function GT(){let n=m(gs);if(!("serviceWorker"in navigator&&n.enabled!==!1))return;let t=m(K_),e=m(O),i=m(Qt);e.runOutsideAngular(()=>{let r=navigator.serviceWorker,o=()=>r.controller?.postMessage({action:"INITIALIZE"});r.addEventListener("controllerchange",o),i.onDestroy(()=>{r.removeEventListener("controllerchange",o)})}),e.runOutsideAngular(()=>{let r,{registrationStrategy:o}=n;if(typeof o=="function")r=new Promise(s=>o().subscribe(()=>s()));else{let[s,...a]=(o||"registerWhenStable:30000").split(":");switch(s){case"registerImmediately":r=Promise.resolve();break;case"registerWithDelay":r=G_(+a[0]||0);break;case"registerWhenStable":r=Promise.race([i.whenStable(),G_(+a[0])]);break;default:throw new _(5600,!1)}}r.then(()=>{i.destroyed||navigator.serviceWorker.register(t,{scope:n.scope,updateViaCache:n.updateViaCache,type:n.type}).catch(s=>console.error(Xn(5604,!1)))})})}function G_(n){return new Promise(t=>setTimeout(t,n))}function WT(){let n=m(gs),t=m(Ie),e=!0;return new Zr(e&&n.enabled!==!1?navigator.serviceWorker:void 0,t)}var gs=class{enabled;updateViaCache;type;scope;registrationStrategy};function qT(n,t={}){return ti([W_,q_,{provide:K_,useValue:n},{provide:gs,useValue:t},{provide:Zr,useFactory:WT},Bf(GT)])}var Q_=(()=>{class n{static register(e,i={}){return{ngModule:n,providers:[qT(e,i)]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({providers:[W_,q_]})}return n})();var r0=(()=>{class n{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||n)($(mt),$(re))};static \u0275dir=G({type:n})}return n})(),KT=(()=>{class n extends r0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=qi(n)))(r||n)}})();static \u0275dir=G({type:n,features:[tt]})}return n})(),Il=new w("");var QT={provide:Il,useExisting:_t(()=>xl),multi:!0};function ZT(){let n=Lt()?Lt().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var YT=new w(""),xl=(()=>{class n extends r0{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!ZT())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||n)($(mt),$(re),$(YT,8))};static \u0275dir=G({type:n,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&ie("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[nt([QT]),tt]})}return n})();function op(n){return n==null||sp(n)===0}function sp(n){return n==null?null:Array.isArray(n)||typeof n=="string"?n.length:n instanceof Set?n.size:null}var eo=new w(""),ap=new w(""),XT=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,ws=class{static min(t){return JT(t)}static max(t){return eA(t)}static required(t){return o0(t)}static requiredTrue(t){return tA(t)}static email(t){return nA(t)}static minLength(t){return iA(t)}static maxLength(t){return rA(t)}static pattern(t){return oA(t)}static nullValidator(t){return gl()}static compose(t){return u0(t)}static composeAsync(t){return f0(t)}};function JT(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e<n?{min:{min:n,actual:t.value}}:null}}function eA(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e>n?{max:{max:n,actual:t.value}}:null}}function o0(n){return op(n.value)?{required:!0}:null}function tA(n){return n.value===!0?null:{required:!0}}function nA(n){return op(n.value)||XT.test(n.value)?null:{email:!0}}function iA(n){return t=>{let e=t.value?.length??sp(t.value);return e===null||e===0?null:e<n?{minlength:{requiredLength:n,actualLength:e}}:null}}function rA(n){return t=>{let e=t.value?.length??sp(t.value);return e!==null&&e>n?{maxlength:{requiredLength:n,actualLength:e}}:null}}function oA(n){if(!n)return gl;let t,e;return typeof n=="string"?(e="",n.charAt(0)!=="^"&&(e+="^"),e+=n,n.charAt(n.length-1)!=="$"&&(e+="$"),t=new RegExp(e)):(e=n.toString(),t=n),i=>{if(op(i.value))return null;let r=i.value;return t.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function gl(n){return null}function s0(n){return n!=null}function a0(n){return Pr(n)?En(n):n}function c0(n){let t={};return n.forEach(e=>{t=e!=null?I(I({},t),e):t}),Object.keys(t).length===0?null:t}function l0(n,t){return t.map(e=>e(n))}function sA(n){return!n.validate}function d0(n){return n.map(t=>sA(t)?t:e=>t.validate(e))}function u0(n){if(!n)return null;let t=n.filter(s0);return t.length==0?null:function(e){return c0(l0(e,t))}}function cp(n){return n!=null?u0(d0(n)):null}function f0(n){if(!n)return null;let t=n.filter(s0);return t.length==0?null:function(e){let i=l0(e,t).map(a0);return lo(i).pipe(ne(c0))}}function lp(n){return n!=null?f0(d0(n)):null}function Z_(n,t){return n===null?[t]:Array.isArray(n)?[...n,t]:[n,t]}function m0(n){return n._rawValidators}function p0(n){return n._rawAsyncValidators}function np(n){return n?Array.isArray(n)?n:[n]:[]}function vl(n,t){return Array.isArray(n)?n.includes(t):n===t}function Y_(n,t){let e=np(t);return np(n).forEach(r=>{vl(e,r)||e.push(r)}),e}function X_(n,t){return np(t).filter(e=>!vl(n,e))}var yl=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=cp(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=lp(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,e){return this.control?this.control.hasError(t,e):!1}getError(t,e){return this.control?this.control.getError(t,e):null}},tr=class extends yl{name;get formDirective(){return null}get path(){return null}};var vs="VALID",hl="INVALID",Yr="PENDING",ys="DISABLED",mi=class{},bl=class extends mi{value;source;constructor(t,e){super(),this.value=t,this.source=e}},_s=class extends mi{pristine;source;constructor(t,e){super(),this.pristine=t,this.source=e}},Es=class extends mi{touched;source;constructor(t,e){super(),this.touched=t,this.source=e}},Xr=class extends mi{status;source;constructor(t,e){super(),this.status=t,this.source=e}},_l=class extends mi{source;constructor(t){super(),this.source=t}},Jr=class extends mi{source;constructor(t){super(),this.source=t}};function h0(n){return(Sl(n)?n.validators:n)||null}function aA(n){return Array.isArray(n)?cp(n):n||null}function g0(n,t){return(Sl(t)?t.asyncValidators:n)||null}function cA(n){return Array.isArray(n)?lp(n):n||null}function Sl(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function lA(n,t,e){let i=n.controls;if(!(t?Object.keys(i):i).length)throw new _(1e3,"");if(!v0(i,e))throw new _(1001,"")}function dA(n,t,e){n._forEachChild((i,r)=>{if(e[r]===void 0)throw new _(-1002,"")})}var El=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=xe(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,e){this._assignValidators(t),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return ht(this.statusReactive)}set status(t){ht(()=>this.statusReactive.set(t))}_status=Ue(()=>this.statusReactive());statusReactive=xe(void 0);get valid(){return this.status===vs}get invalid(){return this.status===hl}get pending(){return this.status===Yr}get disabled(){return this.status===ys}get enabled(){return this.status!==ys}errors;get pristine(){return ht(this.pristineReactive)}set pristine(t){ht(()=>this.pristineReactive.set(t))}_pristine=Ue(()=>this.pristineReactive());pristineReactive=xe(!0);get dirty(){return!this.pristine}get touched(){return ht(this.touchedReactive)}set touched(t){ht(()=>this.touchedReactive.set(t))}_touched=Ue(()=>this.touchedReactive());touchedReactive=xe(!1);get untouched(){return!this.touched}_events=new Q;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(Y_(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(Y_(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(X_(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(X_(t,this._rawAsyncValidators))}hasValidator(t){return vl(this._rawValidators,t)}hasAsyncValidator(t){return vl(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let e=this.touched===!1;this.touched=!0;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(ee(I({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new Es(!0,i))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(t))}markAsUntouched(t={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:i})}),t.onlySelf||this._parent?._updateTouched(t,i),e&&t.emitEvent!==!1&&this._events.next(new Es(!1,i))}markAsDirty(t={}){let e=this.pristine===!0;this.pristine=!1;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(ee(I({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new _s(!1,i))}markAsPristine(t={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,i),e&&t.emitEvent!==!1&&this._events.next(new _s(!0,i))}markAsPending(t={}){this.status=Yr;let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Xr(this.status,e)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(ee(I({},t),{sourceControl:e}))}disable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=ys,this.errors=null,this._forEachChild(r=>{r.disable(ee(I({},t),{onlySelf:!0}))}),this._updateValue();let i=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new bl(this.value,i)),this._events.next(new Xr(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(ee(I({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=vs,this._forEachChild(i=>{i.enable(ee(I({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(ee(I({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(t,e){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===vs||this.status===Yr)&&this._runAsyncValidator(i,t.emitEvent)}let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new bl(this.value,e)),this._events.next(new Xr(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(ee(I({},t),{sourceControl:e}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ys:vs}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,e){if(this.asyncValidator){this.status=Yr,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:t!==!1};let i=a0(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,e={}){this.errors=t,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(t){let e=t;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(t,e){let i=e?this.get(e):this;return i?.errors?i.errors[t]:null}hasError(t,e){return!!this.getError(t,e)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,e,i){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||i)&&this._events.next(new Xr(this.status,e)),this._parent&&this._parent._updateControlsErrors(t,e,i)}_initObservables(){this.valueChanges=new ve,this.statusChanges=new ve}_calculateStatus(){return this._allControlsDisabled()?ys:this.errors?hl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Yr)?Yr:this._anyControlsHaveStatus(hl)?hl:vs}_anyControlsHaveStatus(t){return this._anyControls(e=>e.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,t.onlySelf||this._parent?._updatePristine(t,e),r&&this._events.next(new _s(this.pristine,e))}_updateTouched(t={},e){this.touched=this._anyControlsTouched(),this._events.next(new Es(this.touched,e)),t.onlySelf||this._parent?._updateTouched(t,e)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Sl(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=aA(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=cA(this._rawAsyncValidators)}_updateHasRequiredValidator(){ht(()=>this._hasRequired.set(this.hasValidator(ws.required)))}};function v0(n,t){return Object.hasOwn(n,t)}function uA(n){return n.tagName==="INPUT"||n.tagName==="SELECT"||n.tagName==="TEXTAREA"}function fA(n,t,e,i){switch(e){case"name":n.setAttribute(t,e,i);break;case"disabled":case"readonly":case"required":i?n.setAttribute(t,e,""):n.removeAttribute(t,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?n.setAttribute(t,e,i.toString()):n.removeAttribute(t,e);break}}var ip=class{kind;context;control;message;constructor({kind:t,context:e,control:i}){this.kind=t,this.context=e,this.control=i}};var mA=(()=>{class n{_validator=gl;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):gl,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,features:[Ft]})}return n})();var pA={provide:eo,useExisting:_t(()=>y0),multi:!0};var y0=(()=>{class n extends mA{required;inputName="required";normalizeInput=pe;createValidator=e=>o0;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qi(n)))(r||n)}})();static \u0275dir=G({type:n,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&Se("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[nt([pA]),tt]})}return n})();var hA=new w(""),Ml=new w("",{factory:()=>dp}),dp="always";function gA(n,t){return[...t.path,n]}function J_(n,t,e=dp){up(n,t),t.valueAccessor.writeValue(n.value),(n.disabled||e==="always")&&t.valueAccessor.setDisabledState?.(n.disabled),yA(n,t),_A(n,t),bA(n,t),vA(n,t)}function e0(n,t,e=!0){let i=()=>{};t?.valueAccessor?.registerOnChange(i),t?.valueAccessor?.registerOnTouched(i),Dl(n,t),n&&(t._invokeOnDestroyCallbacks(),n._registerOnCollectionChange(()=>{}))}function wl(n,t){n.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(t)})}function vA(n,t){if(t.valueAccessor.setDisabledState){let e=i=>{t.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(e),t._registerOnDestroy(()=>{n._unregisterOnDisabledChange(e)})}}function up(n,t){let e=m0(n);t.validator!==null?n.setValidators(Z_(e,t.validator)):typeof e=="function"&&n.setValidators([e]);let i=p0(n);t.asyncValidator!==null?n.setAsyncValidators(Z_(i,t.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();wl(t._rawValidators,r),wl(t._rawAsyncValidators,r)}function Dl(n,t){let e=!1;if(n!==null){if(t.validator!==null){let r=m0(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.validator);o.length!==r.length&&(e=!0,n.setValidators(o))}}if(t.asyncValidator!==null){let r=p0(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.asyncValidator);o.length!==r.length&&(e=!0,n.setAsyncValidators(o))}}}let i=()=>{};return wl(t._rawValidators,i),wl(t._rawAsyncValidators,i),e}function yA(n,t){t.valueAccessor.registerOnChange(e=>{n._pendingValue=e,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&b0(n,t)})}function bA(n,t){t.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&b0(n,t),n.updateOn!=="submit"&&n.markAsTouched()})}function b0(n,t){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function _A(n,t){let e=(i,r)=>{t.valueAccessor.writeValue(i),r&&t.viewToModelUpdate(i)};n.registerOnChange(e),t._registerOnDestroy(()=>{n._unregisterOnChange(e)})}function _0(n,t){n==null,up(n,t)}function EA(n,t){return Dl(n,t)}function wA(n,t){if(!n.hasOwnProperty("model"))return!1;let e=n.model;return e.isFirstChange()?!0:!Object.is(t,e.currentValue)}function DA(n){return Object.getPrototypeOf(n.constructor)===KT}function E0(n,t){n._syncPendingControls(),t.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function CA(n,t){if(!t)return null;Array.isArray(t);let e,i,r;return t.forEach(o=>{o.constructor===xl?e=o:DA(o)?i=o:r=o}),r||i||e||null}function IA(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}var xA={provide:hA,useFactory:()=>{let n=m(pi,{self:!0});return{setParseErrors:t=>{n.setParseErrorSource(t)},set onReset(t){n.onReset=t}}}},pi=class extends yl{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(t){this.userOnReset=t,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Jr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=CA(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(t,e,i){super(),this.injector=t,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(fn)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let t=this.injector?.get(vn);if(!this.control||!t)return;let e=t.markForCheck.bind(t);this.subscription=new Ne,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Jr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(t){!t.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!t.customControl||(this.isCustomControlBased=!0,t.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),t.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=uA(t.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof y0))}ngControlUpdate(t,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,t.setCustomControlModelInput(i.value)),this.bindControlProperty(t,r,"touched",i.touched),this.bindControlProperty(t,r,"dirty",i.dirty),this.bindControlProperty(t,r,"valid",i.valid),this.bindControlProperty(t,r,"invalid",i.invalid),this.bindControlProperty(t,r,"pending",i.pending),this.bindControlProperty(t,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(t,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);t.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(t,e,i,r){if(e[i]===r)return;e[i]=r;let o=t.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&fA(this.renderer,t.nativeElement,i,r)}_convertErrors(t){if(t===null)return[];let e=this.control;return Object.entries(t).map(([i,r])=>new ip({context:r,kind:i,control:e}))}setParseErrorSource(t){if(t===void 0)return;let e=null,i=Ue(()=>{let r=t();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Ui(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(t){this.parseErrorsValidator&&(t?.removeValidators(this.parseErrorsValidator),t?.updateValueAndValidity({emitEvent:!1}))}},rp=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var w0=(()=>{class n extends rp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||n)($(pi,2))};static \u0275dir=G({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&W("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[tt]})}return n})();var Cl=class extends El{constructor(t,e,i){super(h0(e),g0(i,e)),this.controls=t,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,e){let i=this._find(t);return i||(this.controls[t]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(t,e,i={}){this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(t,e={}){let i=this._find(t);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(t,e,i={}){let r=this._find(t);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[t],e&&this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(t){return this._find(t)?.enabled===!0}setValue(t,e={}){ht(()=>{dA(this,!0,t),Object.keys(t).forEach(i=>{lA(this,!0,i),this.controls[i].setValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(t,e={}){t!=null&&(Object.keys(t).forEach(i=>{let r=this._find(i);r&&r.patchValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(t={},e={}){this._forEachChild((i,r)=>{i.reset(t?t[r]:null,ee(I({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Jr(this))}getRawValue(){return this._reduceChildren({},(t,e,i)=>(t[i]=e.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&t(i,e)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&t(i))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(t,e){let i=t;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return v0(this.controls,t)?this.controls[t]:null}};var SA={provide:tr,useExisting:_t(()=>fp)},bs=Promise.resolve(),fp=(()=>{class n extends tr{callSetDisabledState;get submitted(){return ht(this.submittedReactive)}_submitted=Ue(()=>this.submittedReactive());submittedReactive=xe(!1);_directives=new Set;form;ngSubmit=new ve;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Cl({},cp(e),lp(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){bs.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){bs.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){bs.then(()=>{let i=this._findContainer(e.path),r=new Cl({});_0(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){bs.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){bs.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),E0(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new _l(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||n)($(eo,10),$(ap,10),$(Ml,8))};static \u0275dir=G({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&ie("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[nt([SA]),tt]})}return n})();function t0(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function n0(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var D0=class extends El{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,e,i){super(h0(e),g0(i,e)),this._applyFormState(t),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Sl(e)&&(e.nonNullable||e.initialValueIsDefault)&&(n0(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,e={}){ht(()=>{this.value=this._pendingValue=t,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(t,e={}){this.setValue(t,e)}reset(t=this.defaultValue,e={}){this._applyFormState(t),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Jr(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){t0(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){t0(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){n0(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var MA=n=>n instanceof D0;var TA={provide:pi,useExisting:_t(()=>mp)},i0=Promise.resolve(),mp=(()=>{class n extends pi{_changeDetectorRef;callSetDisabledState;control=new D0;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new ve;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),wA(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,J_(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,J_(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){i0.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&pe(i);i0.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?gA(e,this._parent):[e]}static \u0275fac=function(i){return new(i||n)($(tr,9),$(eo,10),$(ap,10),$(Il,10),$(vn,8),$(Ml,8),$(Ie,8),$(mt,8))};static \u0275dir=G({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[nt([TA,xA]),tt,Ft,Uf(null)]})}return n})();var AA=(()=>{class n extends tr{callSetDisabledState;get submitted(){return ht(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Ue(()=>this._submittedReactive());_submittedReactive=xe(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Dl(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){e0(e.control||null,e,!1),IA(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,E0(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new _l(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(e0(i||null,e),MA(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);_0(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&EA(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){up(this.form,this),this._oldForm&&Dl(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||n)($(eo,10),$(ap,10),$(Ml,8))};static \u0275dir=G({type:n,features:[tt,Ft]})}return n})();var NA={provide:tr,useExisting:_t(()=>pp)},pp=(()=>{class n extends AA{form=null;ngSubmit=new ve;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qi(n)))(r||n)}})();static \u0275dir=G({type:n,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&ie("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[nt([NA]),tt]})}return n})();var kA=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({})}return n})();var C0=(()=>{class n{static withConfig(e){return{ngModule:n,providers:[{provide:Ml,useValue:e.callSetDisabledState??dp}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[kA]})}return n})();function Ds(n){return n.buttons===0||n.detail===0}function Cs(n){let t=n.touches&&n.touches[0]||n.changedTouches&&n.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var hp;function I0(){if(hp==null){let n=typeof document<"u"?document.head:null;hp=!!(n&&(n.createShadowRoot||n.attachShadow))}return hp}function gp(n){if(I0()){let t=n.getRootNode?n.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function en(n){return n.composedPath?n.composedPath()[0]:n.target}var vp;try{vp=typeof Intl<"u"&&Intl.v8BreakIterator}catch(n){vp=!1}var rt=(()=>{class n{_platformId=m(Bi);isBrowser=this._platformId?jb(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||vp)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var Is;function x0(){if(Is==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Is=!0}))}finally{Is=Is||!1}return Is}function to(n){return x0()?n:!!n.capture}function tn(n){return n instanceof re?n.nativeElement:n}var S0=new w("cdk-input-modality-detector-options"),M0={ignoreKeys:[18,17,224,91,16]},T0=650,yp={passive:!0,capture:!0},A0=(()=>{class n{_platform=m(rt);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new wi(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=en(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<T0||(this._modality.next(Ds(e)?"keyboard":"mouse"),this._mostRecentTarget=en(e))};_onTouchstart=e=>{if(Cs(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=en(e)};constructor(){let e=m(O),i=m(Z),r=m(S0,{optional:!0});if(this._options=I(I({},M0),r),this.modalityDetected=this._modality.pipe(hd(1)),this.modalityChanged=this.modalityDetected.pipe(ud()),this._platform.isBrowser){let o=m(at).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,yp),o.listen(i,"mousedown",this._onMousedown,yp),o.listen(i,"touchstart",this._onTouchstart,yp)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})(),xs=(function(n){return n[n.IMMEDIATE=0]="IMMEDIATE",n[n.EVENTUAL=1]="EVENTUAL",n})(xs||{}),N0=new w("cdk-focus-monitor-default-options"),Tl=to({passive:!0,capture:!0}),nr=(()=>{class n{_ngZone=m(O);_platform=m(rt);_inputModalityDetector=m(A0);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(Z);_stopInputModalityDetector=new Q;constructor(){let e=m(N0,{optional:!0});this._detectionMode=e?.detectionMode||xs.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=en(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=tn(e);if(!this._platform.isBrowser||r.nodeType!==1)return lt();let o=gp(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new Q,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=tn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=tn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===xs.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===xs.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?T0:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=en(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Tl),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Tl)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(xi(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Tl),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Tl),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var Al=new WeakMap,Bt=(()=>{class n{_appRef;_injector=m(Ie);_environmentInjector=m(Ge);load(e){let i=this._appRef=this._appRef||this._injector.get(Qt),r=Al.get(i);r||(r={loaders:new Set,refs:[]},Al.set(i,r),i.onDestroy(()=>{Al.get(i)?.refs.forEach(o=>o.destroy()),Al.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(ob(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var k0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return n})(),Nl;function FA(){if(Nl===void 0&&(Nl=null,typeof window<"u")){let n=window;n.trustedTypes!==void 0&&(Nl=n.trustedTypes.createPolicy("angular#components",{createHTML:t=>t}))}return Nl}function no(n){return FA()?.createHTML(n)||n}var R0=new Set,ir,bp=(()=>{class n{_platform=m(rt);_nonce=m(Hi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):PA}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&OA(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();function OA(n,t){if(!R0.has(n))try{ir||(ir=document.createElement("style"),t&&ir.setAttribute("nonce",t),ir.setAttribute("type","text/css"),document.head.appendChild(ir)),ir.sheet&&(ir.sheet.insertRule(`@media ${n} {body{ }}`,0),R0.add(n))}catch(e){console.error(e)}}function PA(n){return{matches:n==="all"||n==="",media:n,addListener:()=>{},removeListener:()=>{}}}var LA=(()=>{class n{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var F0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({providers:[LA]})}return n})();var O0=new Map,Ht=class n{_appId=m(ji);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,e=!1){this._appId!=="ng"&&(t+=this._appId);let i=O0.get(t);return i===void 0?i=0:i++,O0.set(t,i),`${t}${e?n._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})};var io,P0=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function _p(){if(io)return io;if(typeof document!="object"||!document)return io=new Set(P0),io;let n=document.createElement("input");return io=new Set(P0.filter(t=>(n.setAttribute("type",t),n.type===t))),io}var VA=new w("MATERIAL_ANIMATIONS"),L0=null;function jA(){return m(VA,{optional:!0})?.animationsDisabled||m(xr,{optional:!0})==="NoopAnimations"?"di-disabled":(L0??=m(bp).matchMedia("(prefers-reduced-motion)").matches,L0?"reduced-motion":"enabled")}function Ut(){return jA()!=="enabled"}function ro(n){return n!=null&&`${n}`!="false"}var $t=(function(n){return n[n.FADING_IN=0]="FADING_IN",n[n.VISIBLE=1]="VISIBLE",n[n.FADING_OUT=2]="FADING_OUT",n[n.HIDDEN=3]="HIDDEN",n})($t||{}),Ep=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=$t.HIDDEN;constructor(t,e,i,r=!1){this._renderer=t,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},V0=to({passive:!0,capture:!0}),wp=class{_events=new Map;addHandler(t,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),t.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,V0)})}removeHandler(t,e,i){let r=this._events.get(t);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,V0)))}_delegateEventHandler=t=>{let e=en(t);e&&this._events.get(t.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(t))})}},Ss={enterDuration:225,exitDuration:150},HA=800,j0=to({passive:!0,capture:!0}),B0=["mousedown","touchstart"],H0=["mouseup","mouseleave","touchend","touchcancel"],UA=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return n})(),Ms=class n{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new wp;constructor(t,e,i,r,o){this._target=t,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=tn(i)),o&&o.get(Bt).load(UA)}fadeInRipple(t,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=I(I({},Ss),i.animation);i.centered&&(t=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||$A(t,e,r),a=t-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),h=u.transitionProperty,f=u.transitionDuration,v=h==="none"||f==="0s"||f==="0s, 0s"||r.width===0&&r.height===0,E=new Ep(this,d,i,v);d.style.transform="scale3d(1, 1, 1)",E.state=$t.FADING_IN,i.persistent||(this._mostRecentTransientRipple=E);let D=null;return!v&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let x=()=>{D&&(D.fallbackTimer=null),clearTimeout(ye),this._finishRippleTransition(E)},X=()=>this._destroyRipple(E),ye=setTimeout(X,l+100);d.addEventListener("transitionend",x),d.addEventListener("transitioncancel",X),D={onTransitionEnd:x,onTransitionCancel:X,fallbackTimer:ye}}),this._activeRipples.set(E,D),(v||!l)&&this._finishRippleTransition(E),E}fadeOutRipple(t){if(t.state===$t.FADING_OUT||t.state===$t.HIDDEN)return;let e=t.element,i=I(I({},Ss),t.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",t.state=$t.FADING_OUT,(t._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let e=tn(t);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,B0.forEach(i=>{n._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{H0.forEach(e=>{this._triggerElement.addEventListener(e,this,j0)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===$t.FADING_IN?this._startFadeOutTransition(t):t.state===$t.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let e=t===this._mostRecentTransientRipple,{persistent:i}=t.config;t.state=$t.VISIBLE,!i&&(!e||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let e=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=$t.HIDDEN,e!==null&&(t.element.removeEventListener("transitionend",e.onTransitionEnd),t.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),t.element.remove()}_onMousedown(t){let e=Ds(t),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+HA;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!Cs(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=t.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let e=t.state===$t.VISIBLE||t.config.terminateOnPointerUp&&t.state===$t.FADING_IN;!t.config.persistent&&e&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&(B0.forEach(e=>n._eventManager.removeHandler(e,t,this)),this._pointerUpEventsRegistered&&(H0.forEach(e=>t.removeEventListener(e,this,j0)),this._pointerUpEventsRegistered=!1))}};function $A(n,t,e){let i=Math.max(Math.abs(n-e.left),Math.abs(n-e.right)),r=Math.max(Math.abs(t-e.top),Math.abs(t-e.bottom));return Math.sqrt(i*i+r*r)}var Ts=new w("mat-ripple-global-options"),U0=(()=>{class n{_elementRef=m(re);_animationsDisabled=Ut();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=m(O),i=m(rt),r=m(Ts,{optional:!0}),o=m(Ie);this._globalOptions=r||{},this._rippleRenderer=new Ms(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:I(I(I({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,I(I({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,I(I({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&W("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return n})();var zA={capture:!0},GA=["focus","mousedown","mouseenter","touchstart"],Dp="mat-ripple-loader-uninitialized",Cp="mat-ripple-loader-class-name",$0="mat-ripple-loader-centered",kl="mat-ripple-loader-disabled",Rl=(()=>{class n{_document=m(Z);_animationsDisabled=Ut();_globalRippleOptions=m(Ts,{optional:!0});_platform=m(rt);_ngZone=m(O);_injector=m(Ie);_eventCleanups;_hosts=new Map;constructor(){let e=m(at).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>GA.map(i=>e.listen(this._document,i,this._onInteraction,zA)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Dp,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Cp))&&e.setAttribute(Cp,i.className||""),i.centered&&e.setAttribute($0,""),i.disabled&&e.setAttribute(kl,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(kl,""):e.removeAttribute(kl)}_onInteraction=e=>{let i=en(e);if(i instanceof HTMLElement){let r=i.closest(`[${Dp}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Cp)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ss.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ss.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(kl),rippleConfig:{centered:e.hasAttribute($0),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Ms(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Dp)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var rr=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2})}return n})();var WA=new w("MAT_BUTTON_CONFIG");function z0(n){return n==null?void 0:Br(n)}var G0=(()=>{class n{_elementRef=m(re);_ngZone=m(O);_animationsDisabled=Ut();_config=m(WA,{optional:!0});_focusMonitor=m(nr);_cleanupClick;_renderer=m(mt);_rippleLoader=m(Rl);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=eb(!1,{transform:pe});constructor(){m(Bt).load(rr);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(Se("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),pt(r.color?"mat-"+r.color:""),W("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",pe],disabled:[2,"disabled","disabled",pe],ariaDisabled:[2,"aria-disabled","ariaDisabled",pe],disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe],tabIndex:[2,"tabIndex","tabIndex",z0],_tabindex:[2,"tabindex","_tabindex",z0],showProgress:[1,"showProgress"]}})}return n})();var qA=new w("cdk-dir-doc",{providedIn:"root",factory:()=>m(Z)}),KA=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function W0(n){let t=n?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?KA.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var Fl=(()=>{class n{get value(){return this.valueSignal()}valueSignal=xe("ltr");change=new ve;constructor(){let e=m(qA,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(W0(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var $e=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({})}return n})();var Ol=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[$e]})}return n})();var QA=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],ZA=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function YA(n,t){n&1&&(Ot(0,"div",2),ae(1,3),Pt())}var q0=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),K0=(()=>{class n extends G0{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=XA(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?q0.get(this._appearance):null,o=q0.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[tt],ngContentSelectors:ZA,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ke(QA),hn(0,"span",0),ae(1),Ot(2,"span",1),ae(3,1),Pt(),ae(4,2),Ee(5,YA,2,0,"div",2),hn(6,"span",3)(7,"span",4)),i&2&&(W("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),b(5),we(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return n})();function XA(n){return n.hasAttribute("mat-raised-button")?"elevated":n.hasAttribute("mat-stroked-button")?"outlined":n.hasAttribute("mat-flat-button")?"filled":n.hasAttribute("mat-button")?"text":null}var Q0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[Ol,$e]})}return n})();var eN=["*"];var tN=new w("MAT_CARD_CONFIG"),Z0=(()=>{class n{appearance;constructor(){let e=m(tN,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&W("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:eN,decls:1,vars:0,template:function(i,r){i&1&&(Ke(),ae(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return n})();var Y0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[$e]})}return n})();var Pl=(()=>{class n{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var Ll=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(t,e,i,r,o){this._defaultMatcher=t,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let t=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==t&&(this.errorState=o,this._stateChanges.next())}};var Ip=class{_box;_destroyed=new Q;_resizeSubject=new Q;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new q(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),i.unsubscribe(),this._elementObservables.delete(t)}}).pipe(Mt(e=>e.some(i=>i.target===t)),pd({bufferSize:1,refCount:!0}),xi(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},X0=(()=>{class n{_cleanupErrorListener;_observers=new Map;_ngZone=m(O);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Ip(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var iN=["notch"],rN=["*"],J0=["iconPrefixContainer"],eE=["textPrefixContainer"],tE=["iconSuffixContainer"],nE=["textSuffixContainer"],oN=["textField"],sN=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],aN=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function cN(n,t){n&1&&Re(0,"span",21)}function lN(n,t){if(n&1&&(g(0,"label",20),ae(1,1),Ee(2,cN,1,0,"span",21),p()),n&2){let e=H(2);N("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),Se("for",e._control.disableAutomaticLabeling?null:e._control.id),b(2),we(!e.hideRequiredMarker&&e._control.required?2:-1)}}function dN(n,t){if(n&1&&Ee(0,lN,3,5,"label",20),n&2){let e=H();we(e._hasFloatingLabel()?0:-1)}}function uN(n,t){n&1&&Re(0,"div",7)}function fN(n,t){}function mN(n,t){if(n&1&&me(0,fN,0,0,"ng-template",13),n&2){H(2);let e=Vr(1);N("ngTemplateOutlet",e)}}function pN(n,t){if(n&1&&(g(0,"div",9),Ee(1,mN,1,1,null,13),p()),n&2){let e=H();N("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),b(),we(e._forceDisplayInfixLabel()?-1:1)}}function hN(n,t){n&1&&(g(0,"div",10,2),ae(2,2),p())}function gN(n,t){n&1&&(g(0,"div",11,3),ae(2,3),p())}function vN(n,t){}function yN(n,t){if(n&1&&me(0,vN,0,0,"ng-template",13),n&2){H();let e=Vr(1);N("ngTemplateOutlet",e)}}function bN(n,t){n&1&&(g(0,"div",14,4),ae(2,4),p())}function _N(n,t){n&1&&(g(0,"div",15,5),ae(2,5),p())}function EN(n,t){n&1&&Re(0,"div",16)}function wN(n,t){n&1&&(g(0,"div",18),ae(1,6),p())}function DN(n,t){if(n&1&&(g(0,"mat-hint",22),y(1),p()),n&2){let e=H(2);N("id",e._hintLabelId),b(),T(e.hintLabel)}}function CN(n,t){if(n&1&&(g(0,"div",19),Ee(1,DN,2,2,"mat-hint",22),ae(2,7),Re(3,"div",23),ae(4,8),p()),n&2){let e=H();b(),we(e.hintLabel?1:-1)}}var As=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["mat-label"]]})}return n})(),IN=new w("MatError");var xp=(()=>{class n{align="start";id=m(Ht).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(gn("id",r.id),Se("align",null),W("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return n})(),xN=new w("MatPrefix");var SN=new w("MatSuffix");var lE=new w("FloatingLabelParent"),iE=(()=>{class n{_elementRef=m(re);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(X0);_ngZone=m(O);_parent=m(lE);_resizeSubscription=new Ne;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return MN(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&W("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return n})();function MN(n){let t=n;if(t.offsetParent!==null)return t.scrollWidth;let e=t.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var rE="mdc-line-ripple--active",Vl="mdc-line-ripple--deactivating",oE=(()=>{class n{_elementRef=m(re);_cleanupTransitionEnd;constructor(){let e=m(O),i=m(mt);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Vl),e.add(rE)}deactivate(){this._elementRef.nativeElement.classList.add(Vl)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Vl);e.propertyName==="opacity"&&r&&i.remove(rE,Vl)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return n})(),sE=(()=>{class n{_elementRef=m(re);_ngZone=m(O);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Pn(iN,5),i&2){let o;de(o=ue())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&W("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:rN,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ke(),hn(0,"div",1),Ot(1,"div",2,0),ae(3),Pt(),hn(4,"div",3))},encapsulation:2})}return n})(),Sp=(()=>{class n{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n})}return n})();var Mp=new w("MatFormField"),TN=new w("MAT_FORM_FIELD_DEFAULT_OPTIONS"),aE="fill",AN="auto",cE="fixed",NN="translateY(-50%)",jl=(()=>{class n{_elementRef=m(re);_changeDetectorRef=m(vn);_platform=m(rt);_idGenerator=m(Ht);_ngZone=m(O);_defaults=m(TN,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Qo("iconPrefixContainer");_textPrefixContainerSignal=Qo("textPrefixContainer");_iconSuffixContainerSignal=Qo("iconSuffixContainer");_textSuffixContainerSignal=Qo("textSuffixContainer");_prefixSuffixContainers=Ue(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=tb(As);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=ro(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||AN}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||aE;this._appearanceSignal.set(i)}_appearanceSignal=xe(aE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||cE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||cE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new Q;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ut();constructor(){let e=this._defaults,i=m(Fl);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Ui(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Ue(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(gd([void 0,void 0]),ne(()=>[i.errorState,i.userAriaDescribedBy]),fd(),Mt(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(xi(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),uo(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){rb({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Ue(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,v=`var(--mat-mdc-form-field-label-transform, ${NN} translateX(${f}))`,E=s+a+c+l;return[v,E]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(bc(o,r._labelChild,As,5),Lr(o,Sp,5)(o,xN,5)(o,SN,5)(o,IN,5)(o,xp,5)),i&2){Ec();let s;de(s=ue())&&(r._formFieldControl=s.first),de(s=ue())&&(r._prefixChildren=s),de(s=ue())&&(r._suffixChildren=s),de(s=ue())&&(r._errorChildren=s),de(s=ue())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(_c(r._iconPrefixContainerSignal,J0,5)(r._textPrefixContainerSignal,eE,5)(r._iconSuffixContainerSignal,tE,5)(r._textSuffixContainerSignal,nE,5),Pn(oN,5)(J0,5)(eE,5)(tE,5)(nE,5)(iE,5)(sE,5)(oE,5)),i&2){Ec(4);let o;de(o=ue())&&(r._textField=o.first),de(o=ue())&&(r._iconPrefixContainer=o.first),de(o=ue())&&(r._textPrefixContainer=o.first),de(o=ue())&&(r._iconSuffixContainer=o.first),de(o=ue())&&(r._textSuffixContainer=o.first),de(o=ue())&&(r._floatingLabel=o.first),de(o=ue())&&(r._notchedOutline=o.first),de(o=ue())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&W("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[nt([{provide:Mp,useExisting:n},{provide:lE,useExisting:n}])],ngContentSelectors:aN,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ke(sN),me(0,dN,1,1,"ng-template",null,0,qf),g(2,"div",6,1),ie("click",function(s){return r._control.onContainerClick(s)}),Ee(4,uN,1,0,"div",7),g(5,"div",8),Ee(6,pN,2,2,"div",9),Ee(7,hN,3,0,"div",10),Ee(8,gN,3,0,"div",11),g(9,"div",12),Ee(10,yN,1,1,null,13),ae(11),p(),Ee(12,bN,3,0,"div",14),Ee(13,_N,3,0,"div",15),p(),Ee(14,EN,1,0,"div",16),p(),g(15,"div",17),Ee(16,wN,2,0,"div",18)(17,CN,5,1,"div",19),p()),i&2){let o;b(2),W("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),b(2),we(!r._hasOutline()&&!r._control.disabled?4:-1),b(2),we(r._hasOutline()?6:-1),b(),we(r._hasIconPrefix?7:-1),b(),we(r._hasTextPrefix?8:-1),b(2),we(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),b(2),we(r._hasTextSuffix?12:-1),b(),we(r._hasIconSuffix?13:-1),b(),we(r._hasOutline()?-1:14),b(),W("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();b(),we((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[iE,sE,pm,oE,xp],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return n})();var kN=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],RN=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function FN(n,t){n&1&&(g(0,"span",3),ae(1,1),p())}function ON(n,t){n&1&&(g(0,"span",6),ae(1,2),p())}var PN=new w("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),dE=new w("MatChipAvatar"),uE=new w("MatChipTrailingIcon"),fE=new w("MatChipEdit"),mE=new w("MatChipRemove"),hE=new w("MatChip"),gE=(()=>{class n{_elementRef=m(re);_parentChip=m(hE);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){m(Bt).load(rr),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(Se("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),W("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",pe],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:Br(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return n})(),LN=(()=>{class n extends gE{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qi(n)))(r||n)}})();static \u0275dir=G({type:n,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&ie("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Se("tabindex",r._getTabindex()),W("mdc-evolution-chip__action--presentational",!1))},features:[tt]})}return n})();var vE=(()=>{class n{_changeDetectorRef=m(vn);_elementRef=m(re);_tagName=m(Jy);_ngZone=m(O);_focusMonitor=m(nr);_globalRippleOptions=m(Ts,{optional:!0});_document=m(Z);_onFocus=new Q;_onBlur=new Q;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Ut();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=m(Ht).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new ve;destroyed=new ve;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=m(Rl);_injector=m(Ie);constructor(){let e=m(Bt);e.load(rr),e.load(k0),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=uo(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe(),this.destroyed.emit({chip:this}),this.destroyed.complete()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&Lr(o,dE,5)(o,fE,5)(o,uE,5)(o,mE,5)(o,dE,5)(o,uE,5)(o,fE,5)(o,mE,5),i&2){let s;de(s=ue())&&(r.leadingIcon=s.first),de(s=ue())&&(r.editIcon=s.first),de(s=ue())&&(r.trailingIcon=s.first),de(s=ue())&&(r.removeIcon=s.first),de(s=ue())&&(r._allLeadingIcons=s),de(s=ue())&&(r._allTrailingIcons=s),de(s=ue())&&(r._allEditIcons=s),de(s=ue())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Pn(LN,5),i&2){let o;de(o=ue())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&ie("keydown",function(s){return r._handleKeydown(s)}),i&2&&(gn("id",r.id),Se("role",r.role)("aria-label",r.ariaLabel),pt("mat-"+(r.color||"primary")),W("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",pe],highlighted:[2,"highlighted","highlighted",pe],disableRipple:[2,"disableRipple","disableRipple",pe],disabled:[2,"disabled","disabled",pe]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[nt([{provide:hE,useExisting:n}])],ngContentSelectors:RN,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Ke(kN),Re(0,"span",0),g(1,"span",1)(2,"span",2),Ee(3,FN,2,0,"span",3),g(4,"span",4),ae(5),Re(6,"span",5),p()()(),Ee(7,ON,2,0,"span",6)),i&2&&(b(3),we(r.leadingIcon?3:-1),b(4),we(r._hasTrailingIcon()?7:-1))},dependencies:[gE],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return n})();var yE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({providers:[Pl,{provide:PN,useValue:{separatorKeyCodes:[13]}}],imports:[Ol,$e]})}return n})();var Ns=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[F0,jl,$e]})}return n})();function bE(n){return Error(`Unable to find icon with the name "${n}"`)}function BN(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function _E(n){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${n}".`)}function EE(n){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${n}".`)}var $n=class{url;svgText;options;svgElement=null;constructor(t,e,i){this.url=t,this.svgText=e,this.options=i}},DE=(()=>{class n{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new $n(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(ct.HTML,r);if(!s)throw EE(r);let a=no(s);return this._addSvgIconConfig(e,i,new $n("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new $n(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(ct.HTML,i);if(!o)throw EE(i);let s=no(o);return this._addSvgIconSetConfig(e,new $n("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(ct.RESOURCE_URL,e);if(!i)throw _E(e);let r=this._cachedIconsByUrl.get(i);return r?lt(Bl(r)):this._loadSvgIconFromConfig(new $n(e,null)).pipe(po(o=>this._cachedIconsByUrl.set(i,o)),ne(o=>Bl(o)))}getNamedSvgIcon(e,i=""){let r=wE(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):cd(bE(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?lt(Bl(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ne(i=>Bl(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return lt(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Qn(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(ct.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),lt(null)})));return lo(o).pipe(ne(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw bE(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(po(i=>e.svgText=i),ne(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?lt(null):this._fetchIcon(e).pipe(po(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(no("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(no("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw BN();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(ct.RESOURCE_URL,i);if(!s)throw _E(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ne(l=>no(l)),fo(()=>this._inProgressUrlFetches.delete(s)),mo());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(wE(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return HN(o)?new $n(o.url,null,o.options):new $n(o,null)}}static \u0275fac=function(i){return new(i||n)(M(zr,8),M(Cm),M(Z,8),M(dt))};static \u0275prov=P({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Bl(n){return n.cloneNode(!0)}function wE(n,t){return n+":"+t}function HN(n){return!!(n.url&&n.options)}var UN=["*"],$N=new w("MAT_ICON_DEFAULT_OPTIONS"),zN=new w("mat-icon-location",{providedIn:"root",factory:()=>{let n=m(Z),t=n?n.location:null;return{getPathname:()=>t?t.pathname+t.search:""}}}),CE=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],GN=CE.map(n=>`[${n}]`).join(", "),WN=/^url\(['"]?#(.*?)['"]?\)$/,IE=(()=>{class n{_elementRef=m(re);_iconRegistry=m(DE);_location=m(zN);_errorHandler=m(dt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=Ne.EMPTY;constructor(){let e=m(new jr("aria-hidden"),{optional:!0}),i=m($N,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(GN),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)CE.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(WN):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Dn(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(Se("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),pt(r.color?"mat-"+r.color:""),W("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",pe],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:UN,decls:1,vars:0,template:function(i,r){i&1&&(Ke(),ae(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return n})(),xE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[$e]})}return n})();var KN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return n})(),QN={passive:!0},SE=(()=>{class n{_platform=m(rt);_ngZone=m(O);_renderer=m(at).createRenderer(null,null);_styleLoader=m(Bt);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Di;this._styleLoader.load(KN);let i=tn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new Q,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,QN)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=tn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Y({token:n,factory:n.\u0275fac})}return n})();var ME=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({})}return n})();var TE=new w("MAT_INPUT_VALUE_ACCESSOR");var ZN=["button","checkbox","file","hidden","image","radio","range","reset","submit"],YN=new w("MAT_INPUT_CONFIG"),AE=(()=>{class n{_elementRef=m(re);_platform=m(rt);ngControl=m(pi,{optional:!0,self:!0});_autofillMonitor=m(SE);_ngZone=m(O);_formField=m(Mp,{optional:!0});_renderer=m(mt);_uid=m(Ht).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=m(YN,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new Q;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=ro(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(ws.required)??!1}set required(e){this._required=ro(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&_p().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=ro(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>_p().has(e));constructor(){let e=m(fp,{optional:!0}),i=m(pp,{optional:!0}),r=m(Pl),o=m(TE,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?ko(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ll(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Ui(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){ZN.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=G({type:n,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&ie("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(gn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),Se("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),W("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe]},exportAs:["matInput"],features:[nt([{provide:Sp,useExisting:n}]),Ft]})}return n})(),NE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[Ns,Ns,ME,$e]})}return n})();var kE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[$e]})}return n})();var JN=["*"],RE=(()=>{class n{labelPosition="after";static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&W("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:JN,decls:1,vars:0,template:function(i,r){i&1&&(Ke(),ae(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})}return n})();var ek=["switch"],tk=["*"];function nk(n,t){n&1&&(g(0,"span",11),To(),g(1,"svg",13),Re(2,"path",14),p(),g(3,"svg",15),Re(4,"path",16),p()())}var ik=new w("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),Hl=class{source;checked;constructor(t,e){this.source=t,this.checked=e}},rk=(()=>{class n{_elementRef=m(re);_focusMonitor=m(nr);_changeDetectorRef=m(vn);defaults=m(ik);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new Hl(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Ut();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new ve;toggleChange=new ve;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){m(Bt).load(rr);let e=m(new jr("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=m(Ht).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new Hl(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=le({type:n,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&Pn(ek,5),i&2){let o;de(o=ue())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(gn("id",r.id),Se("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),pt(r.color?"mat-"+r.color:""),W("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",pe],color:"color",disabled:[2,"disabled","disabled",pe],disableRipple:[2,"disableRipple","disableRipple",pe],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Br(e)],checked:[2,"checked","checked",pe],hideIcon:[2,"hideIcon","hideIcon",pe],disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[nt([{provide:Il,useExisting:_t(()=>n),multi:!0},{provide:eo,useExisting:n,multi:!0}]),Ft],ngContentSelectors:tk,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(Ke(),g(0,"div",1)(1,"button",2,0),ie("click",function(){return r._handleClick()}),Re(3,"div",3)(4,"span",4),g(5,"span",5)(6,"span",6)(7,"span",7),Re(8,"span",8),p(),g(9,"span",9),Re(10,"span",10),p(),Ee(11,nk,5,0,"span",11),p()()(),g(12,"label",12),ie("click",function(s){return s.stopPropagation()}),ae(13),p()()),i&2){let o=Vr(2);N("labelPosition",r.labelPosition),b(),W("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),N("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),Se("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),b(9),N("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),b(),we(r.hideIcon?-1:11),b(),N("for",r.buttonId),Se("id",r._labelId)}},dependencies:[U0,RE],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})}return n})(),FE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=K({type:n});static \u0275inj=z({imports:[rk,$e]})}return n})();var ok="https://aamdb-840c9-default-rtdb.firebaseio.com",OE=(()=>{class n{constructor(e,i){this.http=e,this.zone=i,this.user=xe({name:"Admin",role:"ADMIN"})}firebaseUrl(e){return`${ok}/${e}.json`}loadState(){return this.http.get(this.firebaseUrl("aam")).pipe(ne(e=>this.normalizeState(e)),Qn(()=>lt(this.emptyState())))}saveChecklist(e){return this.http.put(this.firebaseUrl("aam/checklist"),e).pipe(Qn(()=>lt(null)))}listenChecklist(e){let i=new EventSource(this.firebaseUrl("aam/checklist")),r=o=>{if(!o.data)return;let s=JSON.parse(o.data);Array.isArray(s.data)&&this.zone.run(()=>e(s.data))};return i.onmessage=r,i.addEventListener("put",r),i.addEventListener("patch",r),i.onerror=()=>i.close(),()=>i.close()}loginAs(e){this.user.set({name:e==="MOM"?"Mom":"Admin",role:e})}normalizeState(e){let i=this.emptyState();return{checklist:e?.checklist??i.checklist,todayMeal:e?.todayMeal??i.todayMeal,weeklyPlan:e?.weeklyPlan??i.weeklyPlan,recipes:e?.recipes??i.recipes,medicines:e?.medicines??i.medicines,monthlyHealth:e?.monthlyHealth??i.monthlyHealth,stats:e?.stats??i.stats,notifications:e?.notifications??i.notifications,updatedAt:e?.updatedAt}}emptyState(){return{checklist:[],todayMeal:[],weeklyPlan:[],recipes:[],medicines:[],monthlyHealth:{},stats:{daily:0,weekly:0,meals:0,tablets:0,missedTablets:0,missedMeals:0,water:0,walking:0,painTrend:[]},notifications:[]}}static{this.\u0275fac=function(i){return new(i||n)(M(zr),M(O))}}static{this.\u0275prov=P({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function ak(n,t){if(n&1){let e=wt();g(0,"button",11),ie("click",function(){let r=Je(e).$implicit,o=H(2);return et(o.active.set(r.key))}),g(1,"mat-icon"),y(2),p(),g(3,"span"),y(4),p()()}if(n&2){let e=t.$implicit,i=H(2);W("active",i.active()===e.key),b(2),T(e.icon),b(2),T(e.label)}}function ck(n,t){if(n&1&&(g(0,"nav",9),me(1,ak,5,4,"button",10),p()),n&2){let e=H();b(),N("ngForOf",e.currentNav())}}function lk(n,t){if(n&1){let e=wt();g(0,"button",12),ie("click",function(){Je(e);let r=H();return et(r.active.set("login"))}),y(1,"Switch"),p()}}function dk(n,t){if(n&1){let e=wt();g(0,"section",13)(1,"div",14),y(2,"AAM"),p(),g(3,"div",15)(4,"button",16),ie("click",function(){Je(e);let r=H();return et(r.loginAs("mom"))}),g(5,"mat-icon"),y(6,"restaurant"),p(),g(7,"span"),y(8,"Mom Login"),p()(),g(9,"button",17),ie("click",function(){Je(e);let r=H();return et(r.loginAs("admin"))}),g(10,"mat-icon"),y(11,"checklist"),p(),g(12,"span"),y(13,"Admin Login"),p()()(),g(14,"button",18),ie("click",function(){Je(e);let r=H();return et(r.active.set("register"))}),y(15,"Register"),p()()}}function uk(n,t){if(n&1){let e=wt();g(0,"section",19)(1,"h1"),y(2,"Register"),p(),g(3,"mat-card",20)(4,"mat-form-field",21)(5,"mat-label"),y(6,"Name"),p(),Re(7,"input",22),p(),g(8,"mat-form-field",21)(9,"mat-label"),y(10,"Email"),p(),Re(11,"input",23),p(),g(12,"mat-form-field",21)(13,"mat-label"),y(14,"Password"),p(),Re(15,"input",24),p(),g(16,"button",25),ie("click",function(){Je(e);let r=H();return et(r.loginAs("admin"))}),y(17,"Create"),p()()()}}function fk(n,t){if(n&1){let e=wt();g(0,"mat-card",39)(1,"div",40)(2,"div")(3,"h2"),y(4),p(),g(5,"p"),y(6),p()()(),g(7,"div",41)(8,"span"),y(9),p(),g(10,"b"),y(11),p(),g(12,"span"),y(13),p()(),g(14,"p",42),y(15),p(),g(16,"button",25),ie("click",function(){let r=Je(e).$implicit,o=H(2);return et(o.markDone(r))}),y(17),p()()}if(n&2){let e=t.$implicit,i=H(2);pt(i.statusClass(e.status)),b(4),T(e.label),b(2),T(e.detail),b(3),T(e.time),b(2),T(e.status),b(2),T(e.completedAt||"Not done"),b(2),T(i.windowLabel(e)),b(2),T(i.buttonText(e))}}function mk(n,t){if(n&1&&(g(0,"article",43)(1,"strong"),y(2),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(2),T(e.meal),b(2),T(e.menu)}}function pk(n,t){n&1&&(g(0,"p",44),y(1,"No meal data in Firebase yet."),p())}function hk(n,t){if(n&1&&(g(0,"article")(1,"mat-icon"),y(2,"priority_high"),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(4),He("",e.label," missed")}}function gk(n,t){if(n&1&&(g(0,"article")(1,"mat-icon"),y(2,"schedule"),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(4),T(e)}}function vk(n,t){if(n&1&&(g(0,"section",26)(1,"div",27)(2,"mat-card",28)(3,"strong"),y(4),Dc(5,"date"),p(),g(6,"span"),y(7),Dc(8,"date"),p()(),g(9,"mat-card",29)(10,"strong"),y(11),p(),g(12,"span"),y(13,"Today"),p()(),g(14,"mat-card",30)(15,"strong"),y(16,"Monthly sugar"),p(),g(17,"span"),y(18),p()()(),g(19,"h1"),y(20,"Dashboard"),p(),g(21,"div",31),me(22,fk,18,9,"mat-card",32),p(),g(23,"div",33)(24,"mat-card",34)(25,"h2"),y(26,"Today's Meal"),p(),me(27,mk,5,2,"article",35)(28,pk,2,0,"p",36),p(),g(29,"mat-card",37)(30,"h2"),y(31,"Missed Alerts"),p(),me(32,hk,5,1,"article",38)(33,gk,5,1,"article",38),g(34,"article")(35,"mat-icon"),y(36,"opacity"),p(),g(37,"span"),y(38),p()()()()()),n&2){let e=H();b(4),T(Cc(5,10,e.today,"EEEE")),b(3),T(Cc(8,13,e.today,"MMM d, y")),b(4),He("",e.completion(),"%"),b(7),T(e.monthlyHealth.notes),b(4),N("ngForOf",e.checklist()),b(5),N("ngForOf",e.todayMeal),b(),N("ngIf",!e.todayMeal.length),b(4),N("ngForOf",e.missedItems()),b(),N("ngForOf",e.timeAlerts()),b(5),T(e.monthlyHealth.notes)}}function yk(n,t){if(n&1&&(g(0,"mat-card",49)(1,"span"),y(2),p(),g(3,"strong"),y(4),p()()),n&2){let e=t.$implicit;pt(e.tone),b(2),T(e.label),b(2),T(e.value)}}function bk(n,t){if(n&1&&(g(0,"article",50)(1,"strong"),y(2),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(2),T(e.label),b(2),wc("",e.status," ",e.completedAt?"- "+e.completedAt:"")}}function _k(n,t){if(n&1&&(g(0,"article")(1,"mat-icon"),y(2,"warning"),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(4),He("",e.label," missed")}}function Ek(n,t){if(n&1&&(g(0,"article")(1,"mat-icon"),y(2,"schedule"),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(4),T(e)}}function wk(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Dashboard"),p(),g(3,"div",45),me(4,yk,5,4,"mat-card",46),p(),g(5,"div",33)(6,"mat-card",47)(7,"h2"),y(8,"Today report"),p(),me(9,bk,5,3,"article",48),p(),g(10,"mat-card",37)(11,"h2"),y(12,"Alerts"),p(),me(13,_k,5,1,"article",38)(14,Ek,5,1,"article",38),g(15,"article")(16,"mat-icon"),y(17,"warning"),p(),g(18,"span"),y(19,"No activity today after lunch"),p()(),g(20,"article")(21,"mat-icon"),y(22,"warning"),p(),g(23,"span"),y(24,"Pain level above 7"),p()(),g(25,"article")(26,"mat-icon"),y(27,"warning"),p(),g(28,"span"),y(29),p()()()()()),n&2){let e=H();b(4),N("ngForOf",e.adminCards()),b(5),N("ngForOf",e.checklist()),b(4),N("ngForOf",e.missedItems()),b(),N("ngForOf",e.timeAlerts()),b(15),T(e.monthlyHealth.notes)}}function Dk(n,t){if(n&1){let e=wt();g(0,"mat-card",53)(1,"h2"),y(2),p(),g(3,"p"),y(4),p(),g(5,"div",54)(6,"span"),y(7,"Curry"),p(),g(8,"b"),y(9),p()(),g(10,"div",54)(11,"span"),y(12,"Portion"),p(),g(13,"b"),y(14),p()(),g(15,"p",55),y(16),p(),g(17,"button",25),ie("click",function(){Je(e);let r=H(2);return et(r.active.set("recipes"))}),y(18,"Recipe"),p()()}if(n&2){let e=t.$implicit;b(2),T(e.meal),b(2),T(e.menu),b(5),T(e.curry),b(5),T(e.portion),b(2),T(e.alternative)}}function Ck(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Today's Meal"),p(),g(3,"div",51),me(4,Dk,19,5,"mat-card",52),p()()),n&2){let e=H();b(4),N("ngForOf",e.todayMeal)}}function Ik(n,t){if(n&1){let e=wt();g(0,"mat-card",58)(1,"h2"),y(2),p(),g(3,"div")(4,"strong"),y(5,"Breakfast"),p(),g(6,"span"),y(7),p()(),g(8,"div")(9,"strong"),y(10,"Lunch"),p(),g(11,"span"),y(12),p()(),g(13,"div")(14,"strong"),y(15,"Dinner"),p(),g(16,"span"),y(17),p()(),g(18,"p",55),y(19),p(),g(20,"button",59),ie("click",function(){Je(e);let r=H(2);return et(r.active.set("recipes"))}),y(21,"Recipe"),p()()}if(n&2){let e=t.$implicit;b(2),T(e.day),b(5),T(e.breakfast),b(5),T(e.lunch),b(5),T(e.dinner),b(2),T(e.alternative)}}function xk(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Weekly Meal Plan"),p(),g(3,"div",56),me(4,Ik,22,5,"mat-card",57),p()()),n&2){let e=H();b(4),N("ngForOf",e.weeklyPlan)}}function Sk(n,t){if(n&1){let e=wt();g(0,"article",68)(1,"strong"),y(2),p(),g(3,"span"),y(4),p(),g(5,"mat-chip"),y(6),p(),g(7,"button",25),ie("click",function(){let r=Je(e).$implicit,o=H(3);return et(o.selectedRecipe.set(r))}),y(8,"View Recipe"),p()()}if(n&2){let e=t.$implicit;b(2),T(e.name),b(2),T(e.vegetable),b(2),T(e.type)}}function Mk(n,t){if(n&1&&(g(0,"mat-card",65)(1,"h2"),y(2),p(),g(3,"div",66),me(4,Sk,9,3,"article",67),p()()),n&2){let e=t.$implicit;b(2),T(e.vegetable),b(2),N("ngForOf",e.recipes)}}function Tk(n,t){n&1&&(g(0,"p",44),y(1,"No recipes in Firebase yet."),p())}function Ak(n,t){if(n&1&&(g(0,"li"),y(1),p()),n&2){let e=t.$implicit;b(),T(e)}}function Nk(n,t){if(n&1){let e=wt();g(0,"mat-card",69)(1,"button",70),ie("click",function(){Je(e);let r=H(2);return et(r.selectedRecipe.set(null))}),g(2,"mat-icon"),y(3,"close"),p()(),g(4,"h2"),y(5),p(),g(6,"p")(7,"b"),y(8,"Ingredients:"),p(),y(9),p(),g(10,"ol"),me(11,Ak,2,1,"li",38),p(),g(12,"p")(13,"b"),y(14,"Oil:"),p(),y(15),p(),g(16,"p")(17,"b"),y(18,"Salt:"),p(),y(19),p(),g(20,"p")(21,"b"),y(22,"Note:"),p(),y(23),p(),g(24,"p")(25,"b"),y(26,"Serving:"),p(),y(27),p()()}if(n&2){let e=H(2);b(5),T(e.selectedRecipe()?.name),b(4),He(" ",e.selectedRecipe()?.ingredients),b(2),N("ngForOf",e.selectedRecipe()?.steps),b(4),He(" ",e.selectedRecipe()?.oil),b(4),He(" ",e.selectedRecipe()?.salt),b(4),He(" ",e.selectedRecipe()?.diabetes),b(4),He(" ",e.selectedRecipe()?.serving)}}function kk(n,t){if(n&1){let e=wt();g(0,"section",26)(1,"div",60)(2,"h1"),y(3,"Recipe List"),p(),g(4,"mat-form-field",21)(5,"mat-label"),y(6,"Search"),p(),g(7,"input",61),ie("ngModelChange",function(r){Je(e);let o=H();return et(o.search.set(r))}),p(),Nf(),p()(),g(8,"div",62),me(9,Mk,5,2,"mat-card",63),p(),me(10,Tk,2,0,"p",36)(11,Nk,28,7,"mat-card",64),p()}if(n&2){let e=H();b(7),N("ngModel",e.search()),kf(),b(2),N("ngForOf",e.recipeGroups()),b(),N("ngIf",!e.recipeGroups().length),b(),N("ngIf",e.selectedRecipe())}}function Rk(n,t){if(n&1&&(g(0,"mat-card",72)(1,"h2"),y(2),p(),g(3,"p"),y(4),p(),g(5,"div",54)(6,"span"),y(7,"Time"),p(),g(8,"b"),y(9),p()(),g(10,"div",54)(11,"span"),y(12,"Food"),p(),g(13,"b"),y(14),p()(),g(15,"div",54)(16,"span"),y(17,"Status"),p(),g(18,"b"),y(19),p()(),g(20,"button",73),y(21,"Taken"),p()()),n&2){let e=t.$implicit,i=H(2);pt(i.statusClass(e.status)),b(2),T(e.name),b(2),T(e.dosage),b(5),T(e.time),b(5),T(e.food),b(5),T(e.status)}}function Fk(n,t){n&1&&(g(0,"p",44),y(1,"No medicine reminders in Firebase yet."),p())}function Ok(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Medicine Reminder"),p(),g(3,"div",51),me(4,Rk,22,7,"mat-card",71),p(),me(5,Fk,2,0,"p",36),p()),n&2){let e=H();b(4),N("ngForOf",e.medicines),b(),N("ngIf",!e.medicines.length)}}function Pk(n,t){if(n&1&&(g(0,"span"),y(1),p()),n&2){let e=t.$implicit;Wo("height",e*10,"%"),b(),T(e)}}function Lk(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Reports / Stats"),p(),g(3,"div",45)(4,"mat-card",74)(5,"span"),y(6,"Daily completion"),p(),g(7,"strong"),y(8),p()(),g(9,"mat-card",75)(10,"span"),y(11,"Weekly completion"),p(),g(12,"strong"),y(13),p()(),g(14,"mat-card",76)(15,"span"),y(16,"Meals completed"),p(),g(17,"strong"),y(18),p()(),g(19,"mat-card",77)(20,"span"),y(21,"Tablets completed"),p(),g(22,"strong"),y(23),p()(),g(24,"mat-card",78)(25,"span"),y(26,"Missed tablets"),p(),g(27,"strong"),y(28),p()(),g(29,"mat-card",78)(30,"span"),y(31,"Missed meals"),p(),g(32,"strong"),y(33),p()(),g(34,"mat-card",74)(35,"span"),y(36,"Water completion"),p(),g(37,"strong"),y(38),p()(),g(39,"mat-card",75)(40,"span"),y(41,"Walking completion"),p(),g(42,"strong"),y(43),p()()(),g(44,"mat-card",47)(45,"h2"),y(46,"Pain trend"),p(),g(47,"div",79),me(48,Pk,2,3,"span",80),p()()()),n&2){let e=H();b(8),He("",e.stats.daily,"%"),b(5),He("",e.stats.weekly,"%"),b(5),He("",e.stats.meals,"%"),b(5),He("",e.stats.tablets,"%"),b(5),T(e.stats.missedTablets),b(5),T(e.stats.missedMeals),b(5),He("",e.stats.water,"%"),b(5),He("",e.stats.walking,"%"),b(5),N("ngForOf",e.stats.painTrend)}}function Vk(n,t){if(n&1&&(g(0,"article")(1,"mat-icon"),y(2,"notifications"),p(),g(3,"span"),y(4),p()()),n&2){let e=t.$implicit;b(4),T(e)}}function jk(n,t){if(n&1&&(g(0,"section",26)(1,"h1"),y(2,"Notifications"),p(),g(3,"mat-card",37),me(4,Vk,5,1,"article",38),p()()),n&2){let e=H();b(4),N("ngForOf",e.notifications)}}function Bk(n,t){if(n&1){let e=wt();g(0,"button",11),ie("click",function(){let r=Je(e).$implicit,o=H(2);return et(o.active.set(r.key))}),g(1,"mat-icon"),y(2),p(),g(3,"span"),y(4),p()()}if(n&2){let e=t.$implicit,i=H(2);W("active",i.active()===e.key),b(2),T(e.icon),b(2),T(e.label)}}function Hk(n,t){if(n&1&&(g(0,"nav",81),me(1,Bk,5,4,"button",10),p()),n&2){let e=H();b(),N("ngForOf",e.currentNav())}}var PE=(()=>{class n{constructor(e){this.aam=e,this.active=xe("login"),this.role=xe(null),this.today=new Date,this.search=xe(""),this.selectedRecipe=xe(null),this.checklist=xe([]),this.recipes=xe([]),this.todayMeal=[],this.weeklyPlan=[],this.medicines=[],this.monthlyHealth={},this.stats={daily:0,weekly:0,meals:0,tablets:0,missedTablets:0,missedMeals:0,water:0,walking:0,painTrend:[]},this.notifications=[],this.windows={breakfast:{start:420,end:600,label:"7 AM - 10 AM"},lunch:{start:720,end:840,label:"12 PM - 2 PM"},dinner:{start:1140,end:1260,label:"7 PM - 9 PM"},"morning-tablet":{start:480,end:600,label:"8 AM - 10 AM"},"afternoon-tablet":{start:780,end:900,label:"1 PM - 3 PM"},"night-tablet":{start:1200,end:1320,label:"8 PM - 10 PM"},walking:{start:1020,end:1200,label:"5 PM - 8 PM"},water:{start:420,end:1320,label:"7 AM - 10 PM"}},this.completion=Ue(()=>{let i=this.checklist();return Math.round(i.filter(r=>r.status==="Done").length/i.length*100)}),this.missedItems=Ue(()=>this.checklist().filter(i=>i.status==="Missed")),this.timeAlerts=Ue(()=>this.checklist().filter(i=>i.status==="Pending"&&this.isExpired(i)).map(i=>`${i.label} not marked during ${this.windowLabel(i)}`)),this.adminCards=Ue(()=>[{label:"Today completion",value:`${this.completion()}%`,tone:"blue"},...this.checklist().filter(i=>["Breakfast","Lunch","Dinner","Morning Tablet","Afternoon Tablet","Night Tablet","Walking"].includes(i.label)).map(i=>({label:i.label,value:i.status,tone:this.tone(i.status)})),{label:"Pain level",value:"8 / 10",tone:"rose"},{label:"Missed items",value:`${this.missedItems().length}`,tone:"yellow"}]),this.recipeGroups=Ue(()=>{let i=this.search().toLowerCase(),r=new Map;for(let o of this.recipes().filter(s=>`${s.vegetable} ${s.name}`.toLowerCase().includes(i)))r.set(o.vegetable,[...r.get(o.vegetable)??[],o]);return[...r.entries()].map(([o,s])=>({vegetable:o,recipes:s}))}),this.momNav=[{key:"mom",label:"Dashboard",icon:"home"},{key:"today",label:"Today's Meal",icon:"restaurant"},{key:"weekly",label:"Weekly Plan",icon:"calendar_view_week"},{key:"recipes",label:"Recipes",icon:"menu_book"},{key:"reminders",label:"Reminders",icon:"notifications_active"}],this.adminNav=[{key:"admin",label:"Dashboard",icon:"space_dashboard"},{key:"stats",label:"Weekly Stats",icon:"bar_chart"},{key:"notifications",label:"Notifications",icon:"notifications"}]}ngOnInit(){this.aam.loadState().subscribe(e=>{this.applyChecklist(e.checklist),this.todayMeal=e.todayMeal,this.weeklyPlan=e.weeklyPlan,this.recipes.set(e.recipes),this.medicines=e.medicines,this.monthlyHealth=e.monthlyHealth,this.stats=e.stats,this.notifications=e.notifications}),this.stopChecklistListener=this.aam.listenChecklist(e=>this.applyChecklist(e))}ngOnDestroy(){this.stopChecklistListener?.()}currentNav(){return this.role()==="admin"?this.adminNav:this.momNav}loginAs(e){this.role.set(e),this.aam.loginAs(e==="mom"?"MOM":"ADMIN"),this.active.set(e==="mom"?"mom":"admin")}markDone(e){if(!this.canMarkNow(e)){window.alert(`${e.label} can be marked only during ${this.windowLabel(e)}.`);return}this.setStatus(e,"Done")}setStatus(e,i){let r=new Date().toLocaleTimeString([],{hour:"numeric",minute:"2-digit"});this.checklist.update(o=>o.map(s=>s.id===e.id?ee(I({},s),{status:i,completedAt:i==="Done"?r:void 0}):s)),this.aam.saveChecklist(this.checklist()).subscribe()}statusClass(e){return e.toLowerCase()}bar(e){return`${Math.max(5,e)}%`}buttonText(e){return e.status==="Done"?"Done":this.isBeforeWindow(e)?this.windowLabel(e):this.isExpired(e)?"Missed":"Mark Done"}canMarkNow(e){let i=this.windows[e.id];if(!i)return!0;let r=this.minutesNow();return r>=i.start&&r<=i.end}windowLabel(e){return this.windows[e.id]?.label??e.time}applyChecklist(e){let i=this.enforceTimeWindows(e);this.checklist.set(i),JSON.stringify(i)!==JSON.stringify(e)&&this.aam.saveChecklist(i).subscribe()}enforceTimeWindows(e){return e.map(i=>{let r=this.windows[i.id];return r?i.status==="Done"?this.completedWithinWindow(i,r)?i:ee(I({},i),{status:"Missed",completedAt:void 0}):this.isExpired(i)?ee(I({},i),{status:"Missed",completedAt:void 0}):i:i})}isBeforeWindow(e){let i=this.windows[e.id];return!!(i&&this.minutesNow()<i.start)}isExpired(e){let i=this.windows[e.id];return!!(i&&this.minutesNow()>i.end)}completedWithinWindow(e,i){if(!e.completedAt)return!1;let r=this.parseTime(e.completedAt);return r>=i.start&&r<=i.end}parseTime(e){let i=e.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);if(!i)return-1;let r=Number(i[1]),o=Number(i[2]),s=i[3].toUpperCase();return s==="PM"&&r!==12&&(r+=12),s==="AM"&&r===12&&(r=0),r*60+o}minutesNow(){let e=new Date;return e.getHours()*60+e.getMinutes()}tone(e){return e==="Done"||e==="Taken"?"lime":e==="Missed"?"rose":"yellow"}static{this.\u0275fac=function(i){return new(i||n)($(OE))}}static{this.\u0275cmp=le({type:n,selectors:[["aam-root"]],decls:17,vars:13,consts:[[1,"app-shell"],[1,"topbar"],["type","button",1,"logo-button",3,"click"],["class","desktop-nav",4,"ngIf"],["mat-flat-button","","class","soft-action",3,"click",4,"ngIf"],["class","login-screen",4,"ngIf"],["class","page narrow",4,"ngIf"],["class","page",4,"ngIf"],["class","bottom-nav",4,"ngIf"],[1,"desktop-nav"],["type","button",3,"active","click",4,"ngFor","ngForOf"],["type","button",3,"click"],["mat-flat-button","",1,"soft-action",3,"click"],[1,"login-screen"],[1,"login-title"],[1,"login-cards"],["type","button",1,"login-card","mom-login",3,"click"],["type","button",1,"login-card","admin-login",3,"click"],["type","button",1,"plain-link",3,"click"],[1,"page","narrow"],[1,"soft-card","form-card"],["appearance","outline"],["matInput","","value","AAM user"],["matInput","","value","admin@aam.local"],["matInput","","type","password","value","Aam@1234"],["mat-flat-button","",1,"primary-button",3,"click"],[1,"page"],[1,"summary-row"],[1,"soft-card","date-card"],[1,"soft-card","percent-card"],[1,"soft-card","health-card"],[1,"card-grid","task-grid"],["class","soft-card task-card",3,"class",4,"ngFor","ngForOf"],[1,"two-column"],[1,"soft-card","meal-summary-card"],["class","mini-row meal-summary-row",4,"ngFor","ngForOf"],["class","empty-note",4,"ngIf"],[1,"soft-card","alert-card"],[4,"ngFor","ngForOf"],[1,"soft-card","task-card"],[1,"task-head"],[1,"task-meta"],[1,"window-note"],[1,"mini-row","meal-summary-row"],[1,"empty-note"],[1,"card-grid","admin-grid"],["class","soft-card stat-card",3,"class",4,"ngFor","ngForOf"],[1,"soft-card"],["class","mini-row",4,"ngFor","ngForOf"],[1,"soft-card","stat-card"],[1,"mini-row"],[1,"card-grid"],["class","soft-card meal-card",4,"ngFor","ngForOf"],[1,"soft-card","meal-card"],[1,"pill-line"],[1,"note"],[1,"week-list"],["class","soft-card week-card",4,"ngFor","ngForOf"],[1,"soft-card","week-card"],["mat-stroked-button","",3,"click"],[1,"page-head"],["matInput","",3,"ngModelChange","ngModel"],[1,"recipe-groups"],["class","soft-card recipe-group",4,"ngFor","ngForOf"],["class","soft-card recipe-detail",4,"ngIf"],[1,"soft-card","recipe-group"],[1,"recipe-cards"],["class","recipe-card",4,"ngFor","ngForOf"],[1,"recipe-card"],[1,"soft-card","recipe-detail"],["type","button",1,"close",3,"click"],["class","soft-card medicine-card",3,"class",4,"ngFor","ngForOf"],[1,"soft-card","medicine-card"],["mat-flat-button","",1,"primary-button"],[1,"soft-card","stat-card","blue"],[1,"soft-card","stat-card","lime"],[1,"soft-card","stat-card","pink"],[1,"soft-card","stat-card","yellow"],[1,"soft-card","stat-card","rose"],[1,"trend"],[3,"height",4,"ngFor","ngForOf"],[1,"bottom-nav"]],template:function(i,r){i&1&&(g(0,"main",0)(1,"header",1)(2,"button",2),ie("click",function(){return r.active.set(r.role()==="admin"?"admin":"mom")}),y(3,"AAM"),p(),me(4,ck,2,1,"nav",3)(5,lk,2,0,"button",4),p(),me(6,dk,16,0,"section",5)(7,uk,18,0,"section",6)(8,vk,39,16,"section",7)(9,wk,30,5,"section",7)(10,Ck,5,1,"section",7)(11,xk,5,1,"section",7)(12,kk,12,4,"section",7)(13,Ok,6,2,"section",7)(14,Lk,49,9,"section",7)(15,jk,5,1,"section",7)(16,Hk,2,1,"nav",8),p()),i&2&&(b(4),N("ngIf",r.role()),b(),N("ngIf",r.role()==="admin"),b(),N("ngIf",r.active()==="login"),b(),N("ngIf",r.active()==="register"),b(),N("ngIf",r.active()==="mom"),b(),N("ngIf",r.active()==="admin"),b(),N("ngIf",r.active()==="today"),b(),N("ngIf",r.active()==="weekly"),b(),N("ngIf",r.active()==="recipes"),b(),N("ngIf",r.active()==="reminders"),b(),N("ngIf",r.active()==="stats"),b(),N("ngIf",r.active()==="notifications"),b(),N("ngIf",r.role()))},dependencies:[$c,Uc,mm,C0,xl,w0,mp,Q0,K0,Y0,Z0,yE,vE,Ns,jl,As,xE,IE,NE,AE,kE,FE,hm],styles:["[_nghost-%COMP%]{--light-pink: #FFDBDF;--rose-pink: #F791A9;--soft-yellow: #FFE797;--lime-pastel: #DDDD7B;--ice-blue: #E0F2F4;--muted-blue: #BAD6DA;--cream: #fffdf7;--ink: #312a26;--soft-shadow: 0 14px 34px rgba(49, 42, 38, .11);display:block;min-height:100vh;background:radial-gradient(circle at top left,rgba(255,219,223,.58),transparent 32rem),radial-gradient(circle at top right,rgba(224,242,244,.72),transparent 28rem),var(--cream);color:var(--ink);font-family:Comic Sans MS,Bradley Hand ITC,Segoe Print,cursive}.app-shell[_ngcontent-%COMP%]{min-height:100vh;padding-bottom:92px}.topbar[_ngcontent-%COMP%]{position:sticky;top:0;z-index:10;display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px clamp(16px,4vw,40px);background:#fffdf7e0;-webkit-backdrop-filter:blur(16px);backdrop-filter:blur(16px)}.logo-button[_ngcontent-%COMP%]{border:0;background:transparent;color:var(--ink);cursor:pointer;font:inherit;font-size:32px;font-weight:800}.desktop-nav[_ngcontent-%COMP%]{display:flex;gap:8px;padding:8px;border-radius:999px;background:#fff;box-shadow:var(--soft-shadow)}.desktop-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%], .bottom-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:8px;border:0;border-radius:999px;padding:10px 14px;background:transparent;color:var(--ink);cursor:pointer;font:inherit}.desktop-nav[_ngcontent-%COMP%]   button.active[_ngcontent-%COMP%], .bottom-nav[_ngcontent-%COMP%]   button.active[_ngcontent-%COMP%]{background:var(--soft-yellow)}.soft-action[_ngcontent-%COMP%], .primary-button[_ngcontent-%COMP%]{border-radius:999px!important;background:var(--rose-pink)!important;color:var(--ink)!important;box-shadow:none!important}.login-screen[_ngcontent-%COMP%], .page[_ngcontent-%COMP%]{width:min(1180px,100% - 28px);margin:0 auto}.login-screen[_ngcontent-%COMP%]{display:grid;min-height:calc(100vh - 90px);place-content:center;gap:26px}.login-title[_ngcontent-%COMP%]{text-align:center;font-size:clamp(54px,15vw,132px);font-weight:900;line-height:.9}.login-cards[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,minmax(180px,300px));gap:18px}.login-card[_ngcontent-%COMP%]{display:grid;min-height:210px;place-items:center;gap:12px;border:0;border-radius:30px;color:var(--ink);cursor:pointer;font:inherit;font-size:28px;font-weight:800;box-shadow:var(--soft-shadow)}.login-card[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{width:54px;height:54px;font-size:54px}.mom-login[_ngcontent-%COMP%]{background:var(--light-pink)}.admin-login[_ngcontent-%COMP%]{background:var(--ice-blue)}.plain-link[_ngcontent-%COMP%]{width:max-content;margin:0 auto;border:0;background:transparent;color:var(--ink);cursor:pointer;font:inherit;text-decoration:underline}.page[_ngcontent-%COMP%]{display:grid;gap:18px;padding:18px 0 28px}.narrow[_ngcontent-%COMP%]{max-width:520px}h1[_ngcontent-%COMP%], h2[_ngcontent-%COMP%], p[_ngcontent-%COMP%]{margin:0}h1[_ngcontent-%COMP%]{font-size:clamp(34px,7vw,58px);line-height:1}h2[_ngcontent-%COMP%]{font-size:21px;line-height:1.15}.soft-card[_ngcontent-%COMP%]{border:0!important;border-radius:26px!important;background:#ffffffe6!important;box-shadow:var(--soft-shadow)!important;color:var(--ink)}.summary-row[_ngcontent-%COMP%], .card-grid[_ngcontent-%COMP%], .two-column[_ngcontent-%COMP%], .week-list[_ngcontent-%COMP%]{display:grid;gap:16px}.summary-row[_ngcontent-%COMP%]{grid-template-columns:1.1fr .8fr 1.4fr}.date-card[_ngcontent-%COMP%], .percent-card[_ngcontent-%COMP%], .health-card[_ngcontent-%COMP%]{padding:20px}.date-card[_ngcontent-%COMP%]{background:var(--ice-blue)!important}.percent-card[_ngcontent-%COMP%]{background:var(--soft-yellow)!important}.health-card[_ngcontent-%COMP%]{background:var(--light-pink)!important}.date-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .percent-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .health-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .stat-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;font-size:34px;line-height:1}.card-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(auto-fit,minmax(205px,1fr))}.task-card[_ngcontent-%COMP%], .meal-card[_ngcontent-%COMP%], .medicine-card[_ngcontent-%COMP%], .recipe-group[_ngcontent-%COMP%], .stat-card[_ngcontent-%COMP%], .form-card[_ngcontent-%COMP%]{min-width:0;padding:14px;overflow-wrap:anywhere;word-break:normal}.task-card[_ngcontent-%COMP%]{display:grid;gap:12px;min-height:184px}.task-head[_ngcontent-%COMP%]{display:flex;justify-content:space-between;gap:12px}.task-head[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .note[_ngcontent-%COMP%]{margin-top:8px;color:#312a26b8;line-height:1.25}.window-note[_ngcontent-%COMP%]{margin:-4px 0 0;color:#312a26ad;font-size:13px;line-height:1.2}.task-meta[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,minmax(0,auto));gap:8px;align-items:center;justify-content:space-between;padding:8px 10px;border-radius:18px;background:#fffdf7b8;font-size:14px}.pill-line[_ngcontent-%COMP%]{grid-template-columns:minmax(64px,auto) minmax(0,1fr)}.pill-line[_ngcontent-%COMP%]   b[_ngcontent-%COMP%], .mini-row[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .week-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .task-head[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .meal-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .medicine-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{min-width:0;overflow-wrap:anywhere}.done[_ngcontent-%COMP%], .taken[_ngcontent-%COMP%], .lime[_ngcontent-%COMP%]{background:var(--lime-pastel)!important}.pending[_ngcontent-%COMP%], .yellow[_ngcontent-%COMP%]{background:var(--soft-yellow)!important}.missed[_ngcontent-%COMP%], .rose[_ngcontent-%COMP%]{background:var(--light-pink)!important}.pink[_ngcontent-%COMP%]{background:var(--rose-pink)!important}.blue[_ngcontent-%COMP%]{background:var(--ice-blue)!important}.two-column[_ngcontent-%COMP%]{grid-template-columns:minmax(0,1fr) minmax(280px,.9fr)}.mini-row[_ngcontent-%COMP%], .alert-card[_ngcontent-%COMP%]   article[_ngcontent-%COMP%]{display:grid;grid-template-columns:minmax(92px,auto) minmax(0,1fr);gap:14px;padding:12px 0}.alert-card[_ngcontent-%COMP%]   article[_ngcontent-%COMP%]{grid-template-columns:28px minmax(0,1fr);align-items:center}.meal-summary-card[_ngcontent-%COMP%]{overflow:hidden}.meal-summary-row[_ngcontent-%COMP%]{grid-template-columns:138px minmax(0,1fr);align-items:start}.admin-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}.stat-card[_ngcontent-%COMP%]{min-height:112px}.stat-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:block;margin-bottom:22px}.week-card[_ngcontent-%COMP%]{display:grid;gap:12px;padding:14px}.week-card[_ngcontent-%COMP%]   div[_ngcontent-%COMP%]{display:grid;grid-template-columns:110px 1fr;gap:12px}.page-head[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:16px}.recipe-groups[_ngcontent-%COMP%]{display:grid;gap:16px}.recipe-cards[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin-top:14px}.recipe-card[_ngcontent-%COMP%]{display:grid;gap:10px;padding:14px;border-radius:22px;background:var(--ice-blue);min-width:0;overflow-wrap:anywhere}.empty-note[_ngcontent-%COMP%]{padding:16px;border-radius:20px;background:#e0f2f4b8}.recipe-detail[_ngcontent-%COMP%]{position:fixed;inset:auto 20px 20px auto;z-index:20;display:grid;width:min(520px,100% - 40px);max-height:78vh;overflow:auto;gap:12px;padding:22px;background:#fff!important}.close[_ngcontent-%COMP%]{justify-self:end;border:0;background:transparent;cursor:pointer}.trend[_ngcontent-%COMP%]{display:flex;align-items:end;gap:12px;height:180px;padding-top:18px}.trend[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:grid;width:38px;min-height:24px;place-items:start center;border-radius:20px 20px 8px 8px;background:var(--muted-blue)}.bottom-nav[_ngcontent-%COMP%]{position:fixed;right:14px;bottom:14px;left:14px;z-index:12;display:none;grid-template-columns:repeat(5,1fr);gap:4px;padding:8px;border-radius:26px;background:#fffffff0;box-shadow:var(--soft-shadow)}.bottom-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex-direction:column;gap:2px;padding:8px 4px;font-size:12px}@media(max-width:820px){.desktop-nav[_ngcontent-%COMP%]{display:none}.bottom-nav[_ngcontent-%COMP%]{display:grid}.summary-row[_ngcontent-%COMP%], .two-column[_ngcontent-%COMP%], .login-cards[_ngcontent-%COMP%], .page-head[_ngcontent-%COMP%]{grid-template-columns:1fr}.page-head[_ngcontent-%COMP%]{display:grid}}@media(max-width:560px){.topbar[_ngcontent-%COMP%]{padding:12px 14px}.login-card[_ngcontent-%COMP%]{min-height:150px;font-size:24px}.card-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}.mini-row[_ngcontent-%COMP%], .week-card[_ngcontent-%COMP%]   div[_ngcontent-%COMP%], .task-meta[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr}}"]})}}return n})();Dm(PE,{providers:[z_(),lm(),Ia(Q_.register("ngsw-worker.js",{enabled:!sb()}))]}).catch(n=>console.error(n));
