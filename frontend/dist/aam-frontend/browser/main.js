var HE=Object.defineProperty,UE=Object.defineProperties;var $E=Object.getOwnPropertyDescriptors;var Ls=Object.getOwnPropertySymbols;var Op=Object.prototype.hasOwnProperty,Pp=Object.prototype.propertyIsEnumerable;var Fp=(n,t,e)=>t in n?HE(n,t,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[t]=e,C=(n,t)=>{for(var e in t||={})Op.call(t,e)&&Fp(n,e,t[e]);if(Ls)for(var e of Ls(t))Pp.call(t,e)&&Fp(n,e,t[e]);return n},G=(n,t)=>UE(n,$E(t));var Vs=(n,t)=>{var e={};for(var i in n)Op.call(n,i)&&t.indexOf(i)<0&&(e[i]=n[i]);if(n!=null&&Ls)for(var i of Ls(n))t.indexOf(i)<0&&Pp.call(n,i)&&(e[i]=n[i]);return e};var bn=(n,t,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(n,t)).next())});var Ye=null,js=!1,ql=1,zE=null,Le=Symbol("SIGNAL");function F(n){let t=Ye;return Ye=n,t}function $s(){return Ye}var yi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function bi(n){if(js)throw new Error("");if(Ye===null)return;Ye.consumerOnSignalRead(n);let t=Ye.producersTail;if(t!==void 0&&t.producer===n)return;let e,i=Ye.recomputing;if(i&&(e=t!==void 0?t.nextProducer:Ye.producers,e!==void 0&&e.producer===n)){Ye.producersTail=e,e.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===Ye&&(!i||WE(r,Ye)))return;let o=lr(Ye),s={producer:n,consumer:Ye,nextProducer:e,prevConsumer:void 0,lastReadVersion:n.version,nextConsumer:void 0};Ye.producersTail=s,t!==void 0?t.nextProducer=s:Ye.producers=s,o&&Bp(n,s)}function Lp(){ql++}function zs(n){if(!(lr(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===ql)){if(!n.producerMustRecompute(n)&&!cr(n)){Us(n);return}n.producerRecomputeValue(n),Us(n)}}function Kl(n){if(n.consumers===void 0)return;let t=js;js=!0;try{for(let e=n.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||GE(i)}}finally{js=t}}function Ql(){return Ye?.consumerAllowSignalWrites!==!1}function GE(n){n.dirty=!0,Kl(n),n.consumerMarkedDirty?.(n)}function Us(n){n.dirty=!1,n.lastCleanEpoch=ql}function qn(n){return n&&Vp(n),F(n)}function Vp(n){n.producersTail=void 0,n.recomputing=!0}function _i(n,t){F(t),n&&jp(n)}function jp(n){n.recomputing=!1;let t=n.producersTail,e=t!==void 0?t.nextProducer:n.producers;if(e!==void 0){if(lr(n))do e=Zl(e);while(e!==void 0);t!==void 0?t.nextProducer=void 0:n.producers=void 0}}function cr(n){for(let t=n.producers;t!==void 0;t=t.nextProducer){let e=t.producer,i=t.lastReadVersion;if(i!==e.version||(zs(e),i!==e.version))return!0}return!1}function Kn(n){if(lr(n)){let t=n.producers;for(;t!==void 0;)t=Zl(t)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Bp(n,t){let e=n.consumersTail,i=lr(n);if(e!==void 0?(t.nextConsumer=e.nextConsumer,e.nextConsumer=t):(t.nextConsumer=void 0,n.consumers=t),t.prevConsumer=e,n.consumersTail=t,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Bp(r.producer,r)}function Zl(n){let t=n.producer,e=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:t.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(t.consumers=i,!lr(t)){let o=t.producers;for(;o!==void 0;)o=Zl(o)}return e}function lr(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Gs(n){zE?.(n)}function WE(n,t){let e=t.producersTail;if(e!==void 0){let i=t.producers;do{if(i===n)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Ws(n,t){return Object.is(n,t)}function ao(n,t){let e=Object.create(qE);e.computation=n,t!==void 0&&(e.equal=t);let i=()=>{if(zs(e),bi(e),e.value===so)throw e.error;return e.value};return i[Le]=e,Gs(e),i}var Bs=Symbol("UNSET"),Hs=Symbol("COMPUTING"),so=Symbol("ERRORED"),qE=G(C({},yi),{value:Bs,dirty:!0,error:null,equal:Ws,kind:"computed",producerMustRecompute(n){return n.value===Bs||n.value===Hs},producerRecomputeValue(n){if(n.value===Hs)throw new Error("");let t=n.value;n.value=Hs;let e=qn(n),i,r=!1;try{i=n.computation(),F(null),r=t!==Bs&&t!==so&&i!==so&&n.equal(t,i)}catch(o){i=so,n.error=o}finally{_i(n,e)}if(r){n.value=t;return}n.value=i,n.version++}});function KE(){throw new Error}var Hp=KE;function Up(n){Hp(n)}function Yl(n){Hp=n}var QE=null;function Xl(n,t){let e=Object.create(co);e.value=n,t!==void 0&&(e.equal=t);let i=()=>$p(e);return i[Le]=e,Gs(e),[i,s=>dr(e,s),s=>Jl(e,s)]}function $p(n){return bi(n),n.value}function dr(n,t){Ql()||Up(n),n.equal(n.value,t)||(n.value=t,ZE(n))}function Jl(n,t){Ql()||Up(n),dr(n,t(n.value))}var co=G(C({},yi),{equal:Ws,value:void 0,kind:"signal"});function ZE(n){n.version++,Lp(),Kl(n),QE?.(n)}var ed=G(C({},yi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function td(n){if(n.dirty=!1,n.version>0&&!cr(n))return;n.version++;let t=qn(n);try{n.cleanup(),n.fn()}finally{_i(n,t)}}var nd;function qs(){return nd}function on(n){let t=nd;return nd=n,t}var zp=Symbol("NotFound");function ur(n){return n===zp||n?.name==="\u0275NotFound"}function Gp(n){let t=F(null);try{return n()}finally{F(t)}}function B(n){return typeof n=="function"}function Ks(n){let e=n(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Qs=Ks(n=>function(e){n(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function lo(n,t){if(n){let e=n.indexOf(t);0<=e&&n.splice(e,1)}}var ke=class n{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(B(i))try{i()}catch(o){t=o instanceof Qs?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Wp(o)}catch(s){t=t??[],s instanceof Qs?t=[...t,...s.errors]:t.push(s)}}if(t)throw new Qs(t)}}add(t){var e;if(t&&t!==this)if(this.closed)Wp(t);else{if(t instanceof n){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(t)}}_hasParent(t){let{_parentage:e}=this;return e===t||Array.isArray(e)&&e.includes(t)}_addParent(t){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(t),e):e?[e,t]:t}_removeParent(t){let{_parentage:e}=this;e===t?this._parentage=null:Array.isArray(e)&&lo(e,t)}remove(t){let{_finalizers:e}=this;e&&lo(e,t),t instanceof n&&t._removeParent(this)}};ke.EMPTY=(()=>{let n=new ke;return n.closed=!0,n})();var id=ke.EMPTY;function Zs(n){return n instanceof ke||n&&"closed"in n&&B(n.remove)&&B(n.add)&&B(n.unsubscribe)}function Wp(n){B(n)?n():n.unsubscribe()}var zt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var fr={setTimeout(n,t,...e){let{delegate:i}=fr;return i?.setTimeout?i.setTimeout(n,t,...e):setTimeout(n,t,...e)},clearTimeout(n){let{delegate:t}=fr;return(t?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Ys(n){fr.setTimeout(()=>{let{onUnhandledError:t}=zt;if(t)t(n);else throw n})}function Ei(){}var qp=rd("C",void 0,void 0);function Kp(n){return rd("E",void 0,n)}function Qp(n){return rd("N",n,void 0)}function rd(n,t,e){return{kind:n,value:t,error:e}}var wi=null;function mr(n){if(zt.useDeprecatedSynchronousErrorHandling){let t=!wi;if(t&&(wi={errorThrown:!1,error:null}),n(),t){let{errorThrown:e,error:i}=wi;if(wi=null,e)throw i}}else n()}function Zp(n){zt.useDeprecatedSynchronousErrorHandling&&wi&&(wi.errorThrown=!0,wi.error=n)}var Di=class extends ke{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Zs(t)&&t.add(this)):this.destination=JE}static create(t,e,i){return new _n(t,e,i)}next(t){this.isStopped?sd(Qp(t),this):this._next(t)}error(t){this.isStopped?sd(Kp(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?sd(qp,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},YE=Function.prototype.bind;function od(n,t){return YE.call(n,t)}var ad=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:e}=this;if(e.next)try{e.next(t)}catch(i){Xs(i)}}error(t){let{partialObserver:e}=this;if(e.error)try{e.error(t)}catch(i){Xs(i)}else Xs(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(e){Xs(e)}}},_n=class extends Di{constructor(t,e,i){super();let r;if(B(t)||!t)r={next:t??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&zt.useDeprecatedNextContext?(o=Object.create(t),o.unsubscribe=()=>this.unsubscribe(),r={next:t.next&&od(t.next,o),error:t.error&&od(t.error,o),complete:t.complete&&od(t.complete,o)}):r=t}this.destination=new ad(r)}};function Xs(n){zt.useDeprecatedSynchronousErrorHandling?Zp(n):Ys(n)}function XE(n){throw n}function sd(n,t){let{onStoppedNotification:e}=zt;e&&fr.setTimeout(()=>e(n,t))}var JE={closed:!0,next:Ei,error:XE,complete:Ei};var pr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Qn(n){return n}function Yp(n){return n.length===0?Qn:n.length===1?n[0]:function(e){return n.reduce((i,r)=>r(i),e)}}var Q=(()=>{class n{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new n;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=tw(e)?e:new _n(e,i,r);return mr(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Xp(i),new i((r,o)=>{let s=new _n({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[pr](){return this}pipe(...e){return Yp(e)(this)}toPromise(e){return e=Xp(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return n.create=t=>new n(t),n})();function Xp(n){var t;return(t=n??zt.Promise)!==null&&t!==void 0?t:Promise}function ew(n){return n&&B(n.next)&&B(n.error)&&B(n.complete)}function tw(n){return n&&n instanceof Di||ew(n)&&Zs(n)}function nw(n){return B(n?.lift)}function oe(n){return t=>{if(nw(t))return t.lift(function(e){try{return n(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function he(n,t,e,i,r){return new cd(n,t,e,i,r)}var cd=class extends Di{constructor(t,e,i,r,o,s){super(t),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){t.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};var Jp=Ks(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Y=(()=>{class n extends Q{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Js(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Jp}next(e){mr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){mr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){mr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?id:(this.currentObservers=null,o.push(e),new ke(()=>{this.currentObservers=null,lo(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new Q;return e.source=this,e}}return n.create=(t,e)=>new Js(t,e),n})(),Js=class extends Y{constructor(t,e){super(),this.destination=t,this.source=e}next(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,t)}error(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,t)}complete(){var t,e;(e=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||e===void 0||e.call(t)}_subscribe(t){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(t))!==null&&i!==void 0?i:id}};var Ci=class extends Y{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let e=super._subscribe(t);return!e.closed&&t.next(this._value),e}getValue(){let{hasError:t,thrownError:e,_value:i}=this;if(t)throw e;return this._throwIfClosed(),i}next(t){super.next(this._value=t)}};var ld={now(){return(ld.delegate||Date).now()},delegate:void 0};var ea=class extends Y{constructor(t=1/0,e=1/0,i=ld){super(),this._bufferSize=t,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,e)}next(t){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(t),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(t),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!t.closed;s+=i?1:2)t.next(o[s]);return this._checkFinalizedStatuses(t),e}_trimBuffer(){let{_bufferSize:t,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*t;if(t<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var xi=new Q(n=>n.complete());function eh(n){return n&&B(n.schedule)}function dd(n){return n[n.length-1]}function th(n){return B(dd(n))?n.pop():void 0}function Zn(n){return eh(dd(n))?n.pop():void 0}function nh(n,t){return typeof dd(n)=="number"?n.pop():t}function rh(n,t,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(u){s(u)}}function c(d){try{l(i.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(n,t||[])).next())})}function ih(n){var t=typeof Symbol=="function"&&Symbol.iterator,e=t&&n[t],i=0;if(e)return e.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ii(n){return this instanceof Ii?(this.v=n,this):new Ii(n)}function oh(n,t,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(n,t||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(f){return function(v){return Promise.resolve(v).then(f,u)}}function a(f,v){i[f]&&(r[f]=function(E){return new Promise(function(D,I){o.push([f,E,D,I])>1||c(f,E)})},v&&(r[f]=v(r[f])))}function c(f,v){try{l(i[f](v))}catch(E){g(o[0][3],E)}}function l(f){f.value instanceof Ii?Promise.resolve(f.value.v).then(d,u):g(o[0][2],f)}function d(f){c("next",f)}function u(f){c("throw",f)}function g(f,v){f(v),o.shift(),o.length&&c(o[0][0],o[0][1])}}function sh(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=n[Symbol.asyncIterator],e;return t?t.call(n):(n=typeof ih=="function"?ih(n):n[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=n[o]&&function(s){return new Promise(function(a,c){s=n[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var ta=n=>n&&typeof n.length=="number"&&typeof n!="function";function na(n){return B(n?.then)}function ia(n){return B(n[pr])}function ra(n){return Symbol.asyncIterator&&B(n?.[Symbol.asyncIterator])}function oa(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function iw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var sa=iw();function aa(n){return B(n?.[sa])}function ca(n){return oh(this,arguments,function*(){let e=n.getReader();try{for(;;){let{value:i,done:r}=yield Ii(e.read());if(r)return yield Ii(void 0);yield yield Ii(i)}}finally{e.releaseLock()}})}function la(n){return B(n?.getReader)}function Ie(n){if(n instanceof Q)return n;if(n!=null){if(ia(n))return rw(n);if(ta(n))return ow(n);if(na(n))return sw(n);if(ra(n))return ah(n);if(aa(n))return aw(n);if(la(n))return cw(n)}throw oa(n)}function rw(n){return new Q(t=>{let e=n[pr]();if(B(e.subscribe))return e.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function ow(n){return new Q(t=>{for(let e=0;e<n.length&&!t.closed;e++)t.next(n[e]);t.complete()})}function sw(n){return new Q(t=>{n.then(e=>{t.closed||(t.next(e),t.complete())},e=>t.error(e)).then(null,Ys)})}function aw(n){return new Q(t=>{for(let e of n)if(t.next(e),t.closed)return;t.complete()})}function ah(n){return new Q(t=>{lw(n,t).catch(e=>t.error(e))})}function cw(n){return ah(ca(n))}function lw(n,t){var e,i,r,o;return rh(this,void 0,void 0,function*(){try{for(e=sh(n);i=yield e.next(),!i.done;){let s=i.value;if(t.next(s),t.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}t.complete()})}function St(n,t,e,i=0,r=!1){let o=t.schedule(function(){e(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(o),!r)return o}function da(n,t=0){return oe((e,i)=>{e.subscribe(he(i,r=>St(i,n,()=>i.next(r),t),()=>St(i,n,()=>i.complete(),t),r=>St(i,n,()=>i.error(r),t)))})}function ua(n,t=0){return oe((e,i)=>{i.add(n.schedule(()=>e.subscribe(i),t))})}function ch(n,t){return Ie(n).pipe(ua(t),da(t))}function lh(n,t){return Ie(n).pipe(ua(t),da(t))}function dh(n,t){return new Q(e=>{let i=0;return t.schedule(function(){i===n.length?e.complete():(e.next(n[i++]),e.closed||this.schedule())})})}function uh(n,t){return new Q(e=>{let i;return St(e,t,()=>{i=n[sa](),St(e,t,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>B(i?.return)&&i.return()})}function fa(n,t){if(!n)throw new Error("Iterable cannot be null");return new Q(e=>{St(e,t,()=>{let i=n[Symbol.asyncIterator]();St(e,t,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function fh(n,t){return fa(ca(n),t)}function mh(n,t){if(n!=null){if(ia(n))return ch(n,t);if(ta(n))return dh(n,t);if(na(n))return lh(n,t);if(ra(n))return fa(n,t);if(aa(n))return uh(n,t);if(la(n))return fh(n,t)}throw oa(n)}function En(n,t){return t?mh(n,t):Ie(n)}function st(...n){let t=Zn(n);return En(n,t)}function ud(n,t){let e=B(n)?n:()=>n,i=r=>r.error(e());return new Q(t?r=>t.schedule(i,0,r):i)}function ie(n,t){return oe((e,i)=>{let r=0;e.subscribe(he(i,o=>{i.next(n.call(t,o,r++))}))})}var{isArray:dw}=Array;function uw(n,t){return dw(t)?n(...t):n(t)}function ph(n){return ie(t=>uw(n,t))}var{isArray:fw}=Array,{getPrototypeOf:mw,prototype:pw,keys:hw}=Object;function hh(n){if(n.length===1){let t=n[0];if(fw(t))return{args:t,keys:null};if(gw(t)){let e=hw(t);return{args:e.map(i=>t[i]),keys:e}}}return{args:n,keys:null}}function gw(n){return n&&typeof n=="object"&&mw(n)===pw}function gh(n,t){return n.reduce((e,i,r)=>(e[i]=t[r],e),{})}function vh(n,t,e,i,r,o,s,a){let c=[],l=0,d=0,u=!1,g=()=>{u&&!c.length&&!l&&t.complete()},f=E=>l<i?v(E):c.push(E),v=E=>{o&&t.next(E),l++;let D=!1;Ie(e(E,d++)).subscribe(he(t,I=>{r?.(I),o?f(I):t.next(I)},()=>{D=!0},void 0,()=>{if(D)try{for(l--;c.length&&l<i;){let I=c.shift();s?St(t,s,()=>v(I)):v(I)}g()}catch(I){t.error(I)}}))};return n.subscribe(he(t,f,()=>{u=!0,g()})),()=>{a?.()}}function hr(n,t,e=1/0){return B(t)?hr((i,r)=>ie((o,s)=>t(i,o,r,s))(Ie(n(i,r))),e):(typeof t=="number"&&(e=t),oe((i,r)=>vh(i,r,n,e)))}function ma(n=1/0){return hr(Qn,n)}function yh(){return ma(1)}function fd(...n){return yh()(En(n,Zn(n)))}function uo(...n){let t=th(n),{args:e,keys:i}=hh(n),r=new Q(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;Ie(e[d]).subscribe(he(o,g=>{u||(u=!0,l--),a[d]=g},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(i?gh(i,a):a),o.complete())}))}});return t?r.pipe(ph(t)):r}function fo(...n){let t=Zn(n),e=nh(n,1/0),i=n;return i.length?i.length===1?Ie(i[0]):ma(e)(En(i,t)):xi}var wn=new Q(Ei);function Mt(n,t){return oe((e,i)=>{let r=0;e.subscribe(he(i,o=>n.call(t,o,r++)&&i.next(o)))})}function Dn(n){return oe((t,e)=>{let i=null,r=!1,o;i=t.subscribe(he(e,void 0,void 0,s=>{o=Ie(n(s,Dn(n)(t))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function md(n,t){return B(t)?hr(n,t,1):hr(n,1)}function Cn(n){return n<=0?()=>xi:oe((t,e)=>{let i=0;t.subscribe(he(e,r=>{++i<=n&&(e.next(r),n<=i&&e.complete())}))})}function pd(n,t=Qn){return n=n??vw,oe((e,i)=>{let r,o=!0;e.subscribe(he(i,s=>{let a=t(s);(o||!n(r,a))&&(o=!1,r=a,i.next(s))}))})}function vw(n,t){return n===t}function mo(n){return oe((t,e)=>{try{t.subscribe(e)}finally{e.add(n)}})}function hd(){return oe((n,t)=>{let e,i=!1;n.subscribe(he(t,r=>{let o=e;e=r,i&&t.next([o,r]),i=!0}))})}function po(n={}){let{connector:t=()=>new Y,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=n;return o=>{let s,a,c,l=0,d=!1,u=!1,g=()=>{a?.unsubscribe(),a=void 0},f=()=>{g(),s=c=void 0,d=u=!1},v=()=>{let E=s;f(),E?.unsubscribe()};return oe((E,D)=>{l++,!u&&!d&&g();let I=c=c??t();D.add(()=>{l--,l===0&&!u&&!d&&(a=gd(v,r))}),I.subscribe(D),!s&&l>0&&(s=new _n({next:ee=>I.next(ee),error:ee=>{u=!0,g(),a=gd(f,e,ee),I.error(ee)},complete:()=>{d=!0,g(),a=gd(f,i),I.complete()}}),Ie(E).subscribe(s))})(o)}}function gd(n,t,...e){if(t===!0){n();return}if(t===!1)return;let i=new _n({next:()=>{i.unsubscribe(),n()}});return Ie(t(...e)).subscribe(i)}function vd(n,t,e){let i,r=!1;return n&&typeof n=="object"?{bufferSize:i=1/0,windowTime:t=1/0,refCount:r=!1,scheduler:e}=n:i=n??1/0,po({connector:()=>new ea(i,t,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function yd(n){return Mt((t,e)=>n<=e)}function bd(...n){let t=Zn(n);return oe((e,i)=>{(t?fd(n,e,t):fd(n,e)).subscribe(i)})}function Si(n,t){return oe((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(he(i,c=>{r?.unsubscribe();let l=0,d=o++;Ie(n(c,d)).subscribe(r=he(i,u=>i.next(t?t(c,u,d,l++):u),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Mi(n){return oe((t,e)=>{Ie(n).subscribe(he(e,()=>e.complete(),Ei)),!e.closed&&t.subscribe(e)})}function ho(n,t,e){let i=B(n)||t||e?{next:n,error:t,complete:e}:n;return i?oe((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(he(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Qn}var _a="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",_=class extends Error{code;constructor(t,e){super(Jn(t,e)),this.code=t}};function yw(n){return`NG0${Math.abs(n)}`}function Jn(n,t){return`${yw(n)}${t?": "+t:""}`}function le(n){for(let t in n)if(n[t]===le)return t;throw Error("")}function Dh(n,t){for(let e in t)t.hasOwnProperty(e)&&!n.hasOwnProperty(e)&&(n[e]=t[e])}function Ea(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Ea).join(", ")}]`;if(n==null)return""+n;let t=n.overriddenName||n.name;if(t)return`${t}`;let e=n.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function wa(n,t){return n?t?`${n} ${t}`:n:t||""}var bw=le({__forward_ref__:le});function _t(n){return n.__forward_ref__=_t,n}function Ge(n){return kd(n)?n():n}function kd(n){return typeof n=="function"&&n.hasOwnProperty(bw)&&n.__forward_ref__===_t}function L(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function W(n){return{providers:n.providers||[],imports:n.imports||[]}}function Da(n){return _w(n,Ca)}function _w(n,t){return n.hasOwnProperty(t)&&n[t]||null}function Ew(n){let t=n?.[Ca]??null;return t||null}function Ed(n){return n&&n.hasOwnProperty(ha)?n[ha]:null}var Ca=le({\u0275prov:le}),ha=le({\u0275inj:le}),w=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,e){this._desc=t,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=L({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Rd(n){return n&&!!n.\u0275providers}var Fd=le({\u0275cmp:le}),Od=le({\u0275dir:le}),Pd=le({\u0275pipe:le});var vo=le({\u0275fac:le}),Ri=le({__NG_ELEMENT_ID__:le}),bh=le({__NG_ENV_ID__:le});function ei(n){return Vd(n,"@Component"),n[Fd]||null}function Ld(n){return Vd(n,"@Directive"),n[Od]||null}function Ch(n){return Vd(n,"@Pipe"),n[Pd]||null}function Vd(n,t){if(n==null)throw new _(-919,!1)}function Eo(n){return typeof n=="string"?n:n==null?"":String(n)}var xh=le({ngErrorCode:le}),ww=le({ngErrorMessage:le}),Dw=le({ngTokenPath:le});function jd(n,t){return Ih("",-200,t)}function xa(n,t){throw new _(-201,!1)}function Ih(n,t,e){let i=new _(t,n);return i[xh]=t,i[ww]=n,e&&(i[Dw]=e),i}function Cw(n){return n[xh]}var wd;function Sh(){return wd}function at(n){let t=wd;return wd=n,t}function Bd(n,t,e){let i=Da(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(t!==void 0)return t;xa(n,"")}var an=globalThis;var xw={},Ti=xw,Iw="__NG_DI_FLAG__",Dd=class{injector;constructor(t){this.injector=t}retrieve(t,e){let i=Ai(e)||0;try{return this.injector.get(t,i&8?null:Ti,i)}catch(r){if(ur(r))return r;throw r}}};function Sw(n,t=0){let e=qs();if(e===void 0)throw new _(-203,!1);if(e===null)return Bd(n,void 0,t);{let i=Mw(t),r=e.retrieve(n,i);if(ur(r)){if(i.optional)return null;throw r}return r}}function T(n,t=0){return(Sh()||Sw)(Ge(n),t)}function p(n,t){return T(n,Ai(t))}function Ai(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Mw(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Cd(n){let t=[];for(let e=0;e<n.length;e++){let i=Ge(n[e]);if(Array.isArray(i)){if(i.length===0)throw new _(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=Tw(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}t.push(T(r,o))}else t.push(T(i))}return t}function Tw(n){return n[Iw]}function Yn(n,t){let e=n.hasOwnProperty(vo);return e?n[vo]:null}function Mh(n,t,e){if(n.length!==t.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],o=t[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Th(n){return n.flat(Number.POSITIVE_INFINITY)}function Ia(n,t){n.forEach(e=>Array.isArray(e)?Ia(e,t):t(e))}function Hd(n,t,e){t>=n.length?n.push(e):n.splice(t,0,e)}function wo(n,t){return t>=n.length-1?n.pop():n.splice(t,1)[0]}function Ah(n,t){let e=[];for(let i=0;i<n;i++)e.push(t);return e}function Nh(n,t,e,i){let r=n.length;if(r==t)n.push(e,i);else if(r===1)n.push(i,n[0]),n[0]=e;else{for(r--,n.push(n[r-1],n[r]);r>t;){let o=r-2;n[r]=n[o],r--}n[t]=e,n[t+1]=i}}function Sa(n,t,e){let i=yr(n,t);return i>=0?n[i|1]=e:(i=~i,Nh(n,i,t,e)),i}function Ma(n,t){let e=yr(n,t);if(e>=0)return n[e|1]}function yr(n,t){return Aw(n,t,1)}function Aw(n,t,e){let i=0,r=n.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=n[o<<e];if(t===s)return o<<e;s>t?r=o:i=o+1}return~(r<<e)}var ti={},Xe=[],br=new w(""),Do=new w("",-1),Ud=new w(""),vr=class{get(t,e=Ti){if(e===Ti){let r=Ih("",-201);throw r.name="\u0275NotFound",r}return e}};function ni(n){return{\u0275providers:n}}function Ta(...n){return{\u0275providers:$d(!0,n),\u0275fromNgModule:!0}}function $d(n,...t){let e=[],i=new Set,r,o=s=>{e.push(s)};return Ia(t,s=>{let a=s;ga(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&kh(r,o),e}function kh(n,t){for(let e=0;e<n.length;e++){let{ngModule:i,providers:r}=n[e];zd(r,o=>{t(o,i)})}}function ga(n,t,e,i){if(n=Ge(n),!n)return!1;let r=null,o=Ed(n),s=!o&&ei(n);if(!o&&!s){let c=n.ngModule;if(o=Ed(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=n}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)ga(l,t,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Ia(o.imports,d=>{ga(d,t,e,i)&&(l||=[],l.push(d))}),l!==void 0&&kh(l,t)}if(!a){let l=Yn(r)||(()=>new r);t({provide:r,useFactory:l,deps:Xe},r),t({provide:Ud,useValue:r,multi:!0},r),t({provide:br,useValue:()=>T(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=n;zd(c,d=>{t(d,l)})}}else return!1;return r!==n&&n.providers!==void 0}function zd(n,t){for(let e of n)Rd(e)&&(e=e.\u0275providers),Array.isArray(e)?zd(e,t):t(e)}var Nw=le({provide:String,useValue:le});function Rh(n){return n!==null&&typeof n=="object"&&Nw in n}function kw(n){return!!(n&&n.useExisting)}function Rw(n){return!!(n&&n.useFactory)}function Ni(n){return typeof n=="function"}function Fh(n){return!!n.useClass}var Co=new w(""),pa={},_h={},_d;function _r(){return _d===void 0&&(_d=new vr),_d}var We=class{},ki=class extends We{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Id(t,s=>this.processProvider(s)),this.records.set(Do,gr(void 0,this)),r.has("environment")&&this.records.set(We,gr(void 0,this));let o=this.records.get(Co);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Ud,Xe,{self:!0}))}retrieve(t,e){let i=Ai(e)||0;try{return this.get(t,Ti,i)}catch(r){if(ur(r))return r;throw r}}destroy(){go(this),this._destroyed=!0;let t=F(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),F(t)}}onDestroy(t){return go(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){go(this);let e=on(this),i=at(void 0),r;try{return t()}finally{on(e),at(i)}}get(t,e=Ti,i){if(go(this),t.hasOwnProperty(bh))return t[bh](this);let r=Ai(i),o,s=on(this),a=at(void 0);try{if(!(r&4)){let l=this.records.get(t);if(l===void 0){let d=Vw(t)&&Da(t);d&&this.injectableDefInScope(d)?l=gr(xd(t),pa):l=null,this.records.set(t,l)}if(l!=null)return this.hydrate(t,l,r)}let c=r&2?_r():this.parent;return e=r&8&&e===Ti?null:e,c.get(t,e)}catch(c){let l=Cw(c);throw l===-200||l===-201?new _(l,null):c}finally{at(a),on(s)}}resolveInjectorInitializers(){let t=F(null),e=on(this),i=at(void 0),r;try{let o=this.get(br,Xe,{self:!0});for(let s of o)s()}finally{on(e),at(i),F(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=Ge(t);let e=Ni(t)?t:Ge(t&&t.provide),i=Ow(t);if(!Ni(t)&&t.multi===!0){let r=this.records.get(e);r||(r=gr(void 0,pa,!0),r.factory=()=>Cd(r.multi),this.records.set(e,r)),e=t,r.multi.push(t)}this.records.set(e,i)}hydrate(t,e,i){let r=F(null);try{if(e.value===_h)throw jd("");return e.value===pa&&(e.value=_h,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Lw(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{F(r)}}injectableDefInScope(t){if(!t.providedIn)return!1;let e=Ge(t.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(t){let e=this._onDestroyHooks.indexOf(t);e!==-1&&this._onDestroyHooks.splice(e,1)}};function xd(n){let t=Da(n),e=t!==null?t.factory:Yn(n);if(e!==null)return e;if(n instanceof w)throw new _(-204,!1);if(n instanceof Function)return Fw(n);throw new _(-204,!1)}function Fw(n){if(n.length>0)throw new _(-204,!1);let e=Ew(n);return e!==null?()=>e.factory(n):()=>new n}function Ow(n){if(Rh(n))return gr(void 0,n.useValue);{let t=Gd(n);return gr(t,pa)}}function Gd(n,t,e){let i;if(Ni(n)){let r=Ge(n);return Yn(r)||xd(r)}else if(Rh(n))i=()=>Ge(n.useValue);else if(Rw(n))i=()=>n.useFactory(...Cd(n.deps||[]));else if(kw(n))i=(r,o)=>T(Ge(n.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Ge(n&&(n.useClass||n.provide));if(Pw(n))i=()=>new r(...Cd(n.deps));else return Yn(r)||xd(r)}return i}function go(n){if(n.destroyed)throw new _(-205,!1)}function gr(n,t,e=!1){return{factory:n,value:t,multi:e?[]:void 0}}function Pw(n){return!!n.deps}function Lw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Vw(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Id(n,t){for(let e of n)Array.isArray(e)?Id(e,t):e&&Rd(e)?Id(e.\u0275providers,t):t(e)}function Er(n,t){let e;n instanceof ki?(go(n),e=n):e=new Dd(n);let i,r=on(e),o=at(void 0);try{return t()}finally{on(r),at(o)}}function Oh(){return Sh()!==void 0||qs()!=null}var Gt=0,A=1,R=2,Ue=3,Tt=4,Je=5,wr=6,Dr=7,qe=8,Sn=9,cn=10,Ee=11,Cr=12,Wd=13,Fi=14,mt=15,ii=16,Oi=17,ln=18,Mn=19,qd=20,xn=21,Aa=22,Xn=23,Et=24,Pi=25,ri=26,Te=27,Ph=1;var oi=7,xo=8,Li=9,Ke=10;function Tn(n){return Array.isArray(n)&&typeof n[Ph]=="object"}function At(n){return Array.isArray(n)&&n[Ph]===!0}function Kd(n){return(n.flags&4)!==0}function An(n){return n.componentOffset>-1}function Io(n){return(n.flags&1)===1}function dn(n){return!!n.template}function xr(n){return(n[R]&512)!==0}function Vi(n){return(n[R]&256)===256}var Qd="svg",Lh="math";function Nt(n){for(;Array.isArray(n);)n=n[Gt];return n}function Zd(n,t){return Nt(t[n])}function kt(n,t){return Nt(t[n.index])}function Na(n,t){return n.data[t]}function Yd(n,t){return n[t]}function Xd(n,t,e,i){e>=n.data.length&&(n.data[e]=null,n.blueprint[e]=null),t[e]=i}function Rt(n,t){let e=t[n];return Tn(e)?e:e[Gt]}function Vh(n){return(n[R]&4)===4}function ka(n){return(n[R]&128)===128}function jh(n){return At(n[Ue])}function un(n,t){return t==null?null:n[t]}function Jd(n){n[Oi]=0}function eu(n){n[R]&1024||(n[R]|=1024,ka(n)&&ji(n))}function Bh(n,t){for(;n>0;)t=t[Fi],n--;return t}function So(n){return!!(n[R]&9216||n[Et]?.dirty)}function Ra(n){n[cn].changeDetectionScheduler?.notify(8),n[R]&64&&(n[R]|=1024),So(n)&&ji(n)}function ji(n){n[cn].changeDetectionScheduler?.notify(0);let t=In(n);for(;t!==null&&!(t[R]&8192||(t[R]|=8192,!ka(t)));)t=In(t)}function Fa(n,t){if(Vi(n))throw new _(911,!1);n[xn]===null&&(n[xn]=[]),n[xn].push(t)}function Hh(n,t){if(n[xn]===null)return;let e=n[xn].indexOf(t);e!==-1&&n[xn].splice(e,1)}function In(n){let t=n[Ue];return At(t)?t[Ue]:t}function tu(n){return n[Dr]??=[]}function nu(n){return n.cleanup??=[]}function Uh(n,t,e,i){let r=tu(t);r.push(e),n.firstCreatePass&&nu(n).push(i,r.length-1)}var j={lFrame:tg(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Sd=!1;function $h(){return j.lFrame.elementDepthCount}function zh(){j.lFrame.elementDepthCount++}function iu(){j.lFrame.elementDepthCount--}function ru(){return j.bindingsEnabled}function ou(){return j.skipHydrationRootTNode!==null}function su(n){return j.skipHydrationRootTNode===n}function au(){j.skipHydrationRootTNode=null}function V(){return j.lFrame.lView}function Ae(){return j.lFrame.tView}function Ve(n){return j.lFrame.contextLView=n,n[qe]}function je(n){return j.lFrame.contextLView=null,n}function $e(){let n=cu();for(;n!==null&&n.type===64;)n=n.parent;return n}function cu(){return j.lFrame.currentTNode}function Gh(){let n=j.lFrame,t=n.currentTNode;return n.isParent?t:t.parent}function Ir(n,t){let e=j.lFrame;e.currentTNode=n,e.isParent=t}function lu(){return j.lFrame.isParent}function du(){j.lFrame.isParent=!1}function Wh(){return j.lFrame.contextLView}function uu(){return Sd}function yo(n){let t=Sd;return Sd=n,t}function qh(){let n=j.lFrame,t=n.bindingRootIndex;return t===-1&&(t=n.bindingRootIndex=n.tView.bindingStartIndex),t}function Kh(){return j.lFrame.bindingIndex}function Qh(n){return j.lFrame.bindingIndex=n}function Sr(){return j.lFrame.bindingIndex++}function Oa(n){let t=j.lFrame,e=t.bindingIndex;return t.bindingIndex=t.bindingIndex+n,e}function Zh(){return j.lFrame.inI18n}function Yh(n,t){let e=j.lFrame;e.bindingIndex=e.bindingRootIndex=n,Pa(t)}function Xh(){return j.lFrame.currentDirectiveIndex}function Pa(n){j.lFrame.currentDirectiveIndex=n}function Jh(n){let t=j.lFrame.currentDirectiveIndex;return t===-1?null:n[t]}function La(){return j.lFrame.currentQueryIndex}function Mo(n){j.lFrame.currentQueryIndex=n}function jw(n){let t=n[A];return t.type===2?t.declTNode:t.type===1?n[Je]:null}function fu(n,t,e){if(e&4){let r=t,o=n;for(;r=r.parent,r===null&&!(e&1);)if(r=jw(o),r===null||(o=o[Fi],r.type&10))break;if(r===null)return!1;t=r,n=o}let i=j.lFrame=eg();return i.currentTNode=t,i.lView=n,!0}function Va(n){let t=eg(),e=n[A];j.lFrame=t,t.currentTNode=e.firstChild,t.lView=n,t.tView=e,t.contextLView=n,t.bindingIndex=e.bindingStartIndex,t.inI18n=!1}function eg(){let n=j.lFrame,t=n===null?null:n.child;return t===null?tg(n):t}function tg(n){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=t),t}function ng(){let n=j.lFrame;return j.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var mu=ng;function ja(){let n=ng();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function ig(n){return(j.lFrame.contextLView=Bh(n,j.lFrame.contextLView))[qe]}function Nn(){return j.lFrame.selectedIndex}function si(n){j.lFrame.selectedIndex=n}function To(){let n=j.lFrame;return Na(n.tView,n.selectedIndex)}function Ao(){j.lFrame.currentNamespace=Qd}function pu(){return j.lFrame.currentNamespace}var rg=!0;function Ba(){return rg}function Ha(n){rg=n}function Md(n,t=null,e=null,i){let r=og(n,t,e,i);return r.resolveInjectorInitializers(),r}function og(n,t=null,e=null,i,r=new Set){let o=[e||Xe,Ta(n)],s;return new ki(o,t||_r(),s||null,r)}var Se=class n{static THROW_IF_NOT_FOUND=Ti;static NULL=new vr;static create(t,e){if(Array.isArray(t))return Md({name:""},e,t,"");{let i=t.name??"";return Md({name:i},t.parent,t.providers,i)}}static \u0275prov=L({token:n,providedIn:"any",factory:()=>T(Do)});static __NG_ELEMENT_ID__=-1},X=new w(""),fn=(()=>{class n{static __NG_ELEMENT_ID__=Bw;static __NG_ENV_ID__=e=>e}return n})(),va=class extends fn{_lView;constructor(t){super(),this._lView=t}get destroyed(){return Vi(this._lView)}onDestroy(t){let e=this._lView;return Fa(e,t),()=>Hh(e,t)}};function Bw(){return new va(V())}var sg=!1,ag=new w(""),Bi=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Ci(!1);debugTaskTracker=p(ag,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Q(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=L({token:n,providedIn:"root",factory:()=>new n})}return n})(),Td=class extends Y{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,Oh()&&(this.destroyRef=p(fn,{optional:!0})??void 0,this.pendingTasks=p(Bi,{optional:!0})??void 0)}emit(t){let e=F(null);try{super.next(t)}finally{F(e)}}subscribe(t,e,i){let r=t,o=e||(()=>null),s=i;if(t&&typeof t=="object"){let c=t;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return t instanceof ke&&t.add(a),a}wrapInTimeout(t){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{t(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},ye=Td;function ya(...n){}function hu(n){let t,e;function i(){n=ya;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),t!==void 0&&clearTimeout(t)}catch(r){}}return t=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{n(),i()})),()=>i()}function cg(n){return queueMicrotask(()=>n()),()=>{n=ya}}var gu="isAngularZone",bo=gu+"_ID",Hw=0,P=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ye(!1);onMicrotaskEmpty=new ye(!1);onStable=new ye(!1);onError=new ye(!1);constructor(t){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=sg}=t;if(typeof Zone>"u")throw new _(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,zw(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(gu)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new _(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new _(909,!1)}run(t,e,i){return this._inner.run(t,e,i)}runTask(t,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,t,Uw,ya,ya);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(t,e,i){return this._inner.runGuarded(t,e,i)}runOutsideAngular(t){return this._outer.run(t)}},Uw={};function vu(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function $w(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function t(){hu(()=>{n.callbackScheduled=!1,Ad(n),n.isCheckStableRunning=!0,vu(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{t()}):n._outer.run(()=>{t()}),Ad(n)}function zw(n){let t=()=>{$w(n)},e=Hw++;n._inner=n._inner.fork({name:"angular",properties:{[gu]:!0,[bo]:e,[bo+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(Gw(c))return i.invokeTask(o,s,a,c);try{return Eh(n),i.invokeTask(o,s,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&t(),wh(n)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return Eh(n),i.invoke(o,s,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!Ww(c)&&t(),wh(n)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,Ad(n),vu(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function Ad(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Eh(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function wh(n){n._nesting--,vu(n)}var _o=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ye;onMicrotaskEmpty=new ye;onStable=new ye;onError=new ye;run(t,e,i){return t.apply(e,i)}runGuarded(t,e,i){return t.apply(e,i)}runOutsideAngular(t){return t()}runTask(t,e,i,r){return t.apply(e,i)}};function Gw(n){return lg(n,"__ignore_ng_zone__")}function Ww(n){return lg(n,"__scheduler_tick__")}function lg(n,t){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[t]===!0}var ft=class{_console=console;handleError(t){this._console.error("ERROR",t)}},ai=new w("",{factory:()=>{let n=p(P),t=p(We),e;return i=>{n.runOutsideAngular(()=>{t.destroyed&&!e?setTimeout(()=>{throw i}):(e??=t.get(ft),e.handleError(i))})}}}),dg={provide:br,useValue:()=>{let n=p(ft,{optional:!0})},multi:!0};function ge(n,t){let[e,i,r]=Xl(n,t?.equal),o=e,s=o[Le];return o.set=i,o.update=r,o.asReadonly=ug.bind(o),o}function ug(){let n=this[Le];if(n.readonlyFn===void 0){let t=()=>this();t[Le]=n,n.readonlyFn=t}return n.readonlyFn}var Hi=new w("",{factory:()=>qw}),qw="ng";var Ua=new w(""),Ui=new w("",{providedIn:"platform",factory:()=>"unknown"}),Mr=new w(""),$i=new w("",{factory:()=>p(X).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var No=(()=>{class n{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Kw}return n})();function Kw(){return new No(V(),$e())}var sn=class{},ko=new w("",{factory:()=>!0});var yu=new w(""),$a=(()=>{class n{static \u0275prov=L({token:n,providedIn:"root",factory:()=>new Nd})}return n})(),Nd=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let e=t.zone,i=this.queues.get(e);i.has(t)&&(i.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let e=t.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(t)||i.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[e,i]of this.queues)e===null?t||=this.flushQueue(i):t||=e.run(()=>this.flushQueue(i));t||(this.dirtyEffectCount=0)}}flushQueue(t){let e=!1;for(let i of t)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},ba=class{[Le];constructor(t){this[Le]=t}destroy(){this[Le].destroy()}};function zi(n,t){let e=t?.injector??p(Se),i=t?.manualCleanup!==!0?e.get(fn):null,r,o=e.get(No,null,{optional:!0}),s=e.get(sn);return o!==null?(r=Yw(o.view,s,n),i instanceof va&&i._lView===o.view&&(i=null)):r=Xw(n,e.get($a),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new ba(r)}var fg=G(C({},ed),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=yo(!1);try{td(this)}finally{yo(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=F(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],F(n)}}}),Qw=G(C({},fg),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),Zw=G(C({},fg),{consumerMarkedDirty(){this.view[R]|=8192,ji(this.view),this.notifier.notify(13)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Xn]?.delete(this)}});function Yw(n,t,e){let i=Object.create(Zw);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=t,i.fn=mg(i,e),n[Xn]??=new Set,n[Xn].add(i),i.consumerMarkedDirty(i),i}function Xw(n,t,e){let i=Object.create(Qw);return i.fn=mg(i,n),i.scheduler=t,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function mg(n,t){return()=>{t(e=>(n.cleanupFns??=[]).push(e))}}function Ro(n){return typeof n=="function"&&n[Le]!==void 0}var za=(()=>{class n{internalPendingTasks=p(Bi);scheduler=p(sn);errorHandler=p(ai);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=L({token:n,providedIn:"root",factory:()=>new n})}return n})();function zo(n){return{toString:n}.toString()}var Xa=class{previousValue;currentValue;firstChange;constructor(t,e,i){this.previousValue=t,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function qg(n,t,e,i){t!==null?t.applyValueToInputSignal(t,i):n[e]=i}var Ft=(()=>{let n=()=>Kg;return n.ngInherit=!0,n})();function Kg(n){return n.type.prototype.ngOnChanges&&(n.setInput=pD),mD}function mD(){let n=Qg(this),t=n?.current;if(t){let e=n.previous;if(e===ti)n.previous=t;else for(let i in t)e[i]=t[i];n.current=null,this.ngOnChanges(t)}}function pD(n,t,e,i,r){let o=this.declaredInputs[i],s=Qg(n)||hD(n,{previous:ti,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Xa(l&&l.currentValue,e,c===ti),qg(n,t,r,e)}var Tu="__ngSimpleChanges__";function Qg(n){return Object.hasOwn(n,Tu)&&n[Tu]||null}function hD(n,t){return n[Tu]=t}var pg=[];var se=function(n,t=null,e){for(let i=0;i<pg.length;i++){let r=pg[i];r(n,t,e)}},ne=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ne||{});function gD(n,t,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=t.type.prototype;if(i){let s=Kg(t);(e.preOrderHooks??=[]).push(n,s),(e.preOrderCheckHooks??=[]).push(n,s)}r&&(e.preOrderHooks??=[]).push(0-n,r),o&&((e.preOrderHooks??=[]).push(n,o),(e.preOrderCheckHooks??=[]).push(n,o))}function Zg(n,t){for(let e=t.directiveStart,i=t.directiveEnd;e<i;e++){let o=n.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(n.contentHooks??=[]).push(-e,s),a&&((n.contentHooks??=[]).push(e,a),(n.contentCheckHooks??=[]).push(e,a)),c&&(n.viewHooks??=[]).push(-e,c),l&&((n.viewHooks??=[]).push(e,l),(n.viewCheckHooks??=[]).push(e,l)),d!=null&&(n.destroyHooks??=[]).push(e,d)}}function Ka(n,t,e){Yg(n,t,3,e)}function Qa(n,t,e,i){(n[R]&3)===e&&Yg(n,t,e,i)}function bu(n,t){let e=n[R];(e&3)===t&&(e&=16383,e+=1,n[R]=e)}function Yg(n,t,e,i){let r=i!==void 0?n[Oi]&65535:0,o=i??-1,s=t.length-1,a=0;for(let c=r;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],i!=null&&a>=i)break}else t[c]<0&&(n[Oi]+=65536),(a<o||o==-1)&&(vD(n,e,t,c),n[Oi]=(n[Oi]&4294901760)+c+2),c++}function hg(n,t){se(ne.LifecycleHookStart,n,t);let e=F(null);try{t.call(n)}finally{F(e),se(ne.LifecycleHookEnd,n,t)}}function vD(n,t,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=n[s];r?n[R]>>14<n[Oi]>>16&&(n[R]&3)===t&&(n[R]+=16384,hg(a,o)):hg(a,o)}var Ar=-1,Gi=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,e,i,r){this.factory=t,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function yD(n){return(n.flags&8)!==0}function bD(n){return(n.flags&16)!==0}function _D(n,t,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];n.setAttribute(t,s,a,o)}else{let o=r,s=e[++i];ED(o)?n.setProperty(t,o,s):n.setAttribute(t,o,s),i++}}return i}function Xg(n){return n===3||n===4||n===6}function ED(n){return n.charCodeAt(0)===64}function kr(n,t){if(!(t===null||t.length===0))if(n===null||n.length===0)n=t.slice();else{let e=-1;for(let i=0;i<t.length;i++){let r=t[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?gg(n,e,r,null,t[++i]):gg(n,e,r,null,null))}}return n}function gg(n,t,e,i,r){let o=0,s=n.length;if(t===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===e){r!==null&&(n[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(n.splice(s,0,t),o=s+1),n.splice(o++,0,e),r!==null&&n.splice(o++,0,r)}function Jg(n){return n!==Ar}function Ja(n){return n&32767}function wD(n){return n>>16}function ec(n,t){let e=wD(n),i=t;for(;e>0;)i=i[Fi],e--;return i}var Au=!0;function tc(n){let t=Au;return Au=n,t}var DD=256,ev=DD-1,tv=5,CD=0,mn={};function xD(n,t,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Ri)&&(i=e[Ri]),i==null&&(i=e[Ri]=CD++);let r=i&ev,o=1<<r;t.data[n+(r>>tv)]|=o}function nc(n,t){let e=nv(n,t);if(e!==-1)return e;let i=t[A];i.firstCreatePass&&(n.injectorIndex=t.length,_u(i.data,n),_u(t,null),_u(i.blueprint,null));let r=sf(n,t),o=n.injectorIndex;if(Jg(r)){let s=Ja(r),a=ec(r,t),c=a[A].data;for(let l=0;l<8;l++)t[o+l]=a[s+l]|c[s+l]}return t[o+8]=r,o}function _u(n,t){n.push(0,0,0,0,0,0,0,0,t)}function nv(n,t){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||t[n.injectorIndex+8]===null?-1:n.injectorIndex}function sf(n,t){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let e=0,i=null,r=t;for(;r!==null;){if(i=av(r),i===null)return Ar;if(e++,r=r[Fi],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ar}function Nu(n,t,e){xD(n,t,e)}function ID(n,t){if(t==="class")return n.classes;if(t==="style")return n.styles;let e=n.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Xg(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===t)return e[r+1];r=r+2}}}return null}function iv(n,t,e){if(e&8||n!==void 0)return n;xa(t,"NodeInjector")}function rv(n,t,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=n[Sn],o=at(void 0);try{return r?r.get(t,i,e&8):Bd(t,i,e&8)}finally{at(o)}}return iv(i,t,e)}function ov(n,t,e,i=0,r){if(n!==null){if(t[R]&2048&&!(i&2)){let s=AD(n,t,e,i,mn);if(s!==mn)return s}let o=sv(n,t,e,i,mn);if(o!==mn)return o}return rv(t,e,i,r)}function sv(n,t,e,i,r){let o=MD(e);if(typeof o=="function"){if(!fu(t,n,i))return i&1?iv(r,e,i):rv(t,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))xa(e);else return s}finally{mu()}}else if(typeof o=="number"){let s=null,a=nv(n,t),c=Ar,l=i&1?t[mt][Je]:null;for((a===-1||i&4)&&(c=a===-1?sf(n,t):t[a+8],c===Ar||!yg(i,!1)?a=-1:(s=t[A],a=Ja(c),t=ec(c,t)));a!==-1;){let d=t[A];if(vg(o,a,d.data)){let u=SD(a,t,e,s,i,l);if(u!==mn)return u}c=t[a+8],c!==Ar&&yg(i,t[A].data[a+8]===l)&&vg(o,a,t)?(s=d,a=Ja(c),t=ec(c,t)):a=-1}}return r}function SD(n,t,e,i,r,o){let s=t[A],a=s.data[n+8],c=i==null?An(a)&&Au:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Za(a,s,e,c,l);return d!==null?Vo(t,s,d,a,r):mn}function Za(n,t,e,i,r){let o=n.providerIndexes,s=t.data,a=o&1048575,c=n.directiveStart,l=n.directiveEnd,d=o>>20,u=i?a:a+d,g=r?a+d:l;for(let f=u;f<g;f++){let v=s[f];if(f<c&&e===v||f>=c&&v.type===e)return f}if(r){let f=s[c];if(f&&dn(f)&&f.type===e)return c}return null}function Vo(n,t,e,i,r){let o=n[e],s=t.data;if(o instanceof Gi){let a=o;if(a.resolving)throw jd("");let c=tc(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,u=a.injectImpl?at(a.injectImpl):null,g=fu(n,i,0);try{o=n[e]=a.factory(void 0,r,s,n,i),t.firstCreatePass&&e>=i.directiveStart&&gD(e,s[e],t)}finally{u!==null&&at(u),tc(c),a.resolving=!1,mu()}}return o}function MD(n){if(typeof n=="string")return n.charCodeAt(0)||0;let t=n.hasOwnProperty(Ri)?n[Ri]:void 0;return typeof t=="number"?t>=0?t&ev:TD:t}function vg(n,t,e){let i=1<<n;return!!(e[t+(n>>tv)]&i)}function yg(n,t){return!(n&2)&&!(n&1&&t)}var ci=class{_tNode;_lView;constructor(t,e){this._tNode=t,this._lView=e}get(t,e,i){return ov(this._tNode,this._lView,t,Ai(i),e)}};function TD(){return new ci($e(),V())}function Qi(n){return zo(()=>{let t=n.prototype.constructor,e=t[vo]||ku(t),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let o=r[vo]||ku(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function ku(n){return kd(n)?()=>{let t=ku(Ge(n));return t&&t()}:Yn(n)}function AD(n,t,e,i,r){let o=n,s=t;for(;o!==null&&s!==null&&s[R]&2048&&!xr(s);){let a=sv(o,s,e,i|2,mn);if(a!==mn)return a;let c=o.parent;if(!c){let l=s[qd];if(l){let d=l.get(e,mn,i&-5);if(d!==mn)return d}c=av(s),s=s[Fi]}o=c}return r}function av(n){let t=n[A],e=t.type;return e===2?t.declTNode:e===1?n[Je]:null}function af(n){return ID($e(),n)}function J(n){return{token:n.token,providedIn:n.autoProvided===!1?null:"root",factory:n.factory,value:void 0}}function ND(){return Pr($e(),V())}function Pr(n,t){return new re(kt(n,t))}var re=(()=>{class n{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=ND}return n})();function cv(n){return n instanceof re?n.nativeElement:n}function kD(){return this._results[Symbol.iterator]()}var jo=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Y}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,e){return this._results.reduce(t,e)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,e){this.dirty=!1;let i=Th(t);(this._changesDetected=!Mh(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=kD};function lv(n){return(n.flags&128)===128}var cf=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(cf||{}),dv=new Map,RD=0;function FD(){return RD++}function OD(n){dv.set(n[Mn],n)}function Ru(n){dv.delete(n[Mn])}var bg="__ngContext__";function Rr(n,t){Tn(t)?(n[bg]=t[Mn],OD(t)):n[bg]=t}function uv(n){return mv(n[Cr])}function fv(n){return mv(n[Tt])}function mv(n){for(;n!==null&&!At(n);)n=n[Tt];return n}var Fu;function lf(n){Fu=n}function pv(){if(Fu!==void 0)return Fu;if(typeof document<"u")return document;throw new _(210,!1)}var hv=!1,gv=new w("",{factory:()=>hv});var _g=new WeakMap;function PD(n,t){if(n==null||typeof n!="object")return;let e=_g.get(n);e||(e=new WeakSet,_g.set(n,e)),e.add(t)}var LD=(n,t,e,i)=>{};function VD(n,t,e,i){LD(n,t,e,i)}function pc(n){return(n.flags&32)===32}var jD=()=>null;function vv(n,t,e=!1){return jD(n,t,e)}function yv(n,t){let e=n.contentQueries;if(e!==null){let i=F(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=n.data[s];Mo(o),a.contentQueries(2,t[s],s)}}}finally{F(i)}}}function Ou(n,t,e){Mo(0);let i=F(null);try{t(n,e)}finally{F(i)}}function bv(n,t,e){if(Kd(t)){let i=F(null);try{let r=t.directiveStart,o=t.directiveEnd;for(let s=r;s<o;s++){let a=n.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{F(i)}}}var Kt=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(Kt||{});var Ga;function BD(){if(Ga===void 0&&(Ga=null,an.trustedTypes))try{Ga=an.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch(n){}return Ga}function hc(n){return BD()?.createHTML(n)||n}var kn=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${_a})`}},Pu=class extends kn{getTypeName(){return"HTML"}},Lu=class extends kn{getTypeName(){return"Style"}},Vu=class extends kn{getTypeName(){return"Script"}},ju=class extends kn{getTypeName(){return"URL"}},Bu=class extends kn{getTypeName(){return"ResourceURL"}};function Fn(n){return n instanceof kn?n.changingThisBreaksApplicationSecurity:n}function Zi(n,t){let e=_v(n);if(e!=null&&e!==t){if(e==="ResourceURL"&&t==="URL")return!0;throw new Error(`Required a safe ${t}, got a ${e} (see ${_a})`)}return e===t}function _v(n){return n instanceof kn&&n.getTypeName()||null}function df(n){return new Pu(n)}function uf(n){return new Lu(n)}function ff(n){return new Vu(n)}function mf(n){return new ju(n)}function pf(n){return new Bu(n)}function HD(n){let t=new Uu(n);return UD()?new Hu(t):t}var Hu=class{inertDocumentHelper;constructor(t){this.inertDocumentHelper=t}getInertBodyElement(t){t="<body><remove></remove>"+t;try{let e=new window.DOMParser().parseFromString(hc(t),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(t):(e.firstChild?.remove(),e)}catch(e){return null}}},Uu=class{defaultDoc;inertDocument;constructor(t){this.defaultDoc=t,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(t){let e=this.inertDocument.createElement("template");return e.innerHTML=hc(t),e}};function UD(){try{return!!new window.DOMParser().parseFromString(hc(""),"text/html")}catch(n){return!1}}var $D=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function gc(n){return n=String(n),n.match($D)?n:"unsafe:"+n}function On(n){let t={};for(let e of n.split(","))t[e]=!0;return t}function Go(...n){let t={};for(let e of n)for(let i in e)e.hasOwnProperty(i)&&(t[i]=!0);return t}var Ev=On("area,br,col,hr,img,wbr"),wv=On("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Dv=On("rp,rt"),zD=Go(Dv,wv),GD=Go(wv,On("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),WD=Go(Dv,On("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Eg=Go(Ev,GD,WD,zD),Cv=On("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),qD=On("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),KD=On("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),QD=Go(Cv,qD,KD),ZD=On("script,style,template"),$u=class{sanitizedSomething=!1;buf=[];sanitizeChildren(t){let e=t.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=JD(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=XD(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(t){let e=wg(t).toLowerCase();if(!Eg.hasOwnProperty(e))return this.sanitizedSomething=!0,!ZD.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=t.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!QD.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;Cv[a]&&(c=gc(c)),this.buf.push(" ",s,'="',Dg(c),'"')}return this.buf.push(">"),!0}endElement(t){let e=wg(t).toLowerCase();Eg.hasOwnProperty(e)&&!Ev.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(t){this.buf.push(Dg(t))}};function YD(n,t){return(n.compareDocumentPosition(t)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function XD(n){let t=n.nextSibling;if(t&&n!==t.previousSibling)throw xv(t);return t}function JD(n){let t=n.firstChild;if(t&&YD(n,t))throw xv(t);return t}function wg(n){let t=n.nodeName;return typeof t=="string"?t:"FORM"}function xv(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var eC=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,tC=/([^\#-~ |!])/g;function Dg(n){return n.replace(/&/g,"&amp;").replace(eC,function(t){let e=t.charCodeAt(0),i=t.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(tC,function(t){return"&#"+t.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Wa;function hf(n,t){let e=null;try{Wa=Wa||HD(n);let i=t?String(t):"";e=Wa.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Wa.getInertBodyElement(i)}while(i!==o);let a=new $u().sanitizeChildren(Cg(e)||e);return hc(a)}finally{if(e){let i=Cg(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Cg(n){return"content"in n&&nC(n)?n.content:null}function nC(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}function iC(n,t){return n.createText(t)}function rC(n,t,e){n.setValue(t,e)}function Iv(n,t,e){return n.createElement(t,e)}function ic(n,t,e,i,r){n.insertBefore(t,e,i,r)}function Sv(n,t,e){n.appendChild(t,e)}function xg(n,t,e,i,r){i!==null?ic(n,t,e,i,r):Sv(n,t,e)}function oC(n,t,e,i){n.removeChild(null,t,e,i)}function sC(n,t,e){n.setAttribute(t,"style",e)}function aC(n,t,e){e===""?n.removeAttribute(t,"class"):n.setAttribute(t,"class",e)}function Mv(n,t,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&_D(n,t,i),r!==null&&aC(n,t,r),o!==null&&sC(n,t,o)}var lt=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n[n.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",n})(lt||{});function cC(n,t,e){let i=n.length;for(;;){let r=n.indexOf(t,e);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let o=t.length;if(r+o===i||n.charCodeAt(r+o)<=32)return r}e=r+1}}var Tv="ng-template";function lC(n,t,e,i){let r=0;if(i){for(;r<t.length&&typeof t[r]=="string";r+=2)if(t[r]==="class"&&cC(t[r+1].toLowerCase(),e,0)!==-1)return!0}else if(gf(n))return!1;if(r=t.indexOf(1,r),r>-1){let o;for(;++r<t.length&&typeof(o=t[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function gf(n){return n.type===4&&n.value!==Tv}function dC(n,t,e){let i=n.type===4&&!e?Tv:n.value;return t===i}function uC(n,t,e){let i=4,r=n.attrs,o=r!==null?pC(r):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!Wt(i)&&!Wt(c))return!1;if(s&&Wt(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!dC(n,c,e)||c===""&&t.length===1){if(Wt(i))return!1;s=!0}}else if(i&8){if(r===null||!lC(n,r,c,e)){if(Wt(i))return!1;s=!0}}else{let l=t[++a],d=fC(c,r,gf(n),e);if(d===-1){if(Wt(i))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&l!==u){if(Wt(i))return!1;s=!0}}}}return Wt(i)||s}function Wt(n){return(n&1)===0}function fC(n,t,e,i){if(t===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<t.length;){let s=t[r];if(s===n)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=t[++r];for(;typeof a=="string";)a=t[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return hC(t,n)}function Av(n,t,e=!1){for(let i=0;i<t.length;i++)if(uC(n,t[i],e))return!0;return!1}function mC(n){let t=n.attrs;if(t!=null){let e=t.indexOf(5);if((e&1)===0)return t[e+1]}return null}function pC(n){for(let t=0;t<n.length;t++){let e=n[t];if(Xg(e))return t}return n.length}function hC(n,t){let e=n.indexOf(4);if(e>-1)for(e++;e<n.length;){let i=n[e];if(typeof i=="number")return-1;if(i===t)return e;e++}return-1}function gC(n,t){e:for(let e=0;e<t.length;e++){let i=t[e];if(n.length===i.length){for(let r=0;r<n.length;r++)if(n[r]!==i[r])continue e;return!0}}return!1}function Ig(n,t){return n?":not("+t.trim()+")":t}function vC(n){let t=n[0],e=1,i=2,r="",o=!1;for(;e<n.length;){let s=n[e];if(typeof s=="string")if(i&2){let a=n[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!Wt(s)&&(t+=Ig(o,r),r=""),i=s,o=o||!Wt(i);e++}return r!==""&&(t+=Ig(o,r)),t}function yC(n){return n.map(vC).join(",")}function bC(n){let t=[],e=[],i=1,r=2;for(;i<n.length;){let o=n[i];if(typeof o=="string")r===2?o!==""&&t.push(o,n[++i]):r===8&&e.push(o);else{if(!Wt(r))break;r=o}i++}return e.length&&t.push(1,...e),t}var pt={},pn=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(pn||{}),_C;function vf(n,t){return _C(n,t)}var R2=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var zu=new WeakMap,Oo=new WeakSet;function EC(n,t){let e=zu.get(n);if(!e||e.length===0)return;let i=t.parentNode,r=t.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===t?(e.splice(o,1),Oo.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function wC(n,t){let e=zu.get(n);e?e.includes(t)||e.push(t):zu.set(n,[t])}var Wi=new Set,vc=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(vc||{}),Pn=new w(""),Sg=new Set;function ui(n){Sg.has(n)||(Sg.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var yf=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=L({token:n,providedIn:"root",factory:()=>new n})}return n})(),bf=[0,1,2,3],Nv=(()=>{class n{ngZone=p(P);scheduler=p(sn);errorHandler=p(ft,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){p(Pn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&se(ne.AfterRenderHooksStart),this.executing=!0;for(let i of bf)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&se(ne.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Pi]??=[]).push(e),ji(i),i[R]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(vc.AFTER_NEXT_RENDER,e):e()}static \u0275prov=L({token:n,providedIn:"root",factory:()=>new n})}return n})(),rc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,e,i,r,o,s=null){this.impl=t,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[Pi];t&&(this.view[Pi]=t.filter(e=>e!==this))}};var kv=new w("",{factory:()=>{let n=p(We),t=new Set;return n.onDestroy(()=>t.clear()),{queue:t,isScheduled:!1,scheduler:null,injector:n}}});function Rv(n,t,e){let i=n.get(kv);if(Array.isArray(t))for(let r of t)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(t),e?.detachedLeaveAnimationFns?.push(t);i.scheduler&&i.scheduler(n)}function DC(n,t){let e=n.get(kv);if(Array.isArray(t))for(let i of t)e.queue.delete(i);else e.queue.delete(t)}function CC(n,t){for(let[e,i]of t)Rv(n,i.animateFns)}function Mg(n,t,e,i){let r=n?.[ri]?.enter;t!==null&&r&&r.has(e.index)&&CC(i,r)}function Tg(n,t,e,i){try{e.get(Do)}catch(s){return i(!1)}let r=n?.[ri];r?.enter?.has(t.index)&&DC(e,r.enter.get(t.index).animateFns);let o=xC(n,t,r);if(o.size===0){let s=!1;if(n){let a=[];yc(n,t,a),s=a.length>0}if(!s)return i(!1)}n&&Wi.add(n[Mn]),Rv(e,()=>IC(n,t,r||void 0,o,i),r||void 0)}function xC(n,t,e){let i=new Map,r=e?.leave;if(r&&r.has(t.index)&&i.set(t.index,r.get(t.index)),n&&r)for(let[o,s]of r){if(i.has(o))continue;let c=n[A].data[o].parent;for(;c;){if(c===t){i.set(o,s);break}c=c.parent}}return i}function IC(n,t,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(n&&yc(n,t,o),o.length>0){let s=e||n?.[ri];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),MC(n,s.running,r)}else Promise.allSettled(o).then(()=>{n&&Wi.delete(n[Mn]),r(!0)})}else n&&Wi.delete(n[Mn]),r(!1)}function yc(n,t,e){if(t.type&12){let r=n[t.index];if(At(r))for(let o=Ke;o<r.length;o++){let s=r[o];s[A].type===2&&SC(s,e)}}let i=t.child;for(;i;)yc(n,i,e),i=i.next}function SC(n,t){let e=n[ri];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();t.push(s)}let i=n[A].firstChild;for(;i;)yc(n,i,t),i=i.next}function MC(n,t,e){t.then(()=>{n[ri]?.running===t&&(n[ri].running=void 0,Wi.delete(n[Mn])),e(!0)})}function Tr(n,t,e,i,r,o,s,a){if(r!=null){let c,l=!1;At(r)?c=r:Tn(r)&&(l=!0,r=r[Gt]);let d=Nt(r);n===0&&i!==null?(Mg(a,i,o,e),s==null?Sv(t,i,d):ic(t,i,d,s||null,!0)):n===1&&i!==null?(Mg(a,i,o,e),ic(t,i,d,s||null,!0),EC(o,d)):n===2?(a?.[ri]?.leave?.has(o.index)&&wC(o,d),Oo.delete(d),Tg(a,o,e,u=>{if(Oo.has(d)){Oo.delete(d);return}oC(t,d,l,u)})):n===3&&(Oo.delete(d),Tg(a,o,e,()=>{t.destroyNode(d)})),c!=null&&VC(t,n,e,c,o,i,s)}}function TC(n,t){Fv(n,t),t[Gt]=null,t[Je]=null}function AC(n,t,e,i,r,o){i[Gt]=r,i[Je]=t,bc(n,i,e,1,r,o)}function Fv(n,t){t[cn].changeDetectionScheduler?.notify(9),bc(n,t,t[Ee],2,null,null)}function NC(n){let t=n[Cr];if(!t)return Eu(n[A],n);for(;t;){let e=null;if(Tn(t))e=t[Cr];else{let i=t[Ke];i&&(e=i)}if(!e){for(;t&&!t[Tt]&&t!==n;)Tn(t)&&Eu(t[A],t),t=t[Ue];t===null&&(t=n),Tn(t)&&Eu(t[A],t),e=t&&t[Tt]}t=e}}function _f(n,t){let e=n[Li],i=e.indexOf(t);e.splice(i,1)}function Ef(n,t){if(Vi(t))return;let e=t[Ee];e.destroyNode&&bc(n,t,e,3,null,null),NC(t)}function Eu(n,t){if(Vi(t))return;let e=F(null);try{t[R]&=-129,t[R]|=256,t[Et]&&Kn(t[Et]),RC(n,t),kC(n,t),t[A].type===1&&t[Ee].destroy();let i=t[ii];if(i!==null&&At(t[Ue])){i!==t[Ue]&&_f(i,t);let r=t[ln];r!==null&&r.detachView(n)}Ru(t)}finally{F(e)}}function kC(n,t){let e=n.cleanup,i=t[Dr];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(t[Dr]=null);let r=t[xn];if(r!==null){t[xn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=t[Xn];if(o!==null){t[Xn]=null;for(let s of o)s.destroy()}}function RC(n,t){let e;if(n!=null&&(e=n.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=t[e[i]];if(!(r instanceof Gi)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];se(ne.LifecycleHookStart,a,c);try{c.call(a)}finally{se(ne.LifecycleHookEnd,a,c)}}else{se(ne.LifecycleHookStart,r,o);try{o.call(r)}finally{se(ne.LifecycleHookEnd,r,o)}}}}}function Ov(n,t,e){return FC(n,t.parent,e)}function FC(n,t,e){let i=t;for(;i!==null&&i.type&168;)t=i,i=t.parent;if(i===null)return e[Gt];if(An(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===Kt.None||r===Kt.Emulated)return null}return kt(i,e)}function Pv(n,t,e){return PC(n,t,e)}function OC(n,t,e){return n.type&40?kt(n,e):null}var PC=OC,Ag;function wf(n,t,e,i){let r=Ov(n,i,t),o=t[Ee],s=i.parent||t[Je],a=Pv(s,i,t);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)xg(o,r,e[c],a,!1);else xg(o,r,e,a,!1);Ag!==void 0&&Ag(o,i,t,e,r)}function Po(n,t){if(t!==null){let e=t.type;if(e&3)return kt(t,n);if(e&4)return Gu(-1,n[t.index]);if(e&8){let i=t.child;if(i!==null)return Po(n,i);{let r=n[t.index];return At(r)?Gu(-1,r):Nt(r)}}else{if(e&128)return Po(n,t.next);if(e&32)return vf(t,n)()||Nt(n[t.index]);{let i=Lv(n,t);if(i!==null){if(Array.isArray(i))return i[0];let r=In(n[mt]);return Po(r,i)}else return Po(n,t.next)}}}return null}function Lv(n,t){if(t!==null){let i=n[mt][Je],r=t.projection;return i.projection[r]}return null}function Gu(n,t){let e=Ke+n+1;if(e<t.length){let i=t[e],r=i[A].firstChild;if(r!==null)return Po(i,r)}return t[oi]}function Df(n,t,e,i,r,o,s){for(;e!=null;){let a=i[Sn];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&t===0&&(c&&Rr(Nt(c),i),e.flags|=2),!pc(e))if(l&8)Df(n,t,e.child,i,r,o,!1),Tr(t,n,a,r,c,e,o,i);else if(l&32){let d=vf(e,i),u;for(;u=d();)Tr(t,n,a,r,u,e,o,i);Tr(t,n,a,r,c,e,o,i)}else l&16?Vv(n,t,i,e,r,o):Tr(t,n,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function bc(n,t,e,i,r,o){Df(e,i,n.firstChild,t,r,o,!1)}function LC(n,t,e){let i=t[Ee],r=Ov(n,e,t),o=e.parent||t[Je],s=Pv(o,e,t);Vv(i,0,t,e,r,s)}function Vv(n,t,e,i,r,o){let s=e[mt],c=s[Je].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Tr(t,n,e[Sn],r,d,i,o,e)}else{let l=c,d=s[Ue];lv(i)&&(l.flags|=128),Df(n,t,l,d,r,o,!0)}}function VC(n,t,e,i,r,o,s){let a=i[oi],c=Nt(i);a!==c&&Tr(t,n,e,o,a,r,s);for(let l=Ke;l<i.length;l++){let d=i[l];bc(d[A],d,n,t,o,a)}}function jC(n,t,e,i,r){if(t)r?n.addClass(e,i):n.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:pn.DashCase;r==null?n.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=pn.Important),n.setStyle(e,i,r,o))}}function Cf(n,t,e,i,r,o,s,a,c,l,d){let u=Te+i,g=u+r,f=BC(u,g),v=typeof l=="function"?l():l;return f[A]={type:n,blueprint:f,template:e,queries:null,viewQuery:a,declTNode:t,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:v,incompleteFirstPass:!1,ssrId:d}}function BC(n,t){let e=[];for(let i=0;i<t;i++)e.push(i<n?null:pt);return e}function HC(n){let t=n.tView;return t===null||t.incompleteFirstPass?n.tView=Cf(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):t}function xf(n,t,e,i,r,o,s,a,c,l,d){let u=t.blueprint.slice();return u[Gt]=r,u[R]=i|4|128|8|64|1024,(l!==null||n&&n[R]&2048)&&(u[R]|=2048),Jd(u),u[Ue]=u[Fi]=n,u[qe]=e,u[cn]=s||n&&n[cn],u[Ee]=a||n&&n[Ee],u[Sn]=c||n&&n[Sn]||null,u[Je]=o,u[Mn]=FD(),u[wr]=d,u[qd]=l,u[mt]=t.type==2?n[mt]:u,u}function UC(n,t,e){let i=kt(t,n),r=HC(e),o=n[cn].rendererFactory,s=If(n,xf(n,r,null,jv(e),i,t,null,o.createRenderer(i,e),null,null,null));return n[t.index]=s}function jv(n){let t=16;return n.signals?t=4096:n.onPush&&(t=64),t}function Bv(n,t,e,i){if(e===0)return-1;let r=t.length;for(let o=0;o<e;o++)t.push(i),n.blueprint.push(i),n.data.push(null);return r}function If(n,t){return n[Cr]?n[Wd][Tt]=t:n[Cr]=t,n[Wd]=t,t}function b(n=1){Hv(Ae(),V(),Nn()+n,!1)}function Hv(n,t,e,i){if(!i)if((t[R]&3)===3){let o=n.preOrderCheckHooks;o!==null&&Ka(t,o,e)}else{let o=n.preOrderHooks;o!==null&&Qa(t,o,0,e)}si(e)}var _c=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(_c||{});function qi(n,t,e,i){let r=F(null);try{let[o,s,a]=n.inputs[e],c=null;(s&_c.SignalBased)!==0&&(c=t[o][Le]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(t,i)),n.setInput!==null?n.setInput(t,c,i,e,o):qg(t,c,o,i)}finally{F(r)}}function Uv(n,t,e,i,r){let o=Nn(),s=i&2;try{si(-1),s&&t.length>Te&&Hv(n,t,Te,!1);let a=s?ne.TemplateUpdateStart:ne.TemplateCreateStart;se(a,r,e),e(i,r)}finally{si(o);let a=s?ne.TemplateUpdateEnd:ne.TemplateCreateEnd;se(a,r,e)}}function Sf(n,t,e){QC(n,t,e),(e.flags&64)===64&&ZC(n,t,e)}function Ec(n,t,e=kt){let i=t.localNames;if(i!==null){let r=t.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(t,n):n[s];n[r++]=a}}}function $C(n,t,e,i){let o=i.get(gv,hv)||e===Kt.ShadowDom||e===Kt.ExperimentalIsolatedShadowDom,s=n.selectRootElement(t,o);if(s.tagName.toLowerCase()==="script")throw new _(905,!1);return zC(s),s}function zC(n){GC(n)}var GC=()=>null;function WC(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function qC(n,t,e,i,r,o){let s=t[A];if(Mf(n,s,t,e,i)){An(n)&&KC(t,n.index);return}n.type&3&&(e=WC(e)),$v(n,t,e,i,r,o)}function $v(n,t,e,i,r,o){if(n.type&3){let s=kt(n,t);i=o!=null?o(i,n.value||"",e):i,r.setProperty(s,e,i)}else n.type&12}function KC(n,t){let e=Rt(t,n);e[R]&16||(e[R]|=64)}function QC(n,t,e){let i=e.directiveStart,r=e.directiveEnd;An(e)&&UC(t,e,n.data[i+e.componentOffset]),n.firstCreatePass||nc(e,t);let o=e.initialInputs;for(let s=i;s<r;s++){let a=n.data[s],c=Vo(t,n,s,e);if(Rr(c,t),o!==null&&ex(t,s-i,c,a,e,o),dn(a)){let l=Rt(e.index,t);l[qe]=Vo(t,n,s,e)}}}function ZC(n,t,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Xh();try{si(o);for(let a=i;a<r;a++){let c=n.data[a],l=t[a];Pa(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&YC(c,l)}}finally{si(-1),Pa(s)}}function YC(n,t){n.hostBindings!==null&&n.hostBindings(1,t)}function zv(n,t){let e=n.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Av(t,o.selectors,!1)&&(i??=[],dn(o)?i.unshift(o):i.push(o))}return i}function XC(n,t,e,i,r,o){let s=kt(n,t);JC(t[Ee],s,o,n.value,e,i,r)}function JC(n,t,e,i,r,o,s){if(o==null)s?.(o,i||"",r),n.removeAttribute(t,r,e);else{let a=s==null?Eo(o):s(o,i||"",r);n.setAttribute(t,r,a,e)}}function ex(n,t,e,i,r,o){let s=o[t];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];qi(i,e,c,l)}}function Gv(n,t,e,i,r){let o=Te+e,s=t[A],a=r(s,t,n,i,e);t[o]=a,Ir(n,!0);let c=n.type===2;return c?(Mv(t[Ee],a,n),($h()===0||Io(n))&&Rr(a,t),zh()):Rr(a,t),Ba()&&(!c||!pc(n))&&wf(s,t,a,n),n}function Wv(n){let t=n;return lu()?du():(t=t.parent,Ir(t,!1)),t}function tx(n,t){let e=n[Sn];if(!e)return;let i;try{i=e.get(ai,null)}catch(r){i=null}i?.(t)}function Mf(n,t,e,i,r){let o=n.inputs?.[i],s=n.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=t.data[l];qi(u,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=t.data[c];qi(d,l,i,r),a=!0}return a}function nx(n,t,e,i,r,o){let s=null,a=null,c=null,l=!1,d=n.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&n.hostDirectiveInputs?.hasOwnProperty(r)){let u=n.hostDirectiveInputs[r];for(let g=0;g<u.length;g+=2){let f=u[g];if(f>=a&&f<=c){let v=t.data[f],E=u[g+1];qi(v,e[f],E,o),l=!0}else if(f>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(qi(i,e[s],r,o),l=!0),l}function ix(n,t){let e=Rt(t,n),i=e[A];rx(i,e);let r=e[Gt];r!==null&&e[wr]===null&&(e[wr]=vv(r,e[Sn])),se(ne.ComponentStart);try{Tf(i,e,e[qe])}finally{se(ne.ComponentEnd,e[qe])}}function rx(n,t){for(let e=t.length;e<n.blueprint.length;e++)t.push(n.blueprint[e])}function Tf(n,t,e){Va(t);try{let i=n.viewQuery;i!==null&&Ou(1,i,e);let r=n.template;r!==null&&Uv(n,t,r,1,e),n.firstCreatePass&&(n.firstCreatePass=!1),t[ln]?.finishViewCreation(n),n.staticContentQueries&&yv(n,t),n.staticViewQueries&&Ou(2,n.viewQuery,e);let o=n.components;o!==null&&ox(t,o)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{t[R]&=-5,ja()}}function ox(n,t){for(let e=0;e<t.length;e++)ix(n,t[e])}function Af(n,t,e,i){let r=F(null);try{let o=t.tView,a=n[R]&4096?4096:16,c=xf(n,o,e,a,null,t,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[t.index];c[ii]=l;let d=n[ln];return d!==null&&(c[ln]=d.createEmbeddedView(o)),Tf(o,c,e),c}finally{F(r)}}function oc(n,t){return!t||t.firstChild===null||lv(n)}function Bo(n,t,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=t[e.index];o!==null&&i.push(Nt(o)),At(o)&&qv(o,i);let s=e.type;if(s&8)Bo(n,t,e.child,i);else if(s&32){let a=vf(e,t),c;for(;c=a();)i.push(c)}else if(s&16){let a=Lv(t,e);if(Array.isArray(a))i.push(...a);else{let c=In(t[mt]);Bo(c[A],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function qv(n,t){for(let e=Ke;e<n.length;e++){let i=n[e],r=i[A].firstChild;r!==null&&Bo(i[A],i,r,t)}n[oi]!==n[Gt]&&t.push(n[oi])}function Kv(n){if(n[Pi]!==null){for(let t of n[Pi])t.impl.addSequence(t);n[Pi].length=0}}var Qv=[];function sx(n){return n[Et]??ax(n)}function ax(n){let t=Qv.pop()??Object.create(lx);return t.lView=n,t}function cx(n){n.lView[Et]!==n&&(n.lView=null,Qv.push(n))}var lx=G(C({},yi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{ji(n.lView)},consumerOnSignalRead(){this.lView[Et]=this}});function dx(n){let t=n[Et]??Object.create(ux);return t.lView=n,t}var ux=G(C({},yi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let t=In(n.lView);for(;t&&!Zv(t[A]);)t=In(t);t&&eu(t)},consumerOnSignalRead(){this.lView[Et]=this}});function Zv(n){return n.type!==2}function Yv(n){if(n[Xn]===null)return;let t=!0;for(;t;){let e=!1;for(let i of n[Xn])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));t=e&&!!(n[R]&8192)}}var fx=100;function Xv(n,t=0){let i=n[cn].rendererFactory,r=!1;r||i.begin?.();try{mx(n,t)}finally{r||i.end?.()}}function mx(n,t){let e=uu();try{yo(!0),Wu(n,t);let i=0;for(;So(n);){if(i===fx)throw new _(103,!1);i++,Wu(n,1)}}finally{yo(e)}}function px(n,t,e,i){if(Vi(t))return;let r=t[R],o=!1,s=!1;Va(t);let a=!0,c=null,l=null;o||(Zv(n)?(l=sx(t),c=qn(l)):$s()===null?(a=!1,l=dx(t),c=qn(l)):t[Et]&&(Kn(t[Et]),t[Et]=null));try{Jd(t),Qh(n.bindingStartIndex),e!==null&&Uv(n,t,e,2,i);let d=(r&3)===3;if(!o)if(d){let f=n.preOrderCheckHooks;f!==null&&Ka(t,f,null)}else{let f=n.preOrderHooks;f!==null&&Qa(t,f,0,null),bu(t,0)}if(s||hx(t),Yv(t),Jv(t,0),n.contentQueries!==null&&yv(n,t),!o)if(d){let f=n.contentCheckHooks;f!==null&&Ka(t,f)}else{let f=n.contentHooks;f!==null&&Qa(t,f,1),bu(t,1)}vx(n,t);let u=n.components;u!==null&&ty(t,u,0);let g=n.viewQuery;if(g!==null&&Ou(2,g,i),!o)if(d){let f=n.viewCheckHooks;f!==null&&Ka(t,f)}else{let f=n.viewHooks;f!==null&&Qa(t,f,2),bu(t,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),t[Aa]){for(let f of t[Aa])f();t[Aa]=null}o||(Kv(t),t[R]&=-73)}catch(d){throw o||ji(t),d}finally{l!==null&&(_i(l,c),a&&cx(l)),ja()}}function Jv(n,t){for(let e=uv(n);e!==null;e=fv(e))for(let i=Ke;i<e.length;i++){let r=e[i];ey(r,t)}}function hx(n){for(let t=uv(n);t!==null;t=fv(t)){if(!(t[R]&2))continue;let e=t[Li];for(let i=0;i<e.length;i++){let r=e[i];eu(r)}}}function gx(n,t,e){se(ne.ComponentStart);let i=Rt(t,n);try{ey(i,e)}finally{se(ne.ComponentEnd,i[qe])}}function ey(n,t){ka(n)&&Wu(n,t)}function Wu(n,t){let i=n[A],r=n[R],o=n[Et],s=!!(t===0&&r&16);if(s||=!!(r&64&&t===0),s||=!!(r&1024),s||=!!(o?.dirty&&cr(o)),s||=!1,o&&(o.dirty=!1),n[R]&=-9217,s)px(i,n,i.template,n[qe]);else if(r&8192){let a=F(null);try{Yv(n),Jv(n,1);let c=i.components;c!==null&&ty(n,c,1),Kv(n)}finally{F(a)}}}function ty(n,t,e){for(let i=0;i<t.length;i++)gx(n,t[i],e)}function vx(n,t){let e=n.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)si(~r);else{let o=r,s=e[++i],a=e[++i];Yh(s,o);let c=t[o];se(ne.HostBindingsUpdateStart,c);try{a(2,c)}finally{se(ne.HostBindingsUpdateEnd,c)}}}}finally{si(-1)}}function Nf(n,t){let e=uu()?64:1088;for(n[cn].changeDetectionScheduler?.notify(t);n;){n[R]|=e;let i=In(n);if(xr(n)&&!i)return n;n=i}return null}function ny(n,t,e,i){return[n,!0,0,t,null,i,null,e,null,null]}function yx(n,t){let e=Ke+t;if(e<n.length)return n[e]}function kf(n,t,e,i=!0){let r=t[A];if(_x(r,t,n,e),i){let s=Gu(e,n),a=t[Ee],c=a.parentNode(n[oi]);c!==null&&AC(r,n[Je],a,t,c,s)}let o=t[wr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function bx(n,t){let e=sc(n,t);return e!==void 0&&Ef(e[A],e),e}function sc(n,t){if(n.length<=Ke)return;let e=Ke+t,i=n[e];if(i){let r=i[ii];r!==null&&r!==n&&_f(r,i),t>0&&(n[e-1][Tt]=i[Tt]);let o=wo(n,Ke+t);TC(i[A],i);let s=o[ln];s!==null&&s.detachView(o[A]),i[Ue]=null,i[Tt]=null,i[R]&=-129}return i}function _x(n,t,e,i){let r=Ke+i,o=e.length;i>0&&(e[r-1][Tt]=t),i<o-Ke?(t[Tt]=e[r],Hd(e,Ke+i,t)):(e.push(t),t[Tt]=null),t[Ue]=e;let s=t[ii];s!==null&&e!==s&&iy(s,t);let a=t[ln];a!==null&&a.insertView(n),Ra(t),t[R]|=128}function iy(n,t){let e=n[Li],i=t[Ue];if(Tn(i))n[R]|=2;else{let r=i[Ue][mt];t[mt]!==r&&(n[R]|=2)}e===null?n[Li]=[t]:e.push(t)}var li=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,e=t[A];return Bo(e,t,e.firstChild,[])}constructor(t,e){this._lView=t,this._cdRefInjectingView=e}get context(){return this._lView[qe]}set context(t){this._lView[qe]=t}get destroyed(){return Vi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[Ue];if(At(t)){let e=t[xo],i=e?e.indexOf(this):-1;i>-1&&(sc(t,i),wo(e,i))}this._attachedToViewContainer=!1}Ef(this._lView[A],this._lView)}onDestroy(t){Fa(this._lView,t)}markForCheck(){Nf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[R]&=-129}reattach(){Ra(this._lView),this._lView[R]|=128}detectChanges(){this._lView[R]|=1024,Xv(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new _(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=xr(this._lView),e=this._lView[ii];e!==null&&!t&&_f(e,this._lView),Fv(this._lView[A],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new _(902,!1);this._appRef=t;let e=xr(this._lView),i=this._lView[ii];i!==null&&!e&&iy(i,this._lView),Ra(this._lView)}};var di=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=Ex;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Af(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new li(o)}}return n})();function Ex(){return wc($e(),V())}function wc(n,t){return n.type&4?new di(t,n,Pr(n,t)):null}function Lr(n,t,e,i,r){let o=n.data[t];if(o===null)o=wx(n,t,e,i,r),Zh()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=Gh();o.injectorIndex=s===null?-1:s.injectorIndex}return Ir(o,!0),o}function wx(n,t,e,i,r){let o=cu(),s=lu(),a=s?o:o&&o.parent,c=n.data[t]=Cx(n,a,e,t,i,r);return Dx(n,c,o,s),c}function Dx(n,t,e,i){n.firstChild===null&&(n.firstChild=t),e!==null&&(i?e.child==null&&t.parent!==null&&(e.child=t):e.next===null&&(e.next=t,t.prev=e))}function Cx(n,t,e,i,r,o){let s=t?t.injectorIndex:-1,a=0;return ou()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:pu(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var xx=()=>null,Ix=()=>null;function qu(n,t){return xx(n,t)}function Sx(n,t,e){return Ix(n,t,e)}var ry=class{},ct=class{},dt=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>Mx()}return n})();function Mx(){let n=V(),t=$e(),e=Rt(t.index,n);return(Tn(e)?e:n)[Ee]}var oy=(()=>{class n{static \u0275prov=L({token:n,providedIn:"root",factory:()=>null})}return n})();function sy(n){return n.debugInfo?.className||n.type.name||null}var Ya={},ac=class{injector;parentInjector;constructor(t,e){this.injector=t,this.parentInjector=e}get(t,e,i){let r=this.injector.get(t,Ya,i);return r!==Ya||e===Ya?r:this.parentInjector.get(t,e,i)}};function Rf(n){return cy(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function ay(n,t){if(Array.isArray(n))for(let e=0;e<n.length;e++)t(n[e]);else{let e=n[Symbol.iterator](),i;for(;!(i=e.next()).done;)t(i.value)}}function cy(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function Tx(n,t,e){return n[t]=e}function Rn(n,t,e){if(e===pt)return!1;let i=n[t];return Object.is(i,e)?!1:(n[t]=e,!0)}function ly(n,t,e,i){let r=Rn(n,t,e);return Rn(n,t+1,i)||r}function Nr(n,t,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&PD(r,o);let s=An(n)?Rt(n.index,t):t;Nf(s,5);let a=t[qe],c=Ng(t,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=Ng(t,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Ng(n,t,e,i){let r=F(null);try{return se(ne.OutputStart,t,e),e(i)!==!1}catch(o){return tx(n,o),!1}finally{se(ne.OutputEnd,t,e),F(r)}}function dy(n,t,e,i,r,o,s,a){let c=Io(n),l=!1,d=null;if(!i&&c&&(d=Nx(t,e,o,n.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=kt(n,e),g=i?i(u):u;VD(e,g,o,a),i||(a.__ngNativeEl__=u);let f=r.listen(g,o,a);if(!Ax(o)){let v=i?E=>i(Nt(E[n.index])):n.index;uy(v,t,e,o,a,f,!1)}}return l}function Ax(n){return n.startsWith("animation")||n.startsWith("transition")}function Nx(n,t,e,i){let r=n.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=t[Dr],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function uy(n,t,e,i,r,o,s){let a=t.firstCreatePass?nu(t):null,c=tu(e),l=c.length;c.push(r,o),a&&a.push(i,n,l,(l+1)*(s?-1:1))}function kg(n,t,e,i,r){let o=null,s=null,a=null,c=!1,l=n.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&n.hostDirectiveOutputs?.hasOwnProperty(i)){let d=n.hostDirectiveOutputs[i];for(let u=0;u<d.length;u+=2){let g=d[u];if(g>=s&&g<=a)c=!0,cc(n,t,g,d[u+1],i,r);else if(g>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,cc(n,t,o,i,i,r)),c}function cc(n,t,e,i,r,o){let s=t[e],a=t[A],l=a.data[e].outputs[i],u=s[l].subscribe(o);uy(n.index,a,t,r,o,u,!0)}function Wo(){kx()}function kx(){let n=V(),t=Ae(),e=$e();if(t.firstCreatePass&&Fx(t,e),e.controlDirectiveIndex===-1)return;ui("NgSignalForms");let i=n[e.controlDirectiveIndex];t.data[e.controlDirectiveIndex].controlDef.create(i,new lc(n,t,e))}function qo(){Rx()}function Rx(){let n=V(),t=Ae(),e=To();if(e.controlDirectiveIndex===-1)return;let i=t.data[e.controlDirectiveIndex].controlDef,r=n[e.controlDirectiveIndex];i.update(r,new lc(n,t,e))}var lc=class{lView;tView;tNode;hasPassThrough;constructor(t,e,i){this.lView=t,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return kt(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(t,e){let i=this.tView.data[this.tNode.customControlIndex];kg(this.tNode,this.lView,i,t,Nr(this.tNode,this.lView,e))}listenToCustomControlModel(t){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];kg(this.tNode,this.lView,i,e,Nr(this.tNode,this.lView,t))}listenToDom(t,e){dy(this.tNode,this.tView,this.lView,void 0,this.lView[Ee],t,e,Nr(this.tNode,this.lView,e))}setInputOnDirectives(t,e){let i=this.tNode.inputs?.[t],r=this.tNode.hostDirectiveInputs?.[t];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];qi(a,c,t,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];qi(l,d,c,e),o=!0}return o}setCustomControlModelInput(t){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";nx(this.tNode,this.tView,this.lView,e,i,t)}customControlHasInput(t){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[t]===!0}_buildCustomControlInputCache(t){let e={};for(let i in t.inputs)e[i]=!0;if(t.hostDirectives!==null){let i=[...t.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=Rg(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=Rg(o.directive);s!==null&&i.push(...s)}}}return e}};function Rg(n){return typeof n=="function"&&"\u0275dir"in n?n.\u0275dir.hostDirectives??null:null}function Fx(n,t,e){for(let r=t.directiveStart;r<t.directiveEnd;r++)if(n.data[r].controlDef){t.controlDirectiveIndex=r;break}if(t.controlDirectiveIndex===-1)return;let i=n.data[t.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(t.inputs?.[i.passThroughInput]?.length??0)>1){t.flags|=4096;return}Ox(n,t)}function Ox(n,t){for(let e=t.directiveStart;e<t.directiveEnd;e++){let i=n.data[e];if(!(t.directiveToIndex&&!t.directiveToIndex.has(i.type))){if(Fg(i,"value")){t.flags|=1024,t.customControlIndex=e;return}if(Fg(i,"checked")){t.flags|=2048,t.customControlIndex=e;return}}}if(t.hostDirectiveInputs!==null&&t.hostDirectiveOutputs!==null&&t.directiveToIndex!==null){let e=(i,r)=>{let o=t.hostDirectiveInputs[i],s=t.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let u of t.directiveToIndex.values()){if(!Array.isArray(u))continue;let[g,f,v]=u;if(c>=f&&c<=v)return t.flags|=r,t.customControlIndex=g,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function Fg(n,t){return Px(n,t)&&Lx(n,t+"Change")}function Px(n,t){return t in n.inputs}function Lx(n,t){return t in n.outputs}var Ku=Symbol("BINDING");var Yi=new w("");function dc(n,t,e){let i=e?n.styles:null,r=e?n.classes:null,o=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")o=a;else if(o==1)r=wa(r,a);else if(o==2){let c=a,l=t[++s];i=wa(i,c+": "+l+";")}}e?n.styles=i:n.stylesWithoutHost=i,e?n.classes=r:n.classesWithoutHost=r}function H(n,t=0){let e=V();if(e===null)return T(n,t);let i=$e();return ov(i,e,Ge(n),t)}function fy(n,t,e,i,r){let o=i===null?null:{"":-1},s=r(n,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}Bx(n,t,e,a,o,c,l)}o!==null&&i!==null&&Vx(e,i,o)}function Vx(n,t,e){let i=n.localNames=[];for(let r=0;r<t.length;r+=2){let o=e[t[r+1]];if(o==null)throw new _(-301,!1);i.push(t[r],o)}}function jx(n,t,e){t.componentOffset=e,(n.components??=[]).push(t.index)}function Bx(n,t,e,i,r,o,s){let a=i.length,c=null;for(let g=0;g<a;g++){let f=i[g];c===null&&dn(f)&&(c=f,jx(n,e,g)),Nu(nc(e,t),n,f.type)}Wx(e,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<a;g++){let f=i[g];f.providersResolver&&f.providersResolver(f)}let l=!1,d=!1,u=Bv(n,t,a,null);a>0&&(e.directiveToIndex=new Map);for(let g=0;g<a;g++){let f=i[g];if(e.mergedAttrs=kr(e.mergedAttrs,f.hostAttrs),Ux(n,e,t,u,f),Gx(u,f,r),s!==null&&s.has(f)){let[E,D]=s.get(f);e.directiveToIndex.set(f.type,[u,E+e.directiveStart,D+e.directiveStart])}else(o===null||!o.has(f))&&e.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let v=f.type.prototype;!l&&(v.ngOnChanges||v.ngOnInit||v.ngDoCheck)&&((n.preOrderHooks??=[]).push(e.index),l=!0),!d&&(v.ngOnChanges||v.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}Hx(n,e,o)}function Hx(n,t,e){for(let i=t.directiveStart;i<t.directiveEnd;i++){let r=n.data[i];if(e===null||!e.has(r))Og(0,t,r,i),Og(1,t,r,i),Lg(t,i,!1);else{let o=e.get(r);Pg(0,t,o,i),Pg(1,t,o,i),Lg(t,i,!0)}}}function Og(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;n===0?s=t.inputs??={}:s=t.outputs??={},s[o]??=[],s[o].push(i),my(t,o)}}function Pg(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;n===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),my(t,s)}}function my(n,t){t==="class"?n.flags|=8:t==="style"&&(n.flags|=16)}function Lg(n,t,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=n;if(i===null||!e&&r===null||e&&o===null||gf(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===t){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===t){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function Ux(n,t,e,i,r){n.data[i]=r;let o=r.factory||(r.factory=Yn(r.type,!0)),s=new Gi(o,dn(r),H,null);n.blueprint[i]=s,e[i]=s,$x(n,t,i,Bv(n,e,r.hostVars,pt),r)}function $x(n,t,e,i,r){let o=r.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~t.index;zx(s)!=a&&s.push(a),s.push(e,i,o)}}function zx(n){let t=n.length;for(;t>0;){let e=n[--t];if(typeof e=="number"&&e<0)return e}return 0}function Gx(n,t,e){if(e){if(t.exportAs)for(let i=0;i<t.exportAs.length;i++)e[t.exportAs[i]]=n;dn(t)&&(e[""]=n)}}function Wx(n,t,e){n.flags|=1,n.directiveStart=t,n.directiveEnd=t+e,n.providerIndexes=t}function py(n,t,e,i,r,o,s,a){let c=t[A],l=c.consts,d=un(l,s),u=Lr(c,n,e,i,d);return o&&fy(c,t,u,un(l,a),r),u.mergedAttrs=kr(u.mergedAttrs,u.attrs),u.attrs!==null&&dc(u,u.attrs,!1),u.mergedAttrs!==null&&dc(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function hy(n,t){Zg(n,t),Kd(t)&&n.queries.elementEnd(t)}function qx(n,t,e,i,r,o){let s=t.consts,a=un(s,r),c=Lr(t,n,e,i,a);if(c.mergedAttrs=kr(c.mergedAttrs,c.attrs),o!=null){let l=un(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&dc(c,c.attrs,!1),c.mergedAttrs!==null&&dc(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}var gy=typeof ShadowRoot<"u",Kx=typeof Document<"u";function Qx(n){return Object.keys(n).map(t=>{let[e,i,r]=n[t],o={propName:e,templateName:t,isSignal:(i&_c.SignalBased)!==0};return r&&(o.transform=r),o})}function Zx(n){return Object.keys(n).map(t=>({propName:n[t],templateName:t}))}function Yx(n,t,e){let i=t instanceof We?t:t?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new ac(e,i):e}function Xx(n){let t=n.get(ct,null);if(t===null)throw new _(407,!1);let e=n.get(oy,null),i=n.get(sn,null),r=n.get(Pn,null,{optional:!0});return{rendererFactory:t,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function Jx(n,t){let e=vy(n);return Iv(t,e,e==="svg"?Qd:e==="math"?Lh:null)}function vy(n){return(n.selectors[0][0]||"div").toLowerCase()}var Fr=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=Qx(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=Zx(this.componentDef.outputs),this.cachedOutputs}constructor(t,e){this.componentDef=t,this.ngModule=e,this.componentType=t.type,this.selector=yC(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!e}create(t,e,i,r,o,s){se(ne.DynamicComponentStart);let a=F(null);try{let c=this.componentDef,l=Yx(c,r||this.ngModule,t),d=Xx(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(sy(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{F(a)}}createComponentRef(t,e,i,r,o,s){let a=this.componentDef,c=eI(r,a,s,o),l=t.rendererFactory.createRenderer(null,a),d=r?$C(l,r,a.encapsulation,e):Jx(a,l),u=e.get(Yi,null),g=tI(d,()=>e.get(X,null)??pv());u&&u.addHost(g);let f=s?.some(Vg)||o?.some(D=>typeof D!="function"&&D.bindings.some(Vg)),v=xf(null,c,null,512|jv(a),null,null,t,l,e,null,vv(d,e,!0));u&&gy&&g instanceof ShadowRoot&&Fa(v,()=>{u.removeHost(g)}),v[Te]=d,Va(v);let E=null;try{let D=py(Te,v,2,"#host",()=>c.directiveRegistry,!0,0);Mv(l,d,D),Rr(d,v),Sf(c,v,D),bv(c,D,v),hy(c,D),i!==void 0&&iI(D,this.ngContentSelectors,i),E=Rt(D.index,v),v[qe]=E[qe],Tf(c,v,null)}catch(D){throw E!==null&&Ru(E),Ru(v),D}finally{se(ne.DynamicComponentEnd),ja()}return new uc(this.componentType,v,!!f)}};function eI(n,t,e,i){let r=n?["ng-version","22.0.1"]:bC(t.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Ku].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let u=i[d];if(typeof u!="function")for(let g of u.bindings){a+=g[Ku].requiredVars;let f=d+1;g.create&&(g.targetIdx=f,(o??=[]).push(g)),g.update&&(g.targetIdx=f,(s??=[]).push(g))}}let c=[t];if(i)for(let d of i){let u=typeof d=="function"?d:d.type,g=Ld(u);c.push(g)}return Cf(0,null,nI(o,s),1,a,c,null,null,null,[r],null)}function tI(n,t){let e=n.getRootNode?.();return Kx&&e instanceof Document?e.head:e&&gy&&e instanceof ShadowRoot?e:t().head}function nI(n,t){return!n&&!t?null:e=>{if(e&1&&n)for(let i of n)i.create();if(e&2&&t)for(let i of t)i.update()}}function Vg(n){let t=n[Ku].kind;return t==="input"||t==="twoWay"}var uc=class extends ry{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Na(e[A],Te),this.location=Pr(this._tNode,e),this.instance=Rt(this._tNode.index,e)[qe],this.hostView=this.changeDetectorRef=new li(e,void 0),this.componentType=t}setInput(t,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),e))return;let r=this._rootLView,o=Mf(i,r[A],r,t,e);this.previousInputValues.set(t,e);let s=Rt(i.index,r);Nf(s,1)}get injector(){return new ci(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function iI(n,t,e){let i=n.projection=[];for(let r=0;r<t.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var fi=(()=>{class n{static __NG_ELEMENT_ID__=rI}return n})();function rI(){let n=$e();return yy(n,V())}var Qu=class n extends fi{_lContainer;_hostTNode;_hostLView;constructor(t,e,i){super(),this._lContainer=t,this._hostTNode=e,this._hostLView=i}get element(){return Pr(this._hostTNode,this._hostLView)}get injector(){return new ci(this._hostTNode,this._hostLView)}get parentInjector(){let t=sf(this._hostTNode,this._hostLView);if(Jg(t)){let e=ec(t,this._hostLView),i=Ja(t),r=e[A].data[i+8];return new ci(r,e)}else return new ci(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let e=jg(this._lContainer);return e!==null&&e[t]||null}get length(){return this._lContainer.length-Ke}createEmbeddedView(t,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=qu(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,oc(this._hostTNode,s)),a}createComponent(t,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new Fr(ei(t)),u=i||this.parentInjector;if(!o&&d.ngModule==null){let I=this.parentInjector.get(We,null);I&&(o=I)}let g=ei(d.componentType??{}),f=qu(this._lContainer,g?.id??null),v=f?.firstChild??null,E=d.create(u,r,v,o,s,a);return this.insertImpl(E.hostView,c,oc(this._hostTNode,f)),E}insert(t,e){return this.insertImpl(t,e,!0)}insertImpl(t,e,i){let r=t._lView;if(jh(r)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=r[Ue],l=new n(c,c[Je],c[Ue]);l.detach(l.indexOf(t))}}let o=this._adjustIndex(e),s=this._lContainer;return kf(s,r,o,i),t.attachToViewContainerRef(),Hd(wu(s),o,t),t}move(t,e){return this.insert(t,e)}indexOf(t){let e=jg(this._lContainer);return e!==null?e.indexOf(t):-1}remove(t){let e=this._adjustIndex(t,-1),i=sc(this._lContainer,e);i&&(wo(wu(this._lContainer),e),Ef(i[A],i))}detach(t){let e=this._adjustIndex(t,-1),i=sc(this._lContainer,e);return i&&wo(wu(this._lContainer),e)!=null?new li(i):null}_adjustIndex(t,e=0){return t??this.length+e}};function jg(n){return n[xo]}function wu(n){return n[xo]||(n[xo]=[])}function yy(n,t){let e,i=t[n.index];return At(i)?e=i:(e=ny(i,t,null,n),t[n.index]=e,If(t,e)),sI(e,t,n,i),new Qu(e,n,t)}function oI(n,t){let e=n[Ee],i=e.createComment(""),r=kt(t,n),o=e.parentNode(r);return ic(e,o,i,e.nextSibling(r),!1),i}var sI=lI,aI=()=>!1;function cI(n,t,e){return aI(n,t,e)}function lI(n,t,e,i){if(n[oi])return;let r;e.type&8?r=Nt(i):r=oI(t,e),n[oi]=r}var Zu=class n{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Yu=class n{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let e=t.queries;if(e!==null){let i=t.contentQueries!==null?t.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let e=0;e<this.queries.length;e++)Of(t,e).matches!==null&&this.queries[e].setDirty()}},fc=class{flags;read;predicate;constructor(t,e,i=null){this.flags=e,this.read=i,typeof t=="string"?this.predicate=pI(t):this.predicate=t}},Xu=class n{queries;constructor(t=[]){this.queries=t}elementStart(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(t,e)}elementEnd(t){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(t)}embeddedTView(t){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(t,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new n(e):null}template(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(t,e)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},Ju=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,e=-1){this.metadata=t,this._declarationNodeIndex=e}elementStart(t,e){this.isApplyingToNode(e)&&this.matchTNode(t,e)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,e){this.elementStart(t,e)}embeddedTView(t,e){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,e),new n(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=t.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(t,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(t,e,dI(e,o)),this.matchTNodeWithReadOption(t,e,Za(e,t,o,!1,!1))}else i===di?e.type&4&&this.matchTNodeWithReadOption(t,e,-1):this.matchTNodeWithReadOption(t,e,Za(e,t,i,!1,!1))}matchTNodeWithReadOption(t,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===re||r===fi||r===di&&e.type&4)this.addMatch(e.index,-2);else{let o=Za(e,t,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(t,e){this.matches===null?this.matches=[t,e]:this.matches.push(t,e)}};function dI(n,t){let e=n.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===t)return e[i+1]}return null}function uI(n,t){return n.type&11?Pr(n,t):n.type&4?wc(n,t):null}function fI(n,t,e,i){return e===-1?uI(t,n):e===-2?mI(n,t,i):Vo(n,n[A],e,t)}function mI(n,t,e){if(e===re)return Pr(t,n);if(e===di)return wc(t,n);if(e===fi)return yy(t,n)}function by(n,t,e,i){let r=t[ln].queries[i];if(r.matches===null){let o=n.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(fI(t,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function ef(n,t,e,i){let r=n.queries.getByIndex(e),o=r.matches;if(o!==null){let s=by(n,t,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=t[-c];for(let u=Ke;u<d.length;u++){let g=d[u];g[ii]===g[Ue]&&ef(g[A],g,l,i)}if(d[Li]!==null){let u=d[Li];for(let g=0;g<u.length;g++){let f=u[g];ef(f[A],f,l,i)}}}}}return i}function Ff(n,t){return n[ln].queries[t].queryList}function _y(n,t,e){let i=new jo((e&4)===4);return Uh(n,t,i,i.destroy),(t[ln]??=new Yu).queries.push(new Zu(i))-1}function Ey(n,t,e){let i=Ae();return i.firstCreatePass&&(Dy(i,new fc(n,t,e),-1),(t&2)===2&&(i.staticViewQueries=!0)),_y(i,V(),t)}function wy(n,t,e,i){let r=Ae();if(r.firstCreatePass){let o=$e();Dy(r,new fc(t,e,i),o.index),hI(r,n),(e&2)===2&&(r.staticContentQueries=!0)}return _y(r,V(),e)}function pI(n){return n.split(",").map(t=>t.trim())}function Dy(n,t,e){n.queries===null&&(n.queries=new Xu),n.queries.track(new Ju(t,e))}function hI(n,t){let e=n.contentQueries||(n.contentQueries=[]),i=e.length?e[e.length-1]:-1;t!==i&&e.push(n.queries.length-1,t)}function Of(n,t){return n.queries.getByIndex(t)}function Cy(n,t){let e=n[A],i=Of(e,t);return i.crossesNgTemplate?ef(e,n,t,[]):by(e,n,i,t)}function xy(n,t,e){let i,r=ao(()=>{i._dirtyCounter();let o=gI(i,n);if(t&&o===void 0)throw new _(-951,!1);return o});return i=r[Le],i._dirtyCounter=ge(0),i._flatValue=void 0,r}function Pf(n){return xy(!0,!1,n)}function Lf(n){return xy(!0,!0,n)}function Iy(n,t){let e=n[Le];e._lView=V(),e._queryIndex=t,e._queryList=Ff(e._lView,t),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function gI(n,t){let e=n._lView,i=n._queryIndex;if(e===void 0||i===void 0||e[R]&4)return t?void 0:Xe;let r=Ff(e,i),o=Cy(e,i);return r.reset(o,cv),t?r.first:r._changesDetected||n._flatValue===void 0?n._flatValue=r.toArray():n._flatValue}function Vr(n){return!!n&&typeof n.then=="function"}function Vf(n){return!!n&&typeof n.subscribe=="function"}var Ho=class{};var Uo=class extends Ho{injector;instance=null;constructor(t){super();let e=new ki([...t.providers,{provide:Ho,useValue:this}],t.parent||_r(),t.debugName,new Set(["environment"]));this.injector=e,t.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function Sy(n,t,e=null){return new Uo({providers:n,parent:t,debugName:e,runEnvironmentInitializers:!0}).injector}var vI=(()=>{class n{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=$d(!1,e.type),r=i.length>0?Sy([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=L({token:n,providedIn:"environment",factory:()=>new n(T(We))})}return n})();function de(n){return zo(()=>{let t=My(n),e=G(C({},t),{type:n.type,decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection!==cf.Eager,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&n.dependencies||null,getStandaloneInjector:t.standalone?r=>r.get(vI).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||Kt.Emulated,styles:n.styles||Xe,_:null,schemas:n.schemas||null,tView:null,id:""});t.standalone&&ui("NgStandalone"),Ty(e);let i=n.dependencies;return e.directiveDefs=Bg(i,yI),e.pipeDefs=Bg(i,Ch),e.id=EI(e),e})}function yI(n){return ei(n)||Ld(n)}function Z(n){return zo(()=>({type:n.type,bootstrap:n.bootstrap||Xe,declarations:n.declarations||Xe,imports:n.imports||Xe,exports:n.exports||Xe,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function bI(n,t){if(n==null)return ti;let e={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=_c.None,c=null),e[o]=[i,a,c],t[o]=s}return e}function _I(n){if(n==null)return ti;let t={};for(let e in n)n.hasOwnProperty(e)&&(t[n[e]]=e);return t}function z(n){return zo(()=>{let t=My(n);return Ty(t),t})}function jf(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function My(n){let t={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:t,inputConfig:n.inputs||ti,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Xe,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:bI(n.inputs,t),outputs:_I(n.outputs),debugInfo:null}}function Ty(n){n.features?.forEach(t=>t(n))}function Bg(n,t){return n?()=>{let e=typeof n=="function"?n():n,i=[];for(let r of e){let o=t(r);o!==null&&i.push(o)}return i}:null}function EI(n){let t=0,e=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,e,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of i.join("|"))t=Math.imul(31,t)+o.charCodeAt(0)<<0;return t+=2147483648,"c"+t}var Bf=new w("");function Hf(n){return ni([{provide:Bf,multi:!0,useValue:n}])}var Uf=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=p(Bf,{optional:!0})??[];injector=p(Se);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Er(this.injector,r);if(Vr(o))e.push(o);else if(Vf(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();function $f(n){return t=>{t.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:n}}}function wI(n){return Object.getPrototypeOf(n.prototype).constructor}function et(n){let t=wI(n.type),e=!0,i=[n];for(;t;){let r;if(dn(n))r=t.\u0275cmp||t.\u0275dir;else{if(t.\u0275cmp)throw new _(903,!1);r=t.\u0275dir}if(r){if(e){i.push(r);let s=n;s.inputs=Du(n.inputs),s.declaredInputs=Du(n.declaredInputs),s.outputs=Du(n.outputs);let a=r.hostBindings;a&&SI(n,a);let c=r.viewQuery,l=r.contentQueries;if(c&&xI(n,c),l&&II(n,l),DI(n,r),Dh(n.outputs,r.outputs),dn(r)&&r.data.animation){let d=n.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(n),a===et&&(e=!1)}}t=Object.getPrototypeOf(t)}CI(i)}function DI(n,t){for(let e in t.inputs){if(!t.inputs.hasOwnProperty(e)||n.inputs.hasOwnProperty(e))continue;let i=t.inputs[e];i!==void 0&&(n.inputs[e]=i,n.declaredInputs[e]=t.declaredInputs[e])}}function CI(n){let t=0,e=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=t+=r.hostVars,r.hostAttrs=kr(r.hostAttrs,e=kr(e,r.hostAttrs))}}function Du(n){return n===ti?{}:n===Xe?[]:n}function xI(n,t){let e=n.viewQuery;e?n.viewQuery=(i,r)=>{t(i,r),e(i,r)}:n.viewQuery=t}function II(n,t){let e=n.contentQueries;e?n.contentQueries=(i,r,o)=>{t(i,r,o),e(i,r,o)}:n.contentQueries=t}function SI(n,t){let e=n.hostBindings;e?n.hostBindings=(i,r)=>{t(i,r),e(i,r)}:n.hostBindings=t}function Ay(n,t,e,i,r,o,s,a){if(e.firstCreatePass){n.mergedAttrs=kr(n.mergedAttrs,n.attrs);let d=n.tView=Cf(2,n,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,n),d.queries=e.queries.embeddedTView(n))}a&&(n.flags|=a),Ir(n,!1);let c=TI(e,t,n,i);Ba()&&wf(e,t,c,n),Rr(c,t);let l=ny(c,t,c,n);t[i+Te]=l,If(t,l),cI(l,n,t)}function MI(n,t,e,i,r,o,s,a,c,l,d){let u=e+Te,g;return t.firstCreatePass?(g=Lr(t,u,4,s||null,a||null),ru()&&fy(t,n,g,un(t.consts,l),zv),Zg(t,g)):g=t.data[u],Ay(g,n,t,e,i,r,o,c),Io(g)&&Sf(t,n,g),l!=null&&Ec(n,g,d),g}function zf(n,t,e,i,r,o,s,a,c,l,d){let u=e+Te,g;if(t.firstCreatePass){if(g=Lr(t,u,4,s||null,a||null),l!=null){let f=un(t.consts,l);g.localNames=[];for(let v=0;v<f.length;v+=2)g.localNames.push(f[v],-1)}}else g=t.data[u];return Ay(g,n,t,e,i,r,o,c),l!=null&&Ec(n,g,d),g}function ae(n,t,e,i,r,o,s,a){let c=V(),l=Ae(),d=un(l.consts,o);return MI(c,l,n,t,e,i,r,d,void 0,s,a),ae}var TI=AI;function AI(n,t,e,i){return Ha(!0),t[Ee].createComment("")}var Gf=new w("");var Wf=new w("");function Ny(){Yl(()=>{let n="";throw new _(600,n)})}var NI=10;var Qt=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=p(ai);afterRenderManager=p(yf);zonelessEnabled=p(ko);rootEffectScheduler=p($a);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Y;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=p(Bi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ie(e=>!e))}constructor(){p(Pn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=p(We);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=Se.NULL){return this._injector.get(P).run(()=>{if(se(ne.BootstrapComponentStart),!this._injector.get(Uf).done){let I="";throw new _(405,I)}let a=ei(e),c=this._injector.get(Ho),l=new Fr(a,c);this.componentTypes.push(e);let{hostElement:d,directives:u,bindings:g}=kI(i),f=d||l.selector,v=l.create(r,[],f,c.injector,u,g),E=v.location.nativeElement,D=v.injector.get(Gf,null);return D?.registerApplication(E),v.onDestroy(()=>{this.detachView(v.hostView),Lo(this.components,v),D?.unregisterApplication(E)}),this._loadComponent(v),se(ne.BootstrapComponentEnd,v),v})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){se(ne.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(vc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw se(ne.ChangeDetectionEnd),new _(101,!1);let e=F(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,F(e),this.afterTick.next(),se(ne.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ct,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<NI;){se(ne.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{se(ne.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!So(r))continue;let o=i&&!this.zonelessEnabled?0:1;Xv(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>So(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Lo(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Wf,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Lo(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new _(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();function kI(n){return n===void 0||typeof n=="string"||n instanceof Element?{hostElement:n}:n}function Lo(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function Me(n,t,e,i){let r=V(),o=Sr();if(Rn(r,o,t)){let s=Ae(),a=To();XC(a,r,n,t,e,i)}return Me}function we(n,t,e,i,r,o,s,a){ui("NgControlFlow");let c=V(),l=Ae(),d=un(l.consts,o);return zf(c,l,n,t,e,i,r,d,256,s,a),qf}function qf(n,t,e,i,r,o,s,a){ui("NgControlFlow");let c=V(),l=Ae(),d=un(l.consts,o);return zf(c,l,n,t,e,i,r,d,512,s,a),qf}function De(n,t){ui("NgControlFlow");let e=V(),i=Sr(),r=e[i]!==pt?e[i]:-1,o=r!==-1?Hg(e,Te+r):void 0,s=0;if(Rn(e,i,n)){let a=F(null);try{if(o!==void 0&&bx(o,s),n!==-1){let c=Te+n,l=Hg(e,c),d=RI(e[A],c),u=Sx(l,d,e),g=Af(e,d,t,{dehydratedView:u});kf(l,g,s,oc(d,u))}}finally{F(a)}}else if(o!==void 0){let a=yx(o,s);a!==void 0&&(a[qe]=t)}}function Hg(n,t){return n[t]}function RI(n,t){return Na(n,t)}function N(n,t,e){let i=V(),r=Sr();if(Rn(i,r,t)){let o=Ae(),s=To();qC(s,i,n,t,i[Ee],e)}return N}function tf(n,t,e,i,r){Mf(t,n,e,r?"class":"style",i)}function h(n,t,e,i){let r=V(),o=r[A],s=n+Te,a=o.firstCreatePass?py(s,r,2,t,zv,ru(),e,i):o.data[s];if(An(a)){let c=r[cn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(sy(l),()=>(Ug(n,t,r,a,i),h))}}return Ug(n,t,r,a,i),h}function Ug(n,t,e,i,r){if(Gv(i,e,n,t,ky),Io(i)){let o=e[A];Sf(o,e,i),bv(o,i,e)}r!=null&&Ec(e,i)}function m(){let n=Ae(),t=$e(),e=Wv(t);return n.firstCreatePass&&hy(n,e),su(e)&&au(),iu(),e.classesWithoutHost!=null&&yD(e)&&tf(n,e,V(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&bD(e)&&tf(n,e,V(),e.stylesWithoutHost,!1),m}function tt(n,t,e,i){return h(n,t,e,i),m(),tt}function Ot(n,t,e,i){let r=V(),o=r[A],s=n+Te,a=o.firstCreatePass?qx(s,o,2,t,e,i):o.data[s];return Gv(a,r,n,t,ky),i!=null&&Ec(r,a),Ot}function Pt(){let n=$e(),t=Wv(n);return su(t)&&au(),iu(),Pt}function hn(n,t,e,i){return Ot(n,t,e,i),Pt(),hn}var ky=(n,t,e,i,r)=>(Ha(!0),Iv(t[Ee],i,pu()));function ut(){return V()}function gn(n,t,e){let i=V(),r=Sr();if(Rn(i,r,t)){let o=Ae(),s=To();$v(s,i,n,t,i[Ee],e)}return gn}var Fo=void 0;function FI(n){let t=Math.floor(Math.abs(n)),e=n.toString().replace(/^[^.]*\.?/,"").length;return t===1&&e===0?1:5}var OI=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Fo,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Fo,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Fo,Fo,Fo],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",FI],Cu=Object.create(null);function wt(n){let t=PI(n),e=$g(t);if(e)return e;let i=t.split("-")[0];if(e=$g(i),e)return e;if(i==="en")return OI;throw new _(701,!1)}function $g(n){return n in Cu||(Cu[n]=an.ng&&an.ng.common&&an.ng.common.locales&&an.ng.common.locales[n]),Cu[n]}var Oe={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function PI(n){return n.toLowerCase().replace(/_/g,"-")}var Ko="en-US";var LI=Ko;function Ry(n){typeof n=="string"&&(LI=n.toLowerCase().replace(/_/g,"-"))}function q(n,t,e){let i=V(),r=Ae(),o=$e();return VI(r,i,i[Ee],o,n,t,e),q}function VI(n,t,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Nr(i,t,o),dy(i,n,t,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let g=d[u],f=d[u+1];c??=Nr(i,t,o),cc(i,t,g,f,r,c)}if(l&&l.length)for(let u of l)c??=Nr(i,t,o),cc(i,t,u,r,r,c)}}function O(n=1){return ig(n)}function jI(n,t){let e=null,i=mC(n);for(let r=0;r<t.length;r++){let o=t[r];if(o==="*"){e=r;continue}if(i===null?Av(n,o,!0):gC(i,o))return r}return e}function Qe(n){let t=V()[mt][Je];if(!t.projection){let e=n?n.length:1,i=t.projection=Ah(e,null),r=i.slice(),o=t.child;for(;o!==null;){if(o.type!==128){let s=n?jI(o,n):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ce(n,t=0,e,i,r,o){let s=V(),a=Ae(),c=i?n+1:null;c!==null&&zf(s,a,c,i,r,o,null,e);let l=Lr(a,Te+n,16,null,e||null);l.projection===null&&(l.projection=t),du();let u=!s[wr]||ou();s[mt][Je].projection[l.projection]===null&&c!==null?BI(s,a,c):u&&!pc(l)&&LC(a,s,l)}function BI(n,t,e){let i=Te+e,r=t.data[i],o=n[i],s=qu(o,r.tView.ssrId),a=Af(n,r,void 0,{dehydratedView:s});kf(o,a,0,oc(r,s))}function jr(n,t,e,i){return wy(n,t,e,i),jr}function Ln(n,t,e){return Ey(n,t,e),Ln}function ue(n){let t=V(),e=Ae(),i=La();Mo(i+1);let r=Of(e,i);if(n.dirty&&Vh(t)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let o=Cy(t,i);n.reset(o,cv),n.notifyOnChanges()}return!0}return!1}function fe(){return Ff(V(),La())}function Dc(n,t,e,i,r){return Iy(t,wy(n,e,i,r)),Dc}function Cc(n,t,e,i){return Iy(n,Ey(t,e,i)),Cc}function xc(n=1){Mo(La()+n)}function mi(n){let t=Wh();return Yd(t,Te+n)}function qa(n,t){return n<<17|t<<2}function Ki(n){return n>>17&32767}function HI(n){return(n&2)==2}function UI(n,t){return n&131071|t<<17}function nf(n){return n|2}function Or(n){return(n&131068)>>2}function xu(n,t){return n&-131069|t<<2}function $I(n){return(n&1)===1}function rf(n){return n|1}function zI(n,t,e,i,r,o){let s=o?t.classBindings:t.styleBindings,a=Ki(s),c=Or(s);n[i]=e;let l=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||yr(u,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let g=Ki(n[a+1]);n[i+1]=qa(g,a),g!==0&&(n[g+1]=xu(n[g+1],i)),n[a+1]=UI(n[a+1],i)}else n[i+1]=qa(a,0),a!==0&&(n[a+1]=xu(n[a+1],i)),a=i;else n[i+1]=qa(c,0),a===0?a=i:n[c+1]=xu(n[c+1],i),c=i;l&&(n[i+1]=nf(n[i+1])),zg(n,d,i,!0),zg(n,d,i,!1),GI(t,d,n,i,o),s=qa(a,c),o?t.classBindings=s:t.styleBindings=s}function GI(n,t,e,i,r){let o=r?n.residualClasses:n.residualStyles;o!=null&&typeof t=="string"&&yr(o,t)>=0&&(e[i+1]=rf(e[i+1]))}function zg(n,t,e,i){let r=n[e+1],o=t===null,s=i?Ki(r):Or(r),a=!1;for(;s!==0&&(a===!1||o);){let c=n[s],l=n[s+1];WI(c,t)&&(a=!0,n[s+1]=i?rf(l):nf(l)),s=i?Ki(l):Or(l)}a&&(n[e+1]=i?nf(r):rf(r))}function WI(n,t){return n===null||t==null||(Array.isArray(n)?n[1]:n)===t?!0:Array.isArray(n)&&typeof t=="string"?yr(n,t)>=0:!1}var qt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function qI(n){return n.substring(qt.key,qt.keyEnd)}function KI(n){return QI(n),Fy(n,Oy(n,0,qt.textEnd))}function Fy(n,t){let e=qt.textEnd;return e===t?-1:(t=qt.keyEnd=ZI(n,qt.key=t,e),Oy(n,t,e))}function QI(n){qt.key=0,qt.keyEnd=0,qt.value=0,qt.valueEnd=0,qt.textEnd=n.length}function Oy(n,t,e){for(;t<e&&n.charCodeAt(t)<=32;)t++;return t}function ZI(n,t,e){for(;t<e&&n.charCodeAt(t)>32;)t++;return t}function Vn(n,t,e){return Py(n,t,e,!1),Vn}function K(n,t){return Py(n,t,null,!0),K}function Dt(n){XI(rS,YI,n,!0)}function YI(n,t){for(let e=KI(t);e>=0;e=Fy(t,e))Sa(n,qI(t),!0)}function Py(n,t,e,i){let r=V(),o=Ae(),s=Oa(2);if(o.firstUpdatePass&&Vy(o,n,s,i),t!==pt&&Rn(r,s,t)){let a=o.data[Nn()];jy(o,a,r,r[Ee],n,r[s+1]=sS(t,e),i,s)}}function XI(n,t,e,i){let r=Ae(),o=Oa(2);r.firstUpdatePass&&Vy(r,null,o,i);let s=V();if(e!==pt&&Rn(s,o,e)){let a=r.data[Nn()];if(By(a,i)&&!Ly(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=wa(c,e||"")),tf(r,a,s,e,i)}else oS(r,a,s,s[Ee],s[o+1],s[o+1]=iS(n,t,e),i,o)}}function Ly(n,t){return t>=n.expandoStartIndex}function Vy(n,t,e,i){let r=n.data;if(r[e+1]===null){let o=r[Nn()],s=Ly(n,e);By(o,i)&&t===null&&!s&&(t=!1),t=JI(r,o,t,i),zI(r,o,t,e,s,i)}}function JI(n,t,e,i){let r=Jh(n),o=i?t.residualClasses:t.residualStyles;if(r===null)(i?t.classBindings:t.styleBindings)===0&&(e=Iu(null,n,t,e,i),e=$o(e,t.attrs,i),o=null);else{let s=t.directiveStylingLast;if(s===-1||n[s]!==r)if(e=Iu(r,n,t,e,i),o===null){let c=eS(n,t,i);c!==void 0&&Array.isArray(c)&&(c=Iu(null,n,t,c[1],i),c=$o(c,t.attrs,i),tS(n,t,i,c))}else o=nS(n,t,i)}return o!==void 0&&(i?t.residualClasses=o:t.residualStyles=o),e}function eS(n,t,e){let i=e?t.classBindings:t.styleBindings;if(Or(i)!==0)return n[Ki(i)]}function tS(n,t,e,i){let r=e?t.classBindings:t.styleBindings;n[Ki(r)]=i}function nS(n,t,e){let i,r=t.directiveEnd;for(let o=1+t.directiveStylingLast;o<r;o++){let s=n[o].hostAttrs;i=$o(i,s,e)}return $o(i,t.attrs,e)}function Iu(n,t,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=t[a],i=$o(i,o.hostAttrs,r),o!==n);)a++;return n!==null&&(e.directiveStylingLast=a),i}function $o(n,t,e){let i=e?1:2,r=-1;if(t!==null)for(let o=0;o<t.length;o++){let s=t[o];typeof s=="number"?r=s:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Sa(n,s,e?!0:t[++o]))}return n===void 0?null:n}function iS(n,t,e){if(e==null||e==="")return Xe;let i=[],r=Fn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)n(i,r[o],!0);else if(r instanceof Set)for(let o of r)n(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&n(i,o,r[o]);else typeof r=="string"&&t(i,r);return i}function rS(n,t,e){let i=String(t);i!==""&&!i.includes(" ")&&Sa(n,i,e)}function oS(n,t,e,i,r,o,s,a){r===pt&&(r=Xe);let c=0,l=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let g=c<r.length?r[c+1]:void 0,f=l<o.length?o[l+1]:void 0,v=null,E;d===u?(c+=2,l+=2,g!==f&&(v=u,E=f)):u===null||d!==null&&d<u?(c+=2,v=d):(l+=2,v=u,E=f),v!==null&&jy(n,t,e,i,v,E,s,a),d=c<r.length?r[c]:null,u=l<o.length?o[l]:null}}function jy(n,t,e,i,r,o,s,a){if(!(t.type&3))return;let c=n.data,l=c[a+1],d=$I(l)?Gg(c,t,e,r,Or(l),s):void 0;if(!mc(d)){mc(o)||HI(l)&&(o=Gg(c,null,e,r,a,s));let u=Zd(Nn(),e);jC(i,s,u,r,o)}}function Gg(n,t,e,i,r,o){let s=t===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),d=l?c[1]:c,u=d===null,g=e[r+1];g===pt&&(g=u?Xe:void 0);let f=u?Ma(g,i):d===i?g:void 0;if(l&&!mc(f)&&(f=Ma(c,i)),mc(f)&&(a=f,s))return a;let v=n[r+1];r=s?Ki(v):Or(v)}if(t!==null){let c=o?t.residualClasses:t.residualStyles;c!=null&&(a=Ma(c,i))}return a}function mc(n){return n!==void 0}function sS(n,t){return n==null||n===""||(typeof t=="string"?n=n+t:typeof n=="object"&&(n=Ea(Fn(n)))),n}function By(n,t){return(n.flags&(t?8:16))!==0}function y(n,t=""){let e=V(),i=Ae(),r=n+Te,o=i.firstCreatePass?Lr(i,r,1,t,null):i.data[r],s=aS(i,e,o,t);e[r]=s,Ba()&&wf(i,e,s,o),Ir(o,!1)}var aS=(n,t,e,i)=>(Ha(!0),iC(t[Ee],i));function cS(n,t,e,i=""){return Rn(n,Sr(),e)?t+Eo(e)+i:pt}function lS(n,t,e,i,r,o=""){let s=Kh(),a=ly(n,s,e,r);return Oa(2),a?t+Eo(e)+i+Eo(r)+o:pt}function S(n){return Ce("",n),S}function Ce(n,t,e){let i=V(),r=cS(i,n,t,e);return r!==pt&&Hy(i,Nn(),r),Ce}function Qo(n,t,e,i,r){let o=V(),s=lS(o,n,t,e,i,r);return s!==pt&&Hy(o,Nn(),s),Qo}function Hy(n,t,e){let i=Zd(t,n);rC(n[Ee],i,e)}function Wg(n,t,e){let i=Ae();i.firstCreatePass&&Uy(t,i.data,i.blueprint,dn(n),e)}function Uy(n,t,e,i,r){if(n=Ge(n),Array.isArray(n))for(let o=0;o<n.length;o++)Uy(n[o],t,e,i,r);else{let o=Ae(),s=V(),a=$e(),c=Ni(n)?n:Ge(n.provide),l=Gd(n),d=a.providerIndexes&1048575,u=a.directiveStart,g=a.providerIndexes>>20;if(Ni(n)||!n.multi){let f=new Gi(l,r,H,null),v=Mu(c,t,r?d:d+g,u);v===-1?(Nu(nc(a,s),o,c),Su(o,n,t.length),t.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(f),s.push(f)):(e[v]=f,s[v]=f)}else{let f=Mu(c,t,d+g,u),v=Mu(c,t,d,d+g),E=f>=0&&e[f],D=v>=0&&e[v];if(r&&!D||!r&&!E){Nu(nc(a,s),o,c);let I=fS(r?uS:dS,e.length,r,i,l,n);!r&&D&&(e[v].providerFactory=I),Su(o,n,t.length,0),t.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(I),s.push(I)}else{let I=$y(e[r?v:f],l,!r&&i);Su(o,n,f>-1?f:v,I)}!r&&i&&D&&e[v].componentProviders++}}}function Su(n,t,e,i){let r=Ni(t),o=Fh(t);if(r||o){let c=(o?Ge(t.useClass):t).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&t.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function $y(n,t,e){return e&&n.componentProviders++,n.multi.push(t)-1}function Mu(n,t,e,i){for(let r=e;r<i;r++)if(t[r]===n)return r;return-1}function dS(n,t,e,i,r){return of(this.multi,[])}function uS(n,t,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Vo(i,i[A],this.providerFactory.index,r);s=c.slice(0,a),of(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],of(o,s);return s}function of(n,t){for(let e=0;e<n.length;e++){let i=n[e];t.push(i())}return t}function fS(n,t,e,i,r,o){let s=new Gi(n,e,H,null);return s.multi=[],s.index=t,s.componentProviders=0,$y(s,r,i&&!e),s}function nt(n,t){return e=>{e.providersResolver=(i,r)=>Wg(i,r?r(n):n,!1),t&&(e.viewProvidersResolver=(i,r)=>Wg(i,r?r(t):t,!0))}}function mS(n,t){let e=n[t];return e===pt?void 0:e}function pS(n,t,e,i,r,o,s){let a=t+e;return ly(n,a,r,o)?Tx(n,a+2,s?i.call(s,r,o):i(r,o)):mS(n,a+2)}function Ic(n,t){let e=Ae(),i,r=n+Te;e.firstCreatePass?(i=hS(t,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Yn(i.type,!0)),s,a=at(H);try{let c=tc(!1),l=o();return tc(c),Xd(e,V(),r,l),l}finally{at(a)}}function hS(n,t){if(t)for(let e=t.length-1;e>=0;e--){let i=t[e];if(n===i.name)return i}}function Sc(n,t,e,i){let r=n+Te,o=V(),s=Yd(o,r);return gS(o,r)?pS(o,qh(),t,s.transform,e,i,s):s.transform(e,i)}function gS(n,t){return n[A].data[t].pure}function Zo(n,t){return wc(n,t)}var zy=(()=>{class n{applicationErrorHandler=p(ai);appRef=p(Qt);taskService=p(Bi);ngZone=p(P);zonelessEnabled=p(ko);tracing=p(Pn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ke;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(bo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(p(yu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?cg:hu;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(bo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();function Gy(){return[{provide:sn,useExisting:zy},{provide:P,useClass:_o},{provide:ko,useValue:!0}]}function vS(){return typeof $localize<"u"&&$localize.locale||Ko}var Yo=new w("",{factory:()=>p(Yo,{optional:!0,skipSelf:!0})||vS()});function Re(n,t){return ao(n,t?.equal)}function ht(n){return Gp(n)}var Jy=Symbol("InputSignalNode#UNSET"),AS=G(C({},co),{transformFn:void 0,applyValueToInputSignal(n,t){dr(n,t)}});function eb(n,t){let e=Object.create(AS);e.value=n,e.transformFn=t?.transform;function i(){if(bi(e),e.value===Jy){let r=null;throw new _(-950,r)}return e.value}return i[Le]=e,i}var Br=class{attributeName;constructor(t){this.attributeName=t}__NG_ELEMENT_ID__=()=>af(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},tb=(()=>{let n=new w("");return n.__NG_ELEMENT_ID__=t=>{let e=$e();if(e===null)throw new _(-204,!1);if(e.type&2)return e.value;if(t&8)return null;throw new _(-204,!1)},n})();function Wy(n,t){return eb(n,t)}function NS(n){return eb(Jy,n)}var nb=(Wy.required=NS,Wy);function qy(n,t){return Pf(t)}function kS(n,t){return Lf(t)}var Jo=(qy.required=kS,qy);function Ky(n,t){return Pf(t)}function RS(n,t){return Lf(t)}var ib=(Ky.required=RS,Ky);var FS=1e4;var e3=FS-1e3;var Qf=class{supports(t){return Rf(t)}create(t){return new Zf(t)}},OS=(n,t)=>t,Zf=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(t){this._trackByFn=t||OS}forEachItem(t){let e;for(e=this._itHead;e!==null;e=e._next)t(e)}forEachOperation(t){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<Qy(i,r,o)?e:i,a=Qy(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let g=0;g<l;g++){let f=g<o.length?o[g]:o[g]=0,v=f+g;d<=v&&v<l&&(o[g]=f+1)}let u=s.previousIndex;o[u]=d-l}}a!==c&&t(s,a,c)}}forEachPreviousItem(t){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)t(e)}forEachAddedItem(t){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)t(e)}forEachMovedItem(t){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)t(e)}forEachRemovedItem(t){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)t(e)}forEachIdentityChange(t){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)t(e)}diff(t){if(t==null&&(t=[]),!Rf(t))throw new _(900,!1);return this.check(t)?this:null}onDestroy(){}check(t){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(t)){this.length=t.length;for(let a=0;a<this.length;a++)o=t[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,ay(t,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=t,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let t;for(t=this._previousItHead=this._itHead;t!==null;t=t._next)t._nextPrevious=t._next;for(t=this._additionsHead;t!==null;t=t._nextAdded)t.previousIndex=t.currentIndex;for(this._additionsHead=this._additionsTail=null,t=this._movesHead;t!==null;t=t._nextMoved)t.previousIndex=t.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(t,e,i,r){let o;return t===null?o=this._itTail:(o=t._prev,this._remove(t)),t=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._reinsertAfter(t,o,r)):(t=this._linkedRecords===null?null:this._linkedRecords.get(i,r),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._moveAfter(t,o,r)):t=this._addAfter(new Yf(e,i),o,r)),t}_verifyReinsertion(t,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?t=this._reinsertAfter(o,t._prev,r):t.currentIndex!=r&&(t.currentIndex=r,this._addToMoves(t,r)),t}_truncate(t){for(;t!==null;){let e=t._next;this._addToRemovals(this._unlink(t)),t=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(t,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(t);let r=t._prevRemoved,o=t._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(t,e,i),this._addToMoves(t,i),t}_moveAfter(t,e,i){return this._unlink(t),this._insertAfter(t,e,i),this._addToMoves(t,i),t}_addAfter(t,e,i){return this._insertAfter(t,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=t:this._additionsTail=this._additionsTail._nextAdded=t,t}_insertAfter(t,e,i){let r=e===null?this._itHead:e._next;return t._next=r,t._prev=e,r===null?this._itTail=t:r._prev=t,e===null?this._itHead=t:e._next=t,this._linkedRecords===null&&(this._linkedRecords=new Tc),this._linkedRecords.put(t),t.currentIndex=i,t}_remove(t){return this._addToRemovals(this._unlink(t))}_unlink(t){this._linkedRecords!==null&&this._linkedRecords.remove(t);let e=t._prev,i=t._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,t}_addToMoves(t,e){return t.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=t:this._movesTail=this._movesTail._nextMoved=t),t}_addToRemovals(t){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Tc),this._unlinkedRecords.put(t),t.currentIndex=null,t._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=t,t._prevRemoved=null):(t._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=t),t}_addIdentityChange(t,e){return t.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=t:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=t,t}},Yf=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(t,e){this.item=t,this.trackById=e}},Xf=class{_head=null;_tail=null;add(t){this._head===null?(this._head=this._tail=t,t._nextDup=null,t._prevDup=null):(this._tail._nextDup=t,t._prevDup=this._tail,t._nextDup=null,this._tail=t)}get(t,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,t))return i;return null}remove(t){let e=t._prevDup,i=t._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Tc=class{map=new Map;put(t){let e=t.trackById,i=this.map.get(e);i||(i=new Xf,this.map.set(e,i)),i.add(t)}get(t,e){let i=t,r=this.map.get(i);return r?r.get(t,e):null}remove(t){let e=t.trackById;return this.map.get(e).remove(t)&&this.map.delete(e),t}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function Qy(n,t,e){let i=n.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+t+r}function Zy(){return new tm([new Qf])}var tm=(()=>{class n{factories;static \u0275prov=L({token:n,providedIn:"root",factory:Zy});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new n(e)}static extend(e){return{provide:n,useFactory:()=>{let i=p(n,{optional:!0,skipSelf:!0});return n.create(e,i||Zy())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new _(901,!1)}}return n})();var vn=(()=>{class n{static __NG_ELEMENT_ID__=PS}return n})();function PS(n){return LS($e(),V(),(n&16)===16)}function LS(n,t,e){if(An(n)&&!e){let i=Rt(n.index,t);return new li(i,i)}else if(n.type&175){let i=t[mt];return new li(i,t)}return null}var Jf=new w(""),VS=new w("");function Xo(n){return!n.moduleRef}function jS(n){let t=Xo(n)?n.r3Injector:n.moduleRef.injector,e=t.get(P);return e.run(()=>{Xo(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=t.get(ai),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Xo(n)){let o=()=>t.destroy(),s=n.platformInjector.get(Jf);s.add(o),t.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(Jf);s.add(o),n.moduleRef.onDestroy(()=>{Lo(n.allPlatformModules,n.moduleRef),r.unsubscribe(),s.delete(o)})}return HS(i,e,()=>{let o=t.get(Bi),s=o.add(),a=t.get(Uf);return a.runInitializers(),a.donePromise.then(()=>{let c=t.get(Yo,Ko);if(Ry(c||Ko),!t.get(VS,!0))return Xo(n)?t.get(Qt):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Xo(n)){let d=t.get(Qt);return n.rootComponent!==void 0&&d.bootstrap(n.rootComponent),d}else return BS?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var BS;function HS(n,t,e){try{let i=e();return Vr(i)?i.catch(r=>{throw t.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw t.runOutsideAngular(()=>n(i)),i}}var Mc=null;function US(n=[],t){return Se.create({name:t,providers:[{provide:Co,useValue:"platform"},{provide:Jf,useValue:new Set([()=>Mc=null])},...n]})}function $S(n=[]){if(Mc)return Mc;let t=US(n);return Mc=t,Ny(),zS(t),t}function zS(n){let t=n.get(Ua,null);Er(n,()=>{t?.forEach(e=>e())})}function rb(n){let{rootComponent:t,appProviders:e,platformProviders:i,platformRef:r}=n;se(ne.BootstrapApplicationStart);try{let o=r?.injector??$S(i),s=[Gy(),dg,...e||[]],a=new Uo({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return jS({r3Injector:a.injector,platformInjector:o,rootComponent:t})}catch(o){return Promise.reject(o)}finally{se(ne.BootstrapApplicationEnd)}}function pe(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function Hr(n,t=NaN){return!isNaN(parseFloat(n))&&!isNaN(Number(n))?Number(n):t}var Kf=Symbol("NOT_SET"),ob=new Set,GS=G(C({},co),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Kf,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(n){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Kf&&!cr(this))return this.signal;try{for(let r of this.cleanup??ob)r()}finally{this.cleanup?.clear()}let t=[];n!==void 0&&t.push(n),t.push(this.registerCleanupFn);let e=qn(this),i;try{i=this.userFn.apply(null,t)}finally{_i(this,e)}return(this.value===Kf||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),em=class extends rc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,e,i,r,o,s=null){super(t,[void 0,void 0,void 0,void 0],i,!1,o.get(fn),s),this.scheduler=r;for(let a of bf){let c=e[a];if(c===void 0)continue;let l=Object.create(GS);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(bi(l),l.value),l.signal[Le]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let e of t.cleanup??ob)e()}finally{Kn(t)}}};function sb(n,t){let e=t?.injector??p(Se),i=e.get(sn),r=e.get(yf),o=e.get(Pn,null,{optional:!0});r.impl??=e.get(Nv);let s=n;typeof s=="function"&&(s={mixedReadWrite:n});let a=e.get(No,null,{optional:!0}),c=new em(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function ab(n,t){let e=ei(n),i=t.elementInjector||_r();return new Fr(e).create(i,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}function cb(){return!1}function es(n,t){t=encodeURIComponent(t);for(let e of n.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===t)return decodeURIComponent(o)}return null}var lb=null;function Lt(){return lb}function nm(n){lb??=n}var ts=class{},Ac=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:()=>p(db),providedIn:"platform"})}return n})();var db=(()=>{class n extends Ac{_location;_history;_doc=p(X);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Lt().getBaseHref(this._doc)}onPopState(e){let i=Lt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Lt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:()=>new n,providedIn:"platform"})}return n})();var Bn=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let e=this.headers.get(t.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,e){return this.clone({name:t,value:e,op:"a"})}set(t,e){return this.clone({name:t,value:e,op:"s"})}delete(t,e){return this.clone({name:t,value:e,op:"d"})}maybeSetNormalizedName(t,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,t)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init(),Array.from(t.headers.keys()).forEach(e=>{this.headers.set(e,t.headers.get(e)),this.normalizedNames.set(e,t.normalizedNames.get(e))})}clone(t){let e=new n;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([t]),e}applyUpdate(t){let e=t.name.toLowerCase();switch(t.op){case"a":case"s":let i=t.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(t.name,e);let r=(t.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=t.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(t,e){let i=t.toLowerCase();this.maybeSetNormalizedName(t,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(t,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=t.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(t,r)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>t(this.normalizedNames.get(e),this.headers.get(e)))}};var kc=class{map=new Map;set(t,e){return this.map.set(t,e),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},Rc=class{encodeKey(t){return ub(t)}encodeValue(t){return ub(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function qS(n,t){let e=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[t.decodeKey(r),""]:[t.decodeKey(r.slice(0,o)),t.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var KS=/%(\d[a-f0-9])/gi,QS={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function ub(n){return encodeURIComponent(n).replace(KS,(t,e)=>QS[e]??t)}function Nc(n){return`${n}`}var jn=class n{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new Rc,t.fromString){if(t.fromObject)throw new _(2805,!1);this.map=qS(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(e=>{let i=t.fromObject[e],r=Array.isArray(i)?i.map(Nc):[Nc(i)];this.map.set(e,r)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let e=this.map.get(t);return e?e[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,e){return this.clone({param:t,value:e,op:"a"})}appendAll(t){let e=[];return Object.keys(t).forEach(i=>{let r=t[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(t,e){return this.clone({param:t,value:e,op:"s"})}delete(t,e){return this.clone({param:t,value:e,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let e=this.encoder.encodeKey(t);return this.map.get(t).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let e=new n({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(t),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(t=>this.map.set(t,this.cloneFrom.map.get(t))),this.updates.forEach(t=>{switch(t.op){case"a":case"s":let e=(t.op==="a"?this.map.get(t.param):void 0)||[];e.push(Nc(t.value)),this.map.set(t.param,e);break;case"d":if(t.value!==void 0){let i=this.map.get(t.param)||[],r=i.indexOf(Nc(t.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(t.param,i):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null)}};function ZS(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function fb(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function mb(n){return typeof Blob<"u"&&n instanceof Blob}function pb(n){return typeof FormData<"u"&&n instanceof FormData}function YS(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var im="Content-Type",hb="Accept",vb="text/plain",yb="application/json",XS=`${yb}, ${vb}, */*`,Ur=class n{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,e,i,r){this.url=e,this.method=t.toUpperCase();let o;if(ZS(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new _(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Bn,this.context??=new kc,!this.params)this.params=new jn,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),u=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+u+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||fb(this.body)||mb(this.body)||pb(this.body)||YS(this.body)?this.body:this.body instanceof jn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||pb(this.body)?null:mb(this.body)?this.body.type||null:fb(this.body)?null:typeof this.body=="string"?vb:this.body instanceof jn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?yb:null}clone(t={}){let e=t.method||this.method,i=t.url||this.url,r=t.responseType||this.responseType,o=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,c=t.mode||this.mode,l=t.redirect||this.redirect,d=t.credentials||this.credentials,u=t.referrer??this.referrer,g=t.integrity||this.integrity,f=t.referrerPolicy||this.referrerPolicy,v=t.transferCache??this.transferCache,E=t.timeout??this.timeout,D=t.body!==void 0?t.body:this.body,I=t.withCredentials??this.withCredentials,ee=t.reportProgress??this.reportProgress,be=t.reportUploadProgress??this.reportUploadProgress,_e=t.reportDownloadProgress??this.reportDownloadProgress,Ct=t.headers||this.headers,Be=t.params||this.params,Fe=t.context??this.context;return t.setHeaders!==void 0&&(Ct=Object.keys(t.setHeaders).reduce((Ne,ot)=>Ne.set(ot,t.setHeaders[ot]),Ct)),t.setParams&&(Be=Object.keys(t.setParams).reduce((Ne,ot)=>Ne.set(ot,t.setParams[ot]),Be)),new n(e,i,D,{params:Be,headers:Ct,context:Fe,reportProgress:ee,reportUploadProgress:be,reportDownloadProgress:_e,responseType:r,withCredentials:I,transferCache:v,keepalive:o,cache:a,priority:s,timeout:E,mode:c,redirect:l,credentials:d,referrer:u,integrity:g,referrerPolicy:f})}},$r=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})($r||{}),zr=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,e=200,i="OK"){this.headers=t.headers||new Bn,this.status=t.status!==void 0?t.status:e,this.statusText=t.statusText||i,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},Fc=class n extends zr{constructor(t={}){super(t)}type=$r.ResponseHeader;clone(t={}){return new n({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},ns=class n extends zr{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=$r.Response;clone(t={}){return new n({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},Xi=class extends zr{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},JS=200;var eM=/^\)\]\}',?\n/,b3=1024*1024,bb=new w("",{factory:()=>null}),Oc=(()=>{class n{fetchImpl=p(om,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=p(P);destroyRef=p(fn);maxResponseSize=p(bb);handle(e){return new Q(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(sm,s=>i.error(new Xi({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return bn(this,null,function*(){let o=this.createRequestInit(e),s;try{let D=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));tM(D),r.next({type:$r.Sent}),s=yield D}catch(D){r.error(new Xi({error:D,status:D.status??0,statusText:D.statusText,url:e.urlWithParams,headers:D.headers}));return}let a=new Bn(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,u=null,g=e.reportProgress||e.reportDownloadProgress;if(g&&r.next(new Fc({headers:a,status:d,statusText:c,url:l})),s.body){let D=s.headers.get("content-length"),I=D!==null?Number(D):NaN;this.maxResponseSize!==null&&Number.isFinite(I)&&I>this.maxResponseSize&&gb(this.maxResponseSize);let ee=[],be=s.body.getReader(),_e=0,Ct,Be,Fe=typeof Zone<"u"&&Zone.current,Ne=!1;if(yield this.ngZone.runOutsideAngular(()=>bn(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield be.cancel(),Ne=!0;break}let{done:xt,value:ar}=yield be.read();if(xt)break;if(ee.push(ar),_e+=ar.length,this.maxResponseSize!==null&&_e>this.maxResponseSize&&(yield be.cancel(),gb(this.maxResponseSize)),g){Be=e.responseType==="text"?(Be??"")+(Ct??=new TextDecoder).decode(ar,{stream:!0}):void 0;let nn=()=>r.next({type:$r.DownloadProgress,total:Number.isFinite(I)?I:void 0,loaded:_e,partialText:Be});Fe?Fe.run(nn):nn()}}})),Ne){r.complete();return}let ot=this.concatChunks(ee,_e);try{let xt=s.headers.get(im)??"";u=this.parseBody(e,ot,xt,d)}catch(xt){r.error(new Xi({error:xt,headers:new Bn(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=u?JS:0);let f=d>=200&&d<300,v=s.redirected,E=s.type;f?(r.next(new ns({body:u,headers:a,status:d,statusText:c,url:l,redirected:v,responseType:E})),r.complete()):r.error(new Xi({error:u,headers:a,status:d,statusText:c,url:l,redirected:v,responseType:E}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(eM,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new _(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(hb)||(i[hb]=XS),!e.headers.has(im)){let o=e.detectContentTypeHeader();o!==null&&(i[im]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})(),om=class{};function sm(){}function tM(n){n.then(sm,sm)}function gb(n){throw new _(2825,!1)}function nM(n,t){return t(n)}function iM(n,t,e){return(i,r)=>Er(e,()=>t(i,o=>n(o,r)))}var _b=new w("",{factory:()=>[]}),Eb=new w(""),wb=new w("",{factory:()=>!0});var am=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=T(Oc),r},providedIn:"root"})}return n})();var Pc=(()=>{class n{backend;injector;chain=null;pendingTasks=p(za);contributeToStability=p(wb);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(_b),...this.injector.get(Eb,[])]));this.chain=i.reduceRight((r,o)=>iM(r,o,this.injector),nM)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(mo(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(T(am),T(We))};static \u0275prov=L({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),cm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=T(Pc),r},providedIn:"root"})}return n})();function rm(n,t){return C({body:t},n)}var Gr=(()=>{class n{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Ur)o=e;else{let c;r.headers instanceof Bn?c=r.headers:c=new Bn(r.headers);let l;r.params&&(r.params instanceof jn?l=r.params:l=new jn({fromObject:r.params})),o=new Ur(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=st(o).pipe(md(c=>this.handler.handle(c)));if(e instanceof Ur||r.observe==="events")return s;let a=s.pipe(Mt(c=>c instanceof ns));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ie(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new _(2806,!1);return c.body}));case"blob":return a.pipe(ie(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new _(2807,!1);return c.body}));case"text":return a.pipe(ie(c=>{if(c.body!==null&&typeof c.body!="string")throw new _(2808,!1);return c.body}));default:return a.pipe(ie(c=>c.body))}case"response":return a;default:throw new _(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new jn().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,rm(r,i))}post(e,i,r={}){return this.request("POST",e,rm(r,i))}put(e,i,r={}){return this.request("PUT",e,rm(r,i))}static \u0275fac=function(i){return new(i||n)(T(cm))};static \u0275prov=L({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var rM=new w("",{factory:()=>!0}),oM="XSRF-TOKEN",sM=new w("",{factory:()=>oM}),aM="X-XSRF-TOKEN",cM=new w("",{factory:()=>aM}),lM=(()=>{class n{cookieName=p(sM);doc=p(X);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=es(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})(),Db=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=T(lM),r},providedIn:"root"})}return n})();function dM(n,t){if(!p(rM)||n.method==="GET"||n.method==="HEAD")return t(n);try{let r=p(Ac).href,{origin:o}=new URL(r),{origin:s}=new URL(n.url,o);if(o!==s)return t(n)}catch(r){return t(n)}let e=p(Db).getToken(),i=p(cM);return e!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,e)})),t(n)}function lm(...n){let t=[Gr,Oc,Pc,{provide:cm,useExisting:Pc},{provide:am,useFactory:()=>p(Oc)},{provide:_b,useValue:dM,multi:!0}];for(let e of n)t.push(...e.\u0275providers);return ni(t)}var it=(function(n){return n[n.Format=0]="Format",n[n.Standalone=1]="Standalone",n})(it||{}),me=(function(n){return n[n.Narrow=0]="Narrow",n[n.Abbreviated=1]="Abbreviated",n[n.Wide=2]="Wide",n[n.Short=3]="Short",n})(me||{}),gt=(function(n){return n[n.Short=0]="Short",n[n.Medium=1]="Medium",n[n.Long=2]="Long",n[n.Full=3]="Full",n})(gt||{}),Un={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Tb(n){return wt(n)[Oe.LocaleId]}function Ab(n,t,e){let i=wt(n),r=[i[Oe.DayPeriodsFormat],i[Oe.DayPeriodsStandalone]],o=Vt(r,t);return Vt(o,e)}function Nb(n,t,e){let i=wt(n),r=[i[Oe.DaysFormat],i[Oe.DaysStandalone]],o=Vt(r,t);return Vt(o,e)}function kb(n,t,e){let i=wt(n),r=[i[Oe.MonthsFormat],i[Oe.MonthsStandalone]],o=Vt(r,t);return Vt(o,e)}function Rb(n,t){let i=wt(n)[Oe.Eras];return Vt(i,t)}function is(n,t){let e=wt(n);return Vt(e[Oe.DateFormat],t)}function rs(n,t){let e=wt(n);return Vt(e[Oe.TimeFormat],t)}function os(n,t){let i=wt(n)[Oe.DateTimeFormat];return Vt(i,t)}function ss(n,t){let e=wt(n),i=e[Oe.NumberSymbols][t];if(typeof i>"u"){if(t===Un.CurrencyDecimal)return e[Oe.NumberSymbols][Un.Decimal];if(t===Un.CurrencyGroup)return e[Oe.NumberSymbols][Un.Group]}return i}function Fb(n){if(!n[Oe.ExtraData])throw new _(2303,!1)}function Ob(n){let t=wt(n);return Fb(t),(t[Oe.ExtraData][2]||[]).map(i=>typeof i=="string"?dm(i):[dm(i[0]),dm(i[1])])}function Pb(n,t,e){let i=wt(n);Fb(i);let r=[i[Oe.ExtraData][0],i[Oe.ExtraData][1]],o=Vt(r,t)||[];return Vt(o,e)||[]}function Vt(n,t){for(let e=t;e>-1;e--)if(typeof n[e]<"u")return n[e];throw new _(2304,!1)}function dm(n){let[t,e]=n.split(":");return{hours:+t,minutes:+e}}var uM=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Lc={},fM=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,mM=256;function Lb(n,t,e,i){let r=DM(n);pM(t),t=Hn(e,t)||t;let s=[],a;for(;t;)if(a=fM.exec(t),a){s=s.concat(a.slice(1));let d=s.pop();if(!d)break;t=d}else{s.push(t);break}let c=r.getTimezoneOffset();i&&(c=jb(i,c),r=wM(r,i));let l="";return s.forEach(d=>{let u=_M(d);l+=u?u(r,e,c):d==="''"?"'":d.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function pM(n){if(n.length>mM)throw new _(2300,!1)}function Uc(n,t,e){let i=new Date(0);return i.setFullYear(n,t,e),i.setHours(0,0,0),i}function Hn(n,t){let e=Tb(n);if(Lc[e]??={},Lc[e][t])return Lc[e][t];let i="";switch(t){case"shortDate":i=is(n,gt.Short);break;case"mediumDate":i=is(n,gt.Medium);break;case"longDate":i=is(n,gt.Long);break;case"fullDate":i=is(n,gt.Full);break;case"shortTime":i=rs(n,gt.Short);break;case"mediumTime":i=rs(n,gt.Medium);break;case"longTime":i=rs(n,gt.Long);break;case"fullTime":i=rs(n,gt.Full);break;case"short":let r=Hn(n,"shortTime"),o=Hn(n,"shortDate");i=Vc(os(n,gt.Short),[r,o]);break;case"medium":let s=Hn(n,"mediumTime"),a=Hn(n,"mediumDate");i=Vc(os(n,gt.Medium),[s,a]);break;case"long":let c=Hn(n,"longTime"),l=Hn(n,"longDate");i=Vc(os(n,gt.Long),[c,l]);break;case"full":let d=Hn(n,"fullTime"),u=Hn(n,"fullDate");i=Vc(os(n,gt.Full),[d,u]);break}return i&&(Lc[e][t]=i),i}function Vc(n,t){return t&&(n=n.replace(/\{([^}]+)}/g,function(e,i){return Object.hasOwn(t,i)?t[i]:e})),n}function Zt(n,t,e="-",i,r){let o="";(n<0||r&&n<=0)&&(r?n=-n+1:(n=-n,o=e));let s=String(n);for(;s.length<t;)s="0"+s;return i&&(s=s.slice(s.length-t)),o+s}function hM(n,t){return Zt(n,3).substring(0,t)}function Pe(n,t,e=0,i=!1,r=!1){return function(o,s){let a=gM(n,o);if((e>0||a>-e)&&(a+=e),n===3)a===0&&e===-12&&(a=12);else if(n===6)return hM(a,t);let c=ss(s,Un.MinusSign);return Zt(a,t,c,i,r)}}function gM(n,t){switch(n){case 0:return t.getFullYear();case 1:return t.getMonth();case 2:return t.getDate();case 3:return t.getHours();case 4:return t.getMinutes();case 5:return t.getSeconds();case 6:return t.getMilliseconds();case 7:return t.getDay();default:throw new _(2301,!1)}}function ve(n,t,e=it.Format,i=!1){return function(r,o){return vM(r,o,n,t,e,i)}}function vM(n,t,e,i,r,o){switch(e){case 2:return kb(t,r,i)[n.getMonth()];case 1:return Nb(t,r,i)[n.getDay()];case 0:let s=n.getHours(),a=n.getMinutes();if(o){let l=Ob(t),d=Pb(t,r,i),u=l.findIndex(g=>{if(Array.isArray(g)){let[f,v]=g,E=s>=f.hours&&a>=f.minutes,D=s<v.hours||s===v.hours&&a<v.minutes;if(f.hours<v.hours){if(E&&D)return!0}else if(E||D)return!0}else if(g.hours===s&&g.minutes===a)return!0;return!1});if(u!==-1)return d[u]}return Ab(t,r,i)[s<12?0:1];case 3:return Rb(t,i)[n.getFullYear()<=0?0:1];default:let c=e;throw new _(2302,!1)}}function jc(n){return function(t,e,i){let r=-1*i,o=ss(e,Un.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(n){case 0:return(r>=0?"+":"")+Zt(s,2,o)+Zt(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+Zt(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+Zt(s,2,o)+":"+Zt(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+Zt(s,2,o)+":"+Zt(Math.abs(r%60),2,o);default:throw new _(2310,!1)}}}var yM=0,Hc=4;function bM(n){let t=Uc(n,yM,1).getDay();return Uc(n,0,1+(t<=Hc?Hc:Hc+7)-t)}function Vb(n){let t=n.getDay(),e=t===0?-3:Hc-t;return Uc(n.getFullYear(),n.getMonth(),n.getDate()+e)}function um(n,t=!1){return function(e,i){let r;if(t){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,s=e.getDate();r=1+Math.floor((s+o)/7)}else{let o=Vb(e),s=bM(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return Zt(r,n,ss(i,Un.MinusSign))}}function Bc(n,t=!1){return function(e,i){let o=Vb(e).getFullYear();return Zt(o,n,ss(i,Un.MinusSign),t)}}var fm={};function _M(n){if(fm[n])return fm[n];let t;switch(n){case"G":case"GG":case"GGG":t=ve(3,me.Abbreviated);break;case"GGGG":t=ve(3,me.Wide);break;case"GGGGG":t=ve(3,me.Narrow);break;case"y":t=Pe(0,1,0,!1,!0);break;case"yy":t=Pe(0,2,0,!0,!0);break;case"yyy":t=Pe(0,3,0,!1,!0);break;case"yyyy":t=Pe(0,4,0,!1,!0);break;case"Y":t=Bc(1);break;case"YY":t=Bc(2,!0);break;case"YYY":t=Bc(3);break;case"YYYY":t=Bc(4);break;case"M":case"L":t=Pe(1,1,1);break;case"MM":case"LL":t=Pe(1,2,1);break;case"MMM":t=ve(2,me.Abbreviated);break;case"MMMM":t=ve(2,me.Wide);break;case"MMMMM":t=ve(2,me.Narrow);break;case"LLL":t=ve(2,me.Abbreviated,it.Standalone);break;case"LLLL":t=ve(2,me.Wide,it.Standalone);break;case"LLLLL":t=ve(2,me.Narrow,it.Standalone);break;case"w":t=um(1);break;case"ww":t=um(2);break;case"W":t=um(1,!0);break;case"d":t=Pe(2,1);break;case"dd":t=Pe(2,2);break;case"c":case"cc":t=Pe(7,1);break;case"ccc":t=ve(1,me.Abbreviated,it.Standalone);break;case"cccc":t=ve(1,me.Wide,it.Standalone);break;case"ccccc":t=ve(1,me.Narrow,it.Standalone);break;case"cccccc":t=ve(1,me.Short,it.Standalone);break;case"E":case"EE":case"EEE":t=ve(1,me.Abbreviated);break;case"EEEE":t=ve(1,me.Wide);break;case"EEEEE":t=ve(1,me.Narrow);break;case"EEEEEE":t=ve(1,me.Short);break;case"a":case"aa":case"aaa":t=ve(0,me.Abbreviated);break;case"aaaa":t=ve(0,me.Wide);break;case"aaaaa":t=ve(0,me.Narrow);break;case"b":case"bb":case"bbb":t=ve(0,me.Abbreviated,it.Standalone,!0);break;case"bbbb":t=ve(0,me.Wide,it.Standalone,!0);break;case"bbbbb":t=ve(0,me.Narrow,it.Standalone,!0);break;case"B":case"BB":case"BBB":t=ve(0,me.Abbreviated,it.Format,!0);break;case"BBBB":t=ve(0,me.Wide,it.Format,!0);break;case"BBBBB":t=ve(0,me.Narrow,it.Format,!0);break;case"h":t=Pe(3,1,-12);break;case"hh":t=Pe(3,2,-12);break;case"H":t=Pe(3,1);break;case"HH":t=Pe(3,2);break;case"m":t=Pe(4,1);break;case"mm":t=Pe(4,2);break;case"s":t=Pe(5,1);break;case"ss":t=Pe(5,2);break;case"S":t=Pe(6,1);break;case"SS":t=Pe(6,2);break;case"SSS":t=Pe(6,3);break;case"Z":case"ZZ":case"ZZZ":t=jc(0);break;case"ZZZZZ":t=jc(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":t=jc(1);break;case"OOOO":case"ZZZZ":case"zzzz":t=jc(2);break;default:return null}return fm[n]=t,t}function jb(n,t){n=n.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+n)/6e4;return isNaN(e)?t:e}function EM(n,t){return n=new Date(n.getTime()),n.setMinutes(n.getMinutes()+t),n}function wM(n,t,e){let r=n.getTimezoneOffset(),o=jb(t,r);return EM(n,-1*(o-r))}function DM(n){if(xb(n))return n;if(typeof n=="number"&&!isNaN(n))return new Date(n);if(typeof n=="string"){if(n=n.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(n)){let[r,o=1,s=1]=n.split("-").map(a=>+a);return Uc(r,o-1,s)}let e=parseFloat(n);if(!isNaN(n-e))return new Date(e);let i;if(i=n.match(uM))return CM(i)}let t=new Date(n);if(!xb(t))throw new _(2311,!1);return t}function CM(n){let t=new Date(0),e=0,i=0,r=n[8]?t.setUTCFullYear:t.setFullYear,o=n[8]?t.setUTCHours:t.setHours;n[9]&&(e=Number(n[9]+n[10]),i=Number(n[9]+n[11])),r.call(t,Number(n[1]),Number(n[2])-1,Number(n[3]));let s=Number(n[4]||0)-e,a=Number(n[5]||0)-i,c=Number(n[6]||0),l=Math.floor(parseFloat("0."+(n[7]||0))*1e3);return o.call(t,s,a,c,l),t}function xb(n){return n instanceof Date&&!isNaN(n.valueOf())}var mm=/\s+/,Ib=[],pm=(()=>{class n{_ngEl;_renderer;initialClasses=Ib;rawClass;stateMap=new Map;constructor(e,i){this._ngEl=e,this._renderer=i}set klass(e){this.initialClasses=e!=null?e.trim().split(mm):Ib}set ngClass(e){this.rawClass=typeof e=="string"?e.trim().split(mm):e}ngDoCheck(){for(let i of this.initialClasses)this._updateState(i,!0);let e=this.rawClass;if(Array.isArray(e)||e instanceof Set)for(let i of e)this._updateState(i,!0);else if(e!=null)for(let i of Object.keys(e))this._updateState(i,!!e[i]);this._applyStateDiff()}_updateState(e,i){let r=this.stateMap.get(e);r!==void 0?(r.enabled!==i&&(r.changed=!0,r.enabled=i),r.touched=!0):this.stateMap.set(e,{enabled:i,changed:!0,touched:!0})}_applyStateDiff(){for(let e of this.stateMap){let i=e[0],r=e[1];r.changed?(this._toggleClass(i,r.enabled),r.changed=!1):r.touched||(r.enabled&&this._toggleClass(i,!1),this.stateMap.delete(i)),r.touched=!1}}_toggleClass(e,i){e=e.trim(),e.length>0&&e.split(mm).forEach(r=>{i?this._renderer.addClass(this._ngEl.nativeElement,r):this._renderer.removeClass(this._ngEl.nativeElement,r)})}static \u0275fac=function(i){return new(i||n)(H(re),H(dt))};static \u0275dir=z({type:n,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return n})();var $c=class{$implicit;ngForOf;index;count;constructor(t,e,i,r){this.$implicit=t,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Gc=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new $c(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),Sb(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);Sb(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||n)(H(fi),H(di),H(tm))};static \u0275dir=z({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function Sb(n,t){n.context.$implicit=t.item}var hm=(()=>{class n{_viewContainer;_context=new zc;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){Mb(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){Mb(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||n)(H(fi),H(di))};static \u0275dir=z({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),zc=class{$implicit=null;ngIf=null};function Mb(n,t){if(n&&!n.createEmbeddedView)throw new _(2020,!1)}var gm=(()=>{class n{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=p(Se);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||n)(H(fi))};static \u0275dir=z({type:n,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ft]})}return n})();function xM(n,t){return new _(2100,!1)}var IM="mediumDate",Bb=new w(""),Hb=new w(""),vm=(()=>{class n{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let s=i??this.defaultOptions?.dateFormat??IM,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return Lb(e,s,o||this.locale,a)}catch(s){throw xM(n,s.message)}}static \u0275fac=function(i){return new(i||n)(H(Yo,16),H(Bb,24),H(Hb,24))};static \u0275pipe=jf({name:"date",type:n,pure:!0})}return n})();var Wc=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({})}return n})();var ym="browser";function Ub(n){return n===ym}var as=class{_doc;constructor(t){this._doc=t}manager},qc=(()=>{class n extends as{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||n)(T(X))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),Zc=new w(""),wm=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof qc));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof qc);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new _(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||n)(T(Zc),T(P))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),bm="ng-app-id";function $b(n){for(let t of n)t.remove()}function zb(n,t){let e=t.createElement("style");return e.textContent=n,e}function MM(n,t,e,i){let r=n.head?.querySelectorAll(`style[${bm}="${t}"],link[${bm}="${t}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(bm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Em(n,t){let e=t.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",n),e}var Dm=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,MM(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,zb);i?.forEach(r=>this.addUsage(r,this.external,Em))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&($b(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])$b(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,zb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Em(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||n)(T(X),T(Hi),T($i,8),T(Ui))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),_m={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Cm=/%COMP%/g;var Wb="%COMP%",TM=`_nghost-${Wb}`,AM=`_ngcontent-${Wb}`,NM=!0,kM=new w("",{factory:()=>NM});function RM(n){return AM.replace(Cm,n)}function FM(n){return TM.replace(Cm,n)}function qb(n,t){return t.map(e=>e.replace(Cm,n))}var ds=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new cs(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Qc?r.applyToHost(e):r instanceof ls&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case Kt.Emulated:o=new Qc(c,l,i,this.appId,d,s,a,u);break;case Kt.ShadowDom:return new Kc(c,e,i,s,a,this.nonce,u,l);case Kt.ExperimentalIsolatedShadowDom:return new Kc(c,e,i,s,a,this.nonce,u);default:o=new ls(c,l,i,d,s,a,u);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||n)(T(wm),T(Yi),T(Hi),T(kM),T(X),T(P),T($i),T(Pn,8))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),cs=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,e,i,r){this.eventManager=t,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(t,e){return e?this.doc.createElementNS(_m[e]||e,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,e){(Gb(t)?t.content:t).appendChild(e)}insertBefore(t,e,i){t&&(Gb(t)?t.content:t).insertBefore(e,i)}removeChild(t,e){e.remove()}selectRootElement(t,e){let i=typeof t=="string"?this.doc.querySelector(t):t;if(!i)throw new _(-5104,!1);return e||(i.textContent=""),i}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,e,i,r){if(r){e=r+":"+e;let o=_m[r];o?t.setAttributeNS(o,e,i):t.setAttribute(e,i)}else t.setAttribute(e,i)}removeAttribute(t,e,i){if(i){let r=_m[i];r?t.removeAttributeNS(r,e):t.removeAttribute(`${i}:${e}`)}else t.removeAttribute(e)}addClass(t,e){t.classList.add(e)}removeClass(t,e){t.classList.remove(e)}setStyle(t,e,i,r){r&(pn.DashCase|pn.Important)?t.style.setProperty(e,i,r&pn.Important?"important":""):t.style[e]=i}removeStyle(t,e,i){i&pn.DashCase?t.style.removeProperty(e):t.style[e]=""}setProperty(t,e,i){t!=null&&(t[e]=i)}setValue(t,e){t.nodeValue=e}listen(t,e,i,r){if(typeof t=="string"&&(t=Lt().getGlobalEventTarget(this.doc,t),!t))throw new _(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(t,e,o)),this.eventManager.addEventListener(t,e,o,r)}decoratePreventDefault(t){return e=>{if(e==="__ngUnwrap__")return t;t(e)===!1&&e.preventDefault()}}};function Gb(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Kc=class extends cs{hostEl;sharedStylesHost;shadowRoot;constructor(t,e,i,r,o,s,a,c){super(t,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=qb(i.id,l);for(let u of l){let g=document.createElement("style");s&&g.setAttribute("nonce",s),g.textContent=u,this.shadowRoot.appendChild(g)}let d=i.getExternalStyles?.();if(d)for(let u of d){let g=Em(u,r);s&&g.setAttribute("nonce",s),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,e){return super.appendChild(this.nodeOrShadowRoot(t),e)}insertBefore(t,e,i){return super.insertBefore(this.nodeOrShadowRoot(t),e,i)}removeChild(t,e){return super.removeChild(null,e)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ls=class extends cs{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,e,i,r,o,s,a,c){super(t,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?qb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Wi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Qc=class extends ls{contentAttr;hostAttr;constructor(t,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(t,e,i,o,s,a,c,l),this.contentAttr=RM(l),this.hostAttr=FM(l)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,e){let i=super.createElement(t,e);return super.setAttribute(i,this.contentAttr,""),i}};var Yc=class n extends ts{supportsDOMEvents=!0;static makeCurrent(){nm(new n)}onAndCancel(t,e,i,r){return t.addEventListener(e,i,r),()=>{t.removeEventListener(e,i,r)}}dispatchEvent(t,e){t.dispatchEvent(e)}remove(t){t.remove()}createElement(t,e){return e=e||this.getDefaultDocument(),e.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,e){return e==="window"?window:e==="document"?t:e==="body"?t.body:null}getBaseHref(t){let e=OM();return e==null?null:PM(e)}resetBaseElement(){us=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return es(document.cookie,t)}},us=null;function OM(){return us=us||document.head.querySelector("base"),us?us.getAttribute("href"):null}function PM(n){return new URL(n,document.baseURI).pathname}var Kb=["alt","control","meta","shift"],LM={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},VM={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Qb=(()=>{class n extends as{constructor(e){super(e)}supports(e){return n.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=n.parseEventName(i),a=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Lt().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=n._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),Kb.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=LM[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Kb.forEach(s=>{if(s!==r){let a=VM[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{n.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||n)(T(X))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})();function xm(n,t,e){return bn(this,null,function*(){let i=C({rootComponent:n},jM(t,e));return rb(i)})}function jM(n,t){return{platformRef:t?.platformRef,appProviders:[...zM,...n?.providers??[]],platformProviders:$M}}function BM(){Yc.makeCurrent()}function HM(){return new ft}function UM(){return lf(document),document}var $M=[{provide:Ui,useValue:ym},{provide:Ua,useValue:BM,multi:!0},{provide:X,useFactory:UM}];var zM=[{provide:Co,useValue:"root"},{provide:ft,useFactory:HM},{provide:Zc,useClass:qc,multi:!0},{provide:Zc,useClass:Qb,multi:!0},ds,{provide:Yi,useClass:Dm},{provide:Dm,useExisting:Yi},wm,{provide:ct,useExisting:ds},[]];var Im=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=T(GM),r},providedIn:"root"})}return n})(),GM=(()=>{class n extends Im{_doc=p(X);sanitize(e,i){if(i==null)return null;switch(e){case lt.NONE:return i;case lt.HTML:return Zi(i,"HTML")?Fn(i):hf(this._doc,String(i)).toString();case lt.STYLE:return Zi(i,"Style")?Fn(i):i;case lt.SCRIPT:if(Zi(i,"Script"))return Fn(i);throw new _(5200,!1);case lt.URL:return Zi(i,"URL")?Fn(i):gc(String(i));case lt.RESOURCE_URL:if(Zi(i,"ResourceURL"))return Fn(i);throw new _(5201,!1);default:throw new _(5202,!1)}}bypassSecurityTrustHtml(e){return df(e)}bypassSecurityTrustStyle(e){return uf(e)}bypassSecurityTrustScript(e){return ff(e)}bypassSecurityTrustUrl(e){return mf(e)}bypassSecurityTrustResourceUrl(e){return pf(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var $=(function(n){return n[n.State=0]="State",n[n.Transition=1]="Transition",n[n.Sequence=2]="Sequence",n[n.Group=3]="Group",n[n.Animate=4]="Animate",n[n.Keyframes=5]="Keyframes",n[n.Style=6]="Style",n[n.Trigger=7]="Trigger",n[n.Reference=8]="Reference",n[n.AnimateChild=9]="AnimateChild",n[n.AnimateRef=10]="AnimateRef",n[n.Query=11]="Query",n[n.Stagger=12]="Stagger",n})($||{}),Yt="*";function Zb(n,t=null){return{type:$.Sequence,steps:n,options:t}}function Sm(n){return{type:$.Style,styles:n,offset:null}}var $n=class{_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_originalOnDoneFns=[];_originalOnStartFns=[];_started=!1;_destroyed=!1;_finished=!1;_position=0;parentPlayer=null;totalTime;constructor(t=0,e=0){this.totalTime=t+e}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}init(){}play(){this.hasStarted()||(this._onStart(),this.triggerMicrotask()),this._started=!0}triggerMicrotask(){queueMicrotask(()=>this._onFinish())}_onStart(){this._onStartFns.forEach(t=>t()),this._onStartFns=[]}pause(){}restart(){}finish(){this._onFinish()}destroy(){this._destroyed||(this._destroyed=!0,this.hasStarted()||this._onStart(),this.finish(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this._started=!1,this._finished=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}setPosition(t){this._position=this.totalTime?t*this.totalTime:1}getPosition(){return this.totalTime?this._position/this.totalTime:1}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Wr=class{_onDoneFns=[];_onStartFns=[];_finished=!1;_started=!1;_destroyed=!1;_onDestroyFns=[];parentPlayer=null;totalTime=0;players;constructor(t){this.players=t;let e=0,i=0,r=0,o=this.players.length;o==0?queueMicrotask(()=>this._onFinish()):this.players.forEach(s=>{s.onDone(()=>{++e==o&&this._onFinish()}),s.onDestroy(()=>{++i==o&&this._onDestroy()}),s.onStart(()=>{++r==o&&this._onStart()})}),this.totalTime=this.players.reduce((s,a)=>Math.max(s,a.totalTime),0)}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this.players.forEach(t=>t.init())}onStart(t){this._onStartFns.push(t)}_onStart(){this.hasStarted()||(this._started=!0,this._onStartFns.forEach(t=>t()),this._onStartFns=[])}onDone(t){this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}play(){this.parentPlayer||this.init(),this._onStart(),this.players.forEach(t=>t.play())}pause(){this.players.forEach(t=>t.pause())}restart(){this.players.forEach(t=>t.restart())}finish(){this._onFinish(),this.players.forEach(t=>t.finish())}destroy(){this._onDestroy()}_onDestroy(){this._destroyed||(this._destroyed=!0,this._onFinish(),this.players.forEach(t=>t.destroy()),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this.players.forEach(t=>t.reset()),this._destroyed=!1,this._finished=!1,this._started=!1}setPosition(t){let e=t*this.totalTime;this.players.forEach(i=>{let r=i.totalTime?Math.min(1,e/i.totalTime):1;i.setPosition(r)})}getPosition(){let t=this.players.reduce((e,i)=>e===null||i.totalTime>e.totalTime?i:e,null);return t!=null?t.getPosition():0}beforeDestroy(){this.players.forEach(t=>{t.beforeDestroy&&t.beforeDestroy()})}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},fs="!";function Yb(n){return new _(3e3,!1)}function qM(){return new _(3100,!1)}function KM(){return new _(3101,!1)}function QM(n){return new _(3001,!1)}function ZM(n){return new _(3003,!1)}function YM(n){return new _(3004,!1)}function Jb(n,t){return new _(3005,!1)}function e_(){return new _(3006,!1)}function t_(){return new _(3007,!1)}function n_(n,t){return new _(3008,!1)}function i_(n){return new _(3002,!1)}function r_(n,t,e,i,r){return new _(3010,!1)}function o_(){return new _(3011,!1)}function s_(){return new _(3012,!1)}function a_(){return new _(3200,!1)}function c_(){return new _(3202,!1)}function l_(){return new _(3013,!1)}function d_(n){return new _(3014,!1)}function u_(n){return new _(3015,!1)}function f_(n){return new _(3016,!1)}function m_(n,t){return new _(3404,!1)}function XM(n){return new _(3502,!1)}function p_(n){return new _(3503,!1)}function h_(){return new _(3300,!1)}function g_(n){return new _(3504,!1)}function v_(n){return new _(3301,!1)}function y_(n,t){return new _(3302,!1)}function b_(n){return new _(3303,!1)}function __(n,t){return new _(3400,!1)}function E_(n){return new _(3401,!1)}function w_(n){return new _(3402,!1)}function D_(n,t){return new _(3505,!1)}function zn(n){switch(n.length){case 0:return new $n;case 1:return n[0];default:return new Wr(n)}}function Nm(n,t,e=new Map,i=new Map){let r=[],o=[],s=-1,a=null;if(t.forEach(c=>{let l=c.get("offset"),d=l==s,u=d&&a||new Map;c.forEach((g,f)=>{let v=f,E=g;if(f!=="offset")switch(v=n.normalizePropertyName(v,r),E){case fs:E=e.get(f);break;case Yt:E=i.get(f);break;default:E=n.normalizeStyleValue(f,v,E,r);break}u.set(v,E)}),d||o.push(u),a=u,s=l}),r.length)throw XM(r);return o}function Xc(n,t,e,i){switch(t){case"start":n.onStart(()=>i(e&&Mm(e,"start",n)));break;case"done":n.onDone(()=>i(e&&Mm(e,"done",n)));break;case"destroy":n.onDestroy(()=>i(e&&Mm(e,"destroy",n)));break}}function Mm(n,t,e){let i=e.totalTime,r=!!e.disabled,o=Jc(n.element,n.triggerName,n.fromState,n.toState,t||n.phaseName,i??n.totalTime,r),s=n._data;return s!=null&&(o._data=s),o}function Jc(n,t,e,i,r="",o=0,s){return{element:n,triggerName:t,fromState:e,toState:i,phaseName:r,totalTime:o,disabled:!!s}}function vt(n,t,e){let i=n.get(t);return i||n.set(t,i=e),i}function km(n){let t=n.indexOf(":"),e=n.substring(1,t),i=n.slice(t+1);return[e,i]}var JM=typeof document>"u"?null:document.documentElement;function el(n){let t=n.parentNode||n.host||null;return t===JM?null:t}function eT(n){return n.substring(1,6)=="ebkit"}var Ji=null,Xb=!1;function C_(n){Ji||(Ji=tT()||{},Xb=Ji.style?"WebkitAppearance"in Ji.style:!1);let t=!0;return Ji.style&&!eT(n)&&(t=n in Ji.style,!t&&Xb&&(t="Webkit"+n.charAt(0).toUpperCase()+n.slice(1)in Ji.style)),t}function tT(){return typeof document<"u"?document.body:null}function Rm(n,t){for(;t;){if(t===n)return!0;t=el(t)}return!1}function Fm(n,t,e){if(e)return Array.from(n.querySelectorAll(t));let i=n.querySelector(t);return i?[i]:[]}var nT=1e3,Om="{{",iT="}}",Pm="ng-enter",tl="ng-leave",ms="ng-trigger",ps=".ng-trigger",Lm="ng-animating",nl=".ng-animating";function yn(n){if(typeof n=="number")return n;let t=n.match(/^(-?[\.\d]+)(m?s)/);return!t||t.length<2?0:Tm(parseFloat(t[1]),t[2])}function Tm(n,t){return t==="s"?n*nT:n}function hs(n,t,e){return n.hasOwnProperty("duration")?n:oT(n,t,e)}var rT=/^(-?[\.\d]+)(m?s)(?:\s+(-?[\.\d]+)(m?s))?(?:\s+([-a-z]+(?:\(.+?\))?))?$/i;function oT(n,t,e){let i,r=0,o="";if(typeof n=="string"){let s=n.match(rT);if(s===null)return t.push(Yb(n)),{duration:0,delay:0,easing:""};i=Tm(parseFloat(s[1]),s[2]);let a=s[3];a!=null&&(r=Tm(parseFloat(a),s[4]));let c=s[5];c&&(o=c)}else i=n;if(!e){let s=!1,a=t.length;i<0&&(t.push(qM()),s=!0),r<0&&(t.push(KM()),s=!0),s&&t.splice(a,0,Yb(n))}return{duration:i,delay:r,easing:o}}function x_(n){return n.length?n[0]instanceof Map?n:n.map(t=>new Map(Object.entries(t))):[]}function Xt(n,t,e){t.forEach((i,r)=>{let o=il(r);e&&!e.has(r)&&e.set(r,n.style[o]),n.style[o]=i})}function pi(n,t){t.forEach((e,i)=>{let r=il(i);n.style[r]=""})}function qr(n){return Array.isArray(n)?n.length==1?n[0]:Zb(n):n}function I_(n,t,e){let i=t.params||{},r=Vm(n);r.length&&r.forEach(o=>{i.hasOwnProperty(o)||e.push(QM(o))})}var Am=new RegExp(`${Om}\\s*(.+?)\\s*${iT}`,"g");function Vm(n){let t=[];if(typeof n=="string"){let e;for(;e=Am.exec(n);)t.push(e[1]);Am.lastIndex=0}return t}function Kr(n,t,e){let i=`${n}`,r=i.replace(Am,(o,s)=>{let a=t[s];return a==null&&(e.push(ZM(s)),a=""),a.toString()});return r==i?n:r}var sT=/-+([a-z0-9])/g;function il(n){return n.replace(sT,(...t)=>t[1].toUpperCase())}function S_(n,t){return n===0||t===0}function M_(n,t,e){if(e.size&&t.length){let i=t[0],r=[];if(e.forEach((o,s)=>{i.has(s)||r.push(s),i.set(s,o)}),r.length)for(let o=1;o<t.length;o++){let s=t[o];r.forEach(a=>s.set(a,rl(n,a)))}}return t}function yt(n,t,e){switch(t.type){case $.Trigger:return n.visitTrigger(t,e);case $.State:return n.visitState(t,e);case $.Transition:return n.visitTransition(t,e);case $.Sequence:return n.visitSequence(t,e);case $.Group:return n.visitGroup(t,e);case $.Animate:return n.visitAnimate(t,e);case $.Keyframes:return n.visitKeyframes(t,e);case $.Style:return n.visitStyle(t,e);case $.Reference:return n.visitReference(t,e);case $.AnimateChild:return n.visitAnimateChild(t,e);case $.AnimateRef:return n.visitAnimateRef(t,e);case $.Query:return n.visitQuery(t,e);case $.Stagger:return n.visitStagger(t,e);default:throw YM(t.type)}}function rl(n,t){return window.getComputedStyle(n)[t]}var tp=(()=>{class n{validateStyleProperty(e){return C_(e)}containsElement(e,i){return Rm(e,i)}getParentElement(e){return el(e)}query(e,i,r){return Fm(e,i,r)}computeStyle(e,i,r){return r||""}animate(e,i,r,o,s,a=[],c){return new $n(r,o)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),tr=class{static NOOP=new tp},nr=class{};var aT=new Set(["width","height","minWidth","minHeight","maxWidth","maxHeight","left","top","bottom","right","fontSize","outlineWidth","outlineOffset","paddingTop","paddingLeft","paddingBottom","paddingRight","marginTop","marginLeft","marginBottom","marginRight","borderRadius","borderWidth","borderTopWidth","borderLeftWidth","borderRightWidth","borderBottomWidth","textIndent","perspective"]),ll=class extends nr{normalizePropertyName(t,e){return il(t)}normalizeStyleValue(t,e,i,r){let o="",s=i.toString().trim();if(aT.has(e)&&i!==0&&i!=="0")if(typeof i=="number")o="px";else{let a=i.match(/^[+-]?[\d\.]+([a-z]*)$/);a&&a[1].length==0&&r.push(Jb(t,i))}return s+o}};var dl="*";function cT(n,t){let e=[];return typeof n=="string"?n.split(/\s*,\s*/).forEach(i=>lT(i,e,t)):e.push(n),e}function lT(n,t,e){if(n[0]==":"){let c=dT(n,e);if(typeof c=="function"){t.push(c);return}n=c}let i=n.match(/^(\*|[-\w]+)\s*(<?[=-]>)\s*(\*|[-\w]+)$/);if(i==null||i.length<4)return e.push(u_(n)),t;let r=i[1],o=i[2],s=i[3];t.push(T_(r,s));let a=r==dl&&s==dl;o[0]=="<"&&!a&&t.push(T_(s,r))}function dT(n,t){switch(n){case":enter":return"void => *";case":leave":return"* => void";case":increment":return(e,i)=>parseFloat(i)>parseFloat(e);case":decrement":return(e,i)=>parseFloat(i)<parseFloat(e);default:return t.push(f_(n)),"* => *"}}var ol=new Set(["true","1"]),sl=new Set(["false","0"]);function T_(n,t){let e=ol.has(n)||sl.has(n),i=ol.has(t)||sl.has(t);return(r,o)=>{let s=n==dl||n==r,a=t==dl||t==o;return!s&&e&&typeof r=="boolean"&&(s=r?ol.has(n):sl.has(n)),!a&&i&&typeof o=="boolean"&&(a=o?ol.has(t):sl.has(t)),s&&a}}var j_=":self",uT=new RegExp(`s*${j_}s*,?`,"g");function B_(n,t,e,i){return new zm(n).build(t,e,i)}var A_="",zm=class{_driver;constructor(t){this._driver=t}build(t,e,i){let r=new Gm(e);return this._resetContextStyleTimingState(r),yt(this,qr(t),r)}_resetContextStyleTimingState(t){t.currentQuerySelector=A_,t.collectedStyles=new Map,t.collectedStyles.set(A_,new Map),t.currentTime=0}visitTrigger(t,e){let i=e.queryCount=0,r=e.depCount=0,o=[],s=[];return t.name.charAt(0)=="@"&&e.errors.push(e_()),t.definitions.forEach(a=>{if(this._resetContextStyleTimingState(e),a.type==$.State){let c=a,l=c.name;l.toString().split(/\s*,\s*/).forEach(d=>{c.name=d,o.push(this.visitState(c,e))}),c.name=l}else if(a.type==$.Transition){let c=this.visitTransition(a,e);i+=c.queryCount,r+=c.depCount,s.push(c)}else e.errors.push(t_())}),{type:$.Trigger,name:t.name,states:o,transitions:s,queryCount:i,depCount:r,options:null}}visitState(t,e){let i=this.visitStyle(t.styles,e),r=t.options&&t.options.params||null;if(i.containsDynamicStyles){let o=new Set,s=r||{};i.styles.forEach(a=>{a instanceof Map&&a.forEach(c=>{Vm(c).forEach(l=>{s.hasOwnProperty(l)||o.add(l)})})}),o.size&&e.errors.push(n_(t.name,[...o.values()]))}return{type:$.State,name:t.name,style:i,options:r?{params:r}:null}}visitTransition(t,e){e.queryCount=0,e.depCount=0;let i=yt(this,qr(t.animation),e),r=cT(t.expr,e.errors);return{type:$.Transition,matchers:r,animation:i,queryCount:e.queryCount,depCount:e.depCount,options:er(t.options)}}visitSequence(t,e){return{type:$.Sequence,steps:t.steps.map(i=>yt(this,i,e)),options:er(t.options)}}visitGroup(t,e){let i=e.currentTime,r=0,o=t.steps.map(s=>{e.currentTime=i;let a=yt(this,s,e);return r=Math.max(r,e.currentTime),a});return e.currentTime=r,{type:$.Group,steps:o,options:er(t.options)}}visitAnimate(t,e){let i=hT(t.timings,e.errors);e.currentAnimateTimings=i;let r,o=t.styles?t.styles:Sm({});if(o.type==$.Keyframes)r=this.visitKeyframes(o,e);else{let s=t.styles,a=!1;if(!s){a=!0;let l={};i.easing&&(l.easing=i.easing),s=Sm(l)}e.currentTime+=i.duration+i.delay;let c=this.visitStyle(s,e);c.isEmptyStep=a,r=c}return e.currentAnimateTimings=null,{type:$.Animate,timings:i,style:r,options:null}}visitStyle(t,e){let i=this._makeStyleAst(t,e);return this._validateStyleAst(i,e),i}_makeStyleAst(t,e){let i=[],r=Array.isArray(t.styles)?t.styles:[t.styles];for(let a of r)typeof a=="string"?a===Yt?i.push(a):e.errors.push(i_(a)):i.push(new Map(Object.entries(a)));let o=!1,s=null;return i.forEach(a=>{if(a instanceof Map&&(a.has("easing")&&(s=a.get("easing"),a.delete("easing")),!o)){for(let c of a.values())if(c.toString().indexOf(Om)>=0){o=!0;break}}}),{type:$.Style,styles:i,easing:s,offset:t.offset,containsDynamicStyles:o,options:null}}_validateStyleAst(t,e){let i=e.currentAnimateTimings,r=e.currentTime,o=e.currentTime;i&&o>0&&(o-=i.duration+i.delay),t.styles.forEach(s=>{typeof s!="string"&&s.forEach((a,c)=>{let l=e.collectedStyles.get(e.currentQuerySelector),d=l.get(c),u=!0;d&&(o!=r&&o>=d.startTime&&r<=d.endTime&&(e.errors.push(r_(c,d.startTime,d.endTime,o,r)),u=!1),o=d.startTime),u&&l.set(c,{startTime:o,endTime:r}),e.options&&I_(a,e.options,e.errors)})})}visitKeyframes(t,e){let i={type:$.Keyframes,styles:[],options:null};if(!e.currentAnimateTimings)return e.errors.push(o_()),i;let r=1,o=0,s=[],a=!1,c=!1,l=0,d=t.steps.map(I=>{let ee=this._makeStyleAst(I,e),be=ee.offset!=null?ee.offset:pT(ee.styles),_e=0;return be!=null&&(o++,_e=ee.offset=be),c=c||_e<0||_e>1,a=a||_e<l,l=_e,s.push(_e),ee});c&&e.errors.push(s_()),a&&e.errors.push(a_());let u=t.steps.length,g=0;o>0&&o<u?e.errors.push(c_()):o==0&&(g=r/(u-1));let f=u-1,v=e.currentTime,E=e.currentAnimateTimings,D=E.duration;return d.forEach((I,ee)=>{let be=g>0?ee==f?1:g*ee:s[ee],_e=be*D;e.currentTime=v+E.delay+_e,E.duration=_e,this._validateStyleAst(I,e),I.offset=be,i.styles.push(I)}),i}visitReference(t,e){return{type:$.Reference,animation:yt(this,qr(t.animation),e),options:er(t.options)}}visitAnimateChild(t,e){return e.depCount++,{type:$.AnimateChild,options:er(t.options)}}visitAnimateRef(t,e){return{type:$.AnimateRef,animation:this.visitReference(t.animation,e),options:er(t.options)}}visitQuery(t,e){let i=e.currentQuerySelector,r=t.options||{};e.queryCount++,e.currentQuery=t;let[o,s]=fT(t.selector);e.currentQuerySelector=i.length?i+" "+o:o,vt(e.collectedStyles,e.currentQuerySelector,new Map);let a=yt(this,qr(t.animation),e);return e.currentQuery=null,e.currentQuerySelector=i,{type:$.Query,selector:o,limit:r.limit||0,optional:!!r.optional,includeSelf:s,animation:a,originalSelector:t.selector,options:er(t.options)}}visitStagger(t,e){e.currentQuery||e.errors.push(l_());let i=t.timings==="full"?{duration:0,delay:0,easing:"full"}:hs(t.timings,e.errors,!0);return{type:$.Stagger,animation:yt(this,qr(t.animation),e),timings:i,options:null}}};function fT(n){let t=!!n.split(/\s*,\s*/).find(e=>e==j_);return t&&(n=n.replace(uT,"")),n=n.replace(/@\*/g,ps).replace(/@\w+/g,e=>ps+"-"+e.slice(1)).replace(/:animating/g,nl),[n,t]}function mT(n){return n?C({},n):null}var Gm=class{errors;queryCount=0;depCount=0;currentTransition=null;currentQuery=null;currentQuerySelector=null;currentAnimateTimings=null;currentTime=0;collectedStyles=new Map;options=null;unsupportedCSSPropertiesFound=new Set;constructor(t){this.errors=t}};function pT(n){if(typeof n=="string")return null;let t=null;if(Array.isArray(n))n.forEach(e=>{if(e instanceof Map&&e.has("offset")){let i=e;t=parseFloat(i.get("offset")),i.delete("offset")}});else if(n instanceof Map&&n.has("offset")){let e=n;t=parseFloat(e.get("offset")),e.delete("offset")}return t}function hT(n,t){if(n.hasOwnProperty("duration"))return n;if(typeof n=="number"){let o=hs(n,t).duration;return jm(o,0,"")}let e=n;if(e.split(/\s+/).some(o=>o.charAt(0)=="{"&&o.charAt(1)=="{")){let o=jm(0,0,"");return o.dynamic=!0,o.strValue=e,o}let r=hs(e,t);return jm(r.duration,r.delay,r.easing)}function er(n){return n?(n=C({},n),n.params&&(n.params=mT(n.params))):n={},n}function jm(n,t,e){return{duration:n,delay:t,easing:e}}function np(n,t,e,i,r,o,s=null,a=!1){return{type:1,element:n,keyframes:t,preStyleProps:e,postStyleProps:i,duration:r,delay:o,totalTime:r+o,easing:s,subTimeline:a}}var vs=class{_map=new Map;get(t){return this._map.get(t)||[]}append(t,e){let i=this._map.get(t);i||this._map.set(t,i=[]),i.push(...e)}has(t){return this._map.has(t)}clear(){this._map.clear()}},gT=1,vT=":enter",yT=new RegExp(vT,"g"),bT=":leave",_T=new RegExp(bT,"g");function H_(n,t,e,i,r,o=new Map,s=new Map,a,c,l=[]){return new Wm().buildKeyframes(n,t,e,i,r,o,s,a,c,l)}var Wm=class{buildKeyframes(t,e,i,r,o,s,a,c,l,d=[]){l=l||new vs;let u=new qm(t,e,l,r,o,d,[]);u.options=c;let g=c.delay?yn(c.delay):0;u.currentTimeline.delayNextStep(g),u.currentTimeline.setStyles([s],null,u.errors,c),yt(this,i,u);let f=u.timelines.filter(v=>v.containsAnimation());if(f.length&&a.size){let v;for(let E=f.length-1;E>=0;E--){let D=f[E];if(D.element===e){v=D;break}}v&&!v.allowOnlyTimelineStyles()&&v.setStyles([a],null,u.errors,c)}return f.length?f.map(v=>v.buildKeyframes()):[np(e,[],[],[],0,g,"",!1)]}visitTrigger(t,e){}visitState(t,e){}visitTransition(t,e){}visitAnimateChild(t,e){let i=e.subInstructions.get(e.element);if(i){let r=e.createSubContext(t.options),o=e.currentTimeline.currentTime,s=this._visitSubInstructions(i,r,r.options);o!=s&&e.transformIntoNewTimeline(s)}e.previousNode=t}visitAnimateRef(t,e){let i=e.createSubContext(t.options);i.transformIntoNewTimeline(),this._applyAnimationRefDelays([t.options,t.animation.options],e,i),this.visitReference(t.animation,i),e.transformIntoNewTimeline(i.currentTimeline.currentTime),e.previousNode=t}_applyAnimationRefDelays(t,e,i){for(let r of t){let o=r?.delay;if(o){let s=typeof o=="number"?o:yn(Kr(o,r?.params??{},e.errors));i.delayNextStep(s)}}}_visitSubInstructions(t,e,i){let o=e.currentTimeline.currentTime,s=i.duration!=null?yn(i.duration):null,a=i.delay!=null?yn(i.delay):null;return s!==0&&t.forEach(c=>{let l=e.appendInstructionToTimeline(c,s,a);o=Math.max(o,l.duration+l.delay)}),o}visitReference(t,e){e.updateOptions(t.options,!0),yt(this,t.animation,e),e.previousNode=t}visitSequence(t,e){let i=e.subContextCount,r=e,o=t.options;if(o&&(o.params||o.delay)&&(r=e.createSubContext(o),r.transformIntoNewTimeline(),o.delay!=null)){r.previousNode.type==$.Style&&(r.currentTimeline.snapshotCurrentStyles(),r.previousNode=ul);let s=yn(o.delay);r.delayNextStep(s)}t.steps.length&&(t.steps.forEach(s=>yt(this,s,r)),r.currentTimeline.applyStylesToKeyframe(),r.subContextCount>i&&r.transformIntoNewTimeline()),e.previousNode=t}visitGroup(t,e){let i=[],r=e.currentTimeline.currentTime,o=t.options&&t.options.delay?yn(t.options.delay):0;t.steps.forEach(s=>{let a=e.createSubContext(t.options);o&&a.delayNextStep(o),yt(this,s,a),r=Math.max(r,a.currentTimeline.currentTime),i.push(a.currentTimeline)}),i.forEach(s=>e.currentTimeline.mergeTimelineCollectedStyles(s)),e.transformIntoNewTimeline(r),e.previousNode=t}_visitTiming(t,e){if(t.dynamic){let i=t.strValue,r=e.params?Kr(i,e.params,e.errors):i;return hs(r,e.errors)}else return{duration:t.duration,delay:t.delay,easing:t.easing}}visitAnimate(t,e){let i=e.currentAnimateTimings=this._visitTiming(t.timings,e),r=e.currentTimeline;i.delay&&(e.incrementTime(i.delay),r.snapshotCurrentStyles());let o=t.style;o.type==$.Keyframes?this.visitKeyframes(o,e):(e.incrementTime(i.duration),this.visitStyle(o,e),r.applyStylesToKeyframe()),e.currentAnimateTimings=null,e.previousNode=t}visitStyle(t,e){let i=e.currentTimeline,r=e.currentAnimateTimings;!r&&i.hasCurrentStyleProperties()&&i.forwardFrame();let o=r&&r.easing||t.easing;t.isEmptyStep?i.applyEmptyStep(o):i.setStyles(t.styles,o,e.errors,e.options),e.previousNode=t}visitKeyframes(t,e){let i=e.currentAnimateTimings,r=e.currentTimeline.duration,o=i.duration,a=e.createSubContext().currentTimeline;a.easing=i.easing,t.styles.forEach(c=>{let l=c.offset||0;a.forwardTime(l*o),a.setStyles(c.styles,c.easing,e.errors,e.options),a.applyStylesToKeyframe()}),e.currentTimeline.mergeTimelineCollectedStyles(a),e.transformIntoNewTimeline(r+o),e.previousNode=t}visitQuery(t,e){let i=e.currentTimeline.currentTime,r=t.options||{},o=r.delay?yn(r.delay):0;o&&(e.previousNode.type===$.Style||i==0&&e.currentTimeline.hasCurrentStyleProperties())&&(e.currentTimeline.snapshotCurrentStyles(),e.previousNode=ul);let s=i,a=e.invokeQuery(t.selector,t.originalSelector,t.limit,t.includeSelf,!!r.optional,e.errors);e.currentQueryTotal=a.length;let c=null;a.forEach((l,d)=>{e.currentQueryIndex=d;let u=e.createSubContext(t.options,l);o&&u.delayNextStep(o),l===e.element&&(c=u.currentTimeline),yt(this,t.animation,u),u.currentTimeline.applyStylesToKeyframe();let g=u.currentTimeline.currentTime;s=Math.max(s,g)}),e.currentQueryIndex=0,e.currentQueryTotal=0,e.transformIntoNewTimeline(s),c&&(e.currentTimeline.mergeTimelineCollectedStyles(c),e.currentTimeline.snapshotCurrentStyles()),e.previousNode=t}visitStagger(t,e){let i=e.parentContext,r=e.currentTimeline,o=t.timings,s=Math.abs(o.duration),a=s*(e.currentQueryTotal-1),c=s*e.currentQueryIndex;switch(o.duration<0?"reverse":o.easing){case"reverse":c=a-c;break;case"full":c=i.currentStaggerTime;break}let d=e.currentTimeline;c&&d.delayNextStep(c);let u=d.currentTime;yt(this,t.animation,e),e.previousNode=t,i.currentStaggerTime=r.currentTime-u+(r.startTime-i.currentTimeline.startTime)}},ul={},qm=class n{_driver;element;subInstructions;_enterClassName;_leaveClassName;errors;timelines;parentContext=null;currentTimeline;currentAnimateTimings=null;previousNode=ul;subContextCount=0;options={};currentQueryIndex=0;currentQueryTotal=0;currentStaggerTime=0;constructor(t,e,i,r,o,s,a,c){this._driver=t,this.element=e,this.subInstructions=i,this._enterClassName=r,this._leaveClassName=o,this.errors=s,this.timelines=a,this.currentTimeline=c||new fl(this._driver,e,0),a.push(this.currentTimeline)}get params(){return this.options.params}updateOptions(t,e){if(!t)return;let i=t,r=this.options;i.duration!=null&&(r.duration=yn(i.duration)),i.delay!=null&&(r.delay=yn(i.delay));let o=i.params;if(o){let s=r.params;s||(s=this.options.params={}),Object.keys(o).forEach(a=>{(!e||!s.hasOwnProperty(a))&&(s[a]=Kr(o[a],s,this.errors))})}}_copyOptions(){let t={};if(this.options){let e=this.options.params;if(e){let i=t.params={};Object.keys(e).forEach(r=>{i[r]=e[r]})}}return t}createSubContext(t=null,e,i){let r=e||this.element,o=new n(this._driver,r,this.subInstructions,this._enterClassName,this._leaveClassName,this.errors,this.timelines,this.currentTimeline.fork(r,i||0));return o.previousNode=this.previousNode,o.currentAnimateTimings=this.currentAnimateTimings,o.options=this._copyOptions(),o.updateOptions(t),o.currentQueryIndex=this.currentQueryIndex,o.currentQueryTotal=this.currentQueryTotal,o.parentContext=this,this.subContextCount++,o}transformIntoNewTimeline(t){return this.previousNode=ul,this.currentTimeline=this.currentTimeline.fork(this.element,t),this.timelines.push(this.currentTimeline),this.currentTimeline}appendInstructionToTimeline(t,e,i){let r={duration:e??t.duration,delay:this.currentTimeline.currentTime+(i??0)+t.delay,easing:""},o=new Km(this._driver,t.element,t.keyframes,t.preStyleProps,t.postStyleProps,r,t.stretchStartingKeyframe);return this.timelines.push(o),r}incrementTime(t){this.currentTimeline.forwardTime(this.currentTimeline.duration+t)}delayNextStep(t){t>0&&this.currentTimeline.delayNextStep(t)}invokeQuery(t,e,i,r,o,s){let a=[];if(r&&a.push(this.element),t.length>0){t=t.replace(yT,"."+this._enterClassName),t=t.replace(_T,"."+this._leaveClassName);let c=i!=1,l=this._driver.query(this.element,t,c);i!==0&&(l=i<0?l.slice(l.length+i,l.length):l.slice(0,i)),a.push(...l)}return!o&&a.length==0&&s.push(d_(e)),a}},fl=class n{_driver;element;startTime;_elementTimelineStylesLookup;duration=0;easing=null;_previousKeyframe=new Map;_currentKeyframe=new Map;_keyframes=new Map;_styleSummary=new Map;_localTimelineStyles=new Map;_globalTimelineStyles;_pendingStyles=new Map;_backFill=new Map;_currentEmptyStepKeyframe=null;constructor(t,e,i,r){this._driver=t,this.element=e,this.startTime=i,this._elementTimelineStylesLookup=r,this._elementTimelineStylesLookup||(this._elementTimelineStylesLookup=new Map),this._globalTimelineStyles=this._elementTimelineStylesLookup.get(e),this._globalTimelineStyles||(this._globalTimelineStyles=this._localTimelineStyles,this._elementTimelineStylesLookup.set(e,this._localTimelineStyles)),this._loadKeyframe()}containsAnimation(){switch(this._keyframes.size){case 0:return!1;case 1:return this.hasCurrentStyleProperties();default:return!0}}hasCurrentStyleProperties(){return this._currentKeyframe.size>0}get currentTime(){return this.startTime+this.duration}delayNextStep(t){let e=this._keyframes.size===1&&this._pendingStyles.size;this.duration||e?(this.forwardTime(this.currentTime+t),e&&this.snapshotCurrentStyles()):this.startTime+=t}fork(t,e){return this.applyStylesToKeyframe(),new n(this._driver,t,e||this.currentTime,this._elementTimelineStylesLookup)}_loadKeyframe(){this._currentKeyframe&&(this._previousKeyframe=this._currentKeyframe),this._currentKeyframe=this._keyframes.get(this.duration),this._currentKeyframe||(this._currentKeyframe=new Map,this._keyframes.set(this.duration,this._currentKeyframe))}forwardFrame(){this.duration+=gT,this._loadKeyframe()}forwardTime(t){this.applyStylesToKeyframe(),this.duration=t,this._loadKeyframe()}_updateStyle(t,e){this._localTimelineStyles.set(t,e),this._globalTimelineStyles.set(t,e),this._styleSummary.set(t,{time:this.currentTime,value:e})}allowOnlyTimelineStyles(){return this._currentEmptyStepKeyframe!==this._currentKeyframe}applyEmptyStep(t){t&&this._previousKeyframe.set("easing",t);for(let[e,i]of this._globalTimelineStyles)this._backFill.set(e,i||Yt),this._currentKeyframe.set(e,Yt);this._currentEmptyStepKeyframe=this._currentKeyframe}setStyles(t,e,i,r){e&&this._previousKeyframe.set("easing",e);let o=r&&r.params||{},s=ET(t,this._globalTimelineStyles);for(let[a,c]of s){let l=Kr(c,o,i);this._pendingStyles.set(a,l),this._localTimelineStyles.has(a)||this._backFill.set(a,this._globalTimelineStyles.get(a)??Yt),this._updateStyle(a,l)}}applyStylesToKeyframe(){this._pendingStyles.size!=0&&(this._pendingStyles.forEach((t,e)=>{this._currentKeyframe.set(e,t)}),this._pendingStyles.clear(),this._localTimelineStyles.forEach((t,e)=>{this._currentKeyframe.has(e)||this._currentKeyframe.set(e,t)}))}snapshotCurrentStyles(){for(let[t,e]of this._localTimelineStyles)this._pendingStyles.set(t,e),this._updateStyle(t,e)}getFinalKeyframe(){return this._keyframes.get(this.duration)}get properties(){let t=[];for(let e in this._currentKeyframe)t.push(e);return t}mergeTimelineCollectedStyles(t){t._styleSummary.forEach((e,i)=>{let r=this._styleSummary.get(i);(!r||e.time>r.time)&&this._updateStyle(i,e.value)})}buildKeyframes(){this.applyStylesToKeyframe();let t=new Set,e=new Set,i=this._keyframes.size===1&&this.duration===0,r=[];this._keyframes.forEach((a,c)=>{let l=new Map([...this._backFill,...a]);l.forEach((d,u)=>{d===fs?t.add(u):d===Yt&&e.add(u)}),i||l.set("offset",c/this.duration),r.push(l)});let o=[...t.values()],s=[...e.values()];if(i){let a=r[0],c=new Map(a);a.set("offset",0),c.set("offset",1),r=[a,c]}return np(this.element,r,o,s,this.duration,this.startTime,this.easing,!1)}},Km=class extends fl{keyframes;preStyleProps;postStyleProps;_stretchStartingKeyframe;timings;constructor(t,e,i,r,o,s,a=!1){super(t,e,s.delay),this.keyframes=i,this.preStyleProps=r,this.postStyleProps=o,this._stretchStartingKeyframe=a,this.timings={duration:s.duration,delay:s.delay,easing:s.easing}}containsAnimation(){return this.keyframes.length>1}buildKeyframes(){let t=this.keyframes,{delay:e,duration:i,easing:r}=this.timings;if(this._stretchStartingKeyframe&&e){let o=[],s=i+e,a=e/s,c=new Map(t[0]);c.set("offset",0),o.push(c);let l=new Map(t[0]);l.set("offset",N_(a)),o.push(l);let d=t.length-1;for(let u=1;u<=d;u++){let g=new Map(t[u]),f=g.get("offset"),v=e+f*i;g.set("offset",N_(v/s)),o.push(g)}i=s,e=0,r="",t=o}return np(this.element,t,this.preStyleProps,this.postStyleProps,i,e,r,!0)}};function N_(n,t=3){let e=Math.pow(10,t-1);return Math.round(n*e)/e}function ET(n,t){let e=new Map,i;return n.forEach(r=>{if(r==="*"){i??=t.keys();for(let o of i)e.set(o,Yt)}else for(let[o,s]of r)e.set(o,s)}),e}function k_(n,t,e,i,r,o,s,a,c,l,d,u,g){return{type:0,element:n,triggerName:t,isRemovalTransition:r,fromState:e,fromStyles:o,toState:i,toStyles:s,timelines:a,queriedElements:c,preStyleProps:l,postStyleProps:d,totalTime:u,errors:g}}var Bm={},ml=class{_triggerName;ast;_stateStyles;constructor(t,e,i){this._triggerName=t,this.ast=e,this._stateStyles=i}match(t,e,i,r){return wT(this.ast.matchers,t,e,i,r)}buildStyles(t,e,i){let r=this._stateStyles.get("*");return t!==void 0&&(r=this._stateStyles.get(t?.toString())||r),r?r.buildStyles(e,i):new Map}build(t,e,i,r,o,s,a,c,l,d){let u=[],g=this.ast.options&&this.ast.options.params||Bm,f=a&&a.params||Bm,v=this.buildStyles(i,f,u),E=c&&c.params||Bm,D=this.buildStyles(r,E,u),I=new Set,ee=new Map,be=new Map,_e=r==="void",Ct={params:U_(E,g),delay:this.ast.options?.delay},Be=d?[]:H_(t,e,this.ast.animation,o,s,v,D,Ct,l,u),Fe=0;return Be.forEach(Ne=>{Fe=Math.max(Ne.duration+Ne.delay,Fe)}),u.length?k_(e,this._triggerName,i,r,_e,v,D,[],[],ee,be,Fe,u):(Be.forEach(Ne=>{let ot=Ne.element,xt=vt(ee,ot,new Set);Ne.preStyleProps.forEach(nn=>xt.add(nn));let ar=vt(be,ot,new Set);Ne.postStyleProps.forEach(nn=>ar.add(nn)),ot!==e&&I.add(ot)}),k_(e,this._triggerName,i,r,_e,v,D,Be,[...I.values()],ee,be,Fe))}};function wT(n,t,e,i,r){return n.some(o=>o(t,e,i,r))}function U_(n,t){let e=C({},t);return Object.entries(n).forEach(([i,r])=>{r!=null&&(e[i]=r)}),e}var Qm=class{styles;defaultParams;normalizer;constructor(t,e,i){this.styles=t,this.defaultParams=e,this.normalizer=i}buildStyles(t,e){let i=new Map,r=U_(t,this.defaultParams);return this.styles.styles.forEach(o=>{typeof o!="string"&&o.forEach((s,a)=>{s&&(s=Kr(s,r,e));let c=this.normalizer.normalizePropertyName(a,e);s=this.normalizer.normalizeStyleValue(a,c,s,e),i.set(a,s)})}),i}};function DT(n,t,e){return new Zm(n,t,e)}var Zm=class{name;ast;_normalizer;transitionFactories=[];fallbackTransition;states=new Map;constructor(t,e,i){this.name=t,this.ast=e,this._normalizer=i,e.states.forEach(r=>{let o=r.options&&r.options.params||{};this.states.set(r.name,new Qm(r.style,o,i))}),R_(this.states,"true","1"),R_(this.states,"false","0"),e.transitions.forEach(r=>{this.transitionFactories.push(new ml(t,r,this.states))}),this.fallbackTransition=CT(t,this.states)}get containsQueries(){return this.ast.queryCount>0}matchTransition(t,e,i,r){return this.transitionFactories.find(s=>s.match(t,e,i,r))||null}matchStyles(t,e,i){return this.fallbackTransition.buildStyles(t,e,i)}};function CT(n,t,e){let i=[(s,a)=>!0],r={type:$.Sequence,steps:[],options:null},o={type:$.Transition,animation:r,matchers:i,options:null,queryCount:0,depCount:0};return new ml(n,o,t)}function R_(n,t,e){n.has(t)?n.has(e)||n.set(e,n.get(t)):n.has(e)&&n.set(t,n.get(e))}var xT=new vs,Ym=class{bodyNode;_driver;_normalizer;_animations=new Map;_playersById=new Map;players=[];constructor(t,e,i){this.bodyNode=t,this._driver=e,this._normalizer=i}register(t,e){let i=[],r=[],o=B_(this._driver,e,i,r);if(i.length)throw p_(i);this._animations.set(t,o)}_buildPlayer(t,e,i){let r=t.element,o=Nm(this._normalizer,t.keyframes,e,i);return this._driver.animate(r,o,t.duration,t.delay,t.easing,[],!0)}create(t,e,i={}){let r=[],o=this._animations.get(t),s,a=new Map;if(o?(s=H_(this._driver,e,o,Pm,tl,new Map,new Map,i,xT,r),s.forEach(d=>{let u=vt(a,d.element,new Map);d.postStyleProps.forEach(g=>u.set(g,null))})):(r.push(h_()),s=[]),r.length)throw g_(r);a.forEach((d,u)=>{d.forEach((g,f)=>{d.set(f,this._driver.computeStyle(u,f,Yt))})});let c=s.map(d=>{let u=a.get(d.element);return this._buildPlayer(d,new Map,u)}),l=zn(c);return this._playersById.set(t,l),l.onDestroy(()=>this.destroy(t)),this.players.push(l),l}destroy(t){let e=this._getPlayer(t);e.destroy(),this._playersById.delete(t);let i=this.players.indexOf(e);i>=0&&this.players.splice(i,1)}_getPlayer(t){let e=this._playersById.get(t);if(!e)throw v_(t);return e}listen(t,e,i,r){let o=Jc(e,"","","");return Xc(this._getPlayer(t),i,o,r),()=>{}}command(t,e,i,r){if(i=="register"){this.register(t,r[0]);return}if(i=="create"){let s=r[0]||{};this.create(t,e,s);return}let o=this._getPlayer(t);switch(i){case"play":o.play();break;case"pause":o.pause();break;case"reset":o.reset();break;case"restart":o.restart();break;case"finish":o.finish();break;case"init":o.init();break;case"setPosition":o.setPosition(parseFloat(r[0]));break;case"destroy":this.destroy(t);break}}},F_="ng-animate-queued",IT=".ng-animate-queued",Hm="ng-animate-disabled",ST=".ng-animate-disabled",MT="ng-star-inserted",TT=".ng-star-inserted",AT=[],$_={namespaceId:"",setForRemoval:!1,setForMove:!1,hasAnimation:!1,removedBeforeQueried:!1},NT={namespaceId:"",setForMove:!1,setForRemoval:!1,hasAnimation:!1,removedBeforeQueried:!0},Jt="__ng_removed",ys=class{namespaceId;value;options;get params(){return this.options.params}constructor(t,e=""){this.namespaceId=e;let i=t&&t.hasOwnProperty("value"),r=i?t.value:t;if(this.value=RT(r),i){let o=t,{value:s}=o,a=Vs(o,["value"]);this.options=a}else this.options={};this.options.params||(this.options.params={})}absorbOptions(t){let e=t.params;if(e){let i=this.options.params;Object.keys(e).forEach(r=>{i[r]==null&&(i[r]=e[r])})}}},gs="void",Um=new ys(gs),Xm=class{id;hostElement;_engine;players=[];_triggers=new Map;_queue=[];_elementListeners=new Map;_hostClassName;constructor(t,e,i){this.id=t,this.hostElement=e,this._engine=i,this._hostClassName="ng-tns-"+t,jt(e,this._hostClassName)}listen(t,e,i,r){if(!this._triggers.has(e))throw y_(i,e);if(i==null||i.length==0)throw b_(e);if(!FT(i))throw __(i,e);let o=vt(this._elementListeners,t,[]),s={name:e,phase:i,callback:r};o.push(s);let a=vt(this._engine.statesByElement,t,new Map);return a.has(e)||(jt(t,ms),jt(t,ms+"-"+e),a.set(e,Um)),()=>{this._engine.afterFlush(()=>{let c=o.indexOf(s);c>=0&&o.splice(c,1),this._triggers.has(e)||a.delete(e)})}}register(t,e){return this._triggers.has(t)?!1:(this._triggers.set(t,e),!0)}_getTrigger(t){let e=this._triggers.get(t);if(!e)throw E_(t);return e}trigger(t,e,i,r=!0){let o=this._getTrigger(e),s=new bs(this.id,e,t),a=this._engine.statesByElement.get(t);a||(jt(t,ms),jt(t,ms+"-"+e),this._engine.statesByElement.set(t,a=new Map));let c=a.get(e),l=new ys(i,this.id);if(!(i&&i.hasOwnProperty("value"))&&c&&l.absorbOptions(c.options),a.set(e,l),c||(c=Um),!(l.value===gs)&&c.value===l.value){if(!LT(c.params,l.params)){let E=[],D=o.matchStyles(c.value,c.params,E),I=o.matchStyles(l.value,l.params,E);E.length?this._engine.reportError(E):this._engine.afterFlush(()=>{pi(t,D),Xt(t,I)})}return}let g=vt(this._engine.playersByElement,t,[]);g.forEach(E=>{E.namespaceId==this.id&&E.triggerName==e&&E.queued&&E.destroy()});let f=o.matchTransition(c.value,l.value,t,l.params),v=!1;if(!f){if(!r)return;f=o.fallbackTransition,v=!0}return this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:e,transition:f,fromState:c,toState:l,player:s,isFallbackTransition:v}),v||(jt(t,F_),s.onStart(()=>{Qr(t,F_)})),s.onDone(()=>{let E=this.players.indexOf(s);E>=0&&this.players.splice(E,1);let D=this._engine.playersByElement.get(t);if(D){let I=D.indexOf(s);I>=0&&D.splice(I,1)}}),this.players.push(s),g.push(s),s}deregister(t){this._triggers.delete(t),this._engine.statesByElement.forEach(e=>e.delete(t)),this._elementListeners.forEach((e,i)=>{this._elementListeners.set(i,e.filter(r=>r.name!=t))})}clearElementCache(t){this._engine.statesByElement.delete(t),this._elementListeners.delete(t);let e=this._engine.playersByElement.get(t);e&&(e.forEach(i=>i.destroy()),this._engine.playersByElement.delete(t))}_signalRemovalForInnerTriggers(t,e){let i=this._engine.driver.query(t,ps,!0);i.forEach(r=>{if(r[Jt])return;let o=this._engine.fetchNamespacesByElement(r);o.size?o.forEach(s=>s.triggerLeaveAnimation(r,e,!1,!0)):this.clearElementCache(r)}),this._engine.afterFlushAnimationsDone(()=>i.forEach(r=>this.clearElementCache(r)))}triggerLeaveAnimation(t,e,i,r){let o=this._engine.statesByElement.get(t),s=new Map;if(o){let a=[];if(o.forEach((c,l)=>{if(s.set(l,c.value),this._triggers.has(l)){let d=this.trigger(t,l,gs,r);d&&a.push(d)}}),a.length)return this._engine.markElementAsRemoved(this.id,t,!0,e,s),i&&zn(a).onDone(()=>this._engine.processLeaveNode(t)),!0}return!1}prepareLeaveAnimationListeners(t){let e=this._elementListeners.get(t),i=this._engine.statesByElement.get(t);if(e&&i){let r=new Set;e.forEach(o=>{let s=o.name;if(r.has(s))return;r.add(s);let c=this._triggers.get(s).fallbackTransition,l=i.get(s)||Um,d=new ys(gs),u=new bs(this.id,s,t);this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:s,transition:c,fromState:l,toState:d,player:u,isFallbackTransition:!0})})}}removeNode(t,e){let i=this._engine;if(t.childElementCount&&this._signalRemovalForInnerTriggers(t,e),this.triggerLeaveAnimation(t,e,!0))return;let r=!1;if(i.totalAnimations){let o=i.players.length?i.playersByQueriedElement.get(t):[];if(o&&o.length)r=!0;else{let s=t;for(;s=s.parentNode;)if(i.statesByElement.get(s)){r=!0;break}}}if(this.prepareLeaveAnimationListeners(t),r)i.markElementAsRemoved(this.id,t,!1,e);else{let o=t[Jt];(!o||o===$_)&&(i.afterFlush(()=>this.clearElementCache(t)),i.destroyInnerAnimations(t),i._onRemovalComplete(t,e))}}insertNode(t,e){jt(t,this._hostClassName)}drainQueuedTransitions(t){let e=[];return this._queue.forEach(i=>{let r=i.player;if(r.destroyed)return;let o=i.element,s=this._elementListeners.get(o);s&&s.forEach(a=>{if(a.name==i.triggerName){let c=Jc(o,i.triggerName,i.fromState.value,i.toState.value);c._data=t,Xc(i.player,a.phase,c,a.callback)}}),r.markedForDestroy?this._engine.afterFlush(()=>{r.destroy()}):e.push(i)}),this._queue=[],e.sort((i,r)=>{let o=i.transition.ast.depCount,s=r.transition.ast.depCount;return o==0||s==0?o-s:this._engine.driver.containsElement(i.element,r.element)?1:-1})}destroy(t){this.players.forEach(e=>e.destroy()),this._signalRemovalForInnerTriggers(this.hostElement,t)}},Jm=class{bodyNode;driver;_normalizer;players=[];newHostElements=new Map;playersByElement=new Map;playersByQueriedElement=new Map;statesByElement=new Map;disabledNodes=new Set;totalAnimations=0;totalQueuedPlayers=0;_namespaceLookup={};_namespaceList=[];_flushFns=[];_whenQuietFns=[];namespacesByHostElement=new Map;collectedEnterElements=[];collectedLeaveElements=[];onRemovalComplete=(t,e)=>{};_onRemovalComplete(t,e){this.onRemovalComplete(t,e)}constructor(t,e,i){this.bodyNode=t,this.driver=e,this._normalizer=i}get queuedPlayers(){let t=[];return this._namespaceList.forEach(e=>{e.players.forEach(i=>{i.queued&&t.push(i)})}),t}createNamespace(t,e){let i=new Xm(t,e,this);return this.bodyNode&&this.driver.containsElement(this.bodyNode,e)?this._balanceNamespaceList(i,e):(this.newHostElements.set(e,i),this.collectEnterElement(e)),this._namespaceLookup[t]=i}_balanceNamespaceList(t,e){let i=this._namespaceList,r=this.namespacesByHostElement;if(i.length-1>=0){let s=!1,a=this.driver.getParentElement(e);for(;a;){let c=r.get(a);if(c){let l=i.indexOf(c);i.splice(l+1,0,t),s=!0;break}a=this.driver.getParentElement(a)}s||i.unshift(t)}else i.push(t);return r.set(e,t),t}register(t,e){let i=this._namespaceLookup[t];return i||(i=this.createNamespace(t,e)),i}registerTrigger(t,e,i){let r=this._namespaceLookup[t];r&&r.register(e,i)&&this.totalAnimations++}destroy(t,e){t&&(this.afterFlush(()=>{}),this.afterFlushAnimationsDone(()=>{let i=this._fetchNamespace(t);this.namespacesByHostElement.delete(i.hostElement);let r=this._namespaceList.indexOf(i);r>=0&&this._namespaceList.splice(r,1),i.destroy(e),delete this._namespaceLookup[t]}))}_fetchNamespace(t){return this._namespaceLookup[t]}fetchNamespacesByElement(t){let e=new Set,i=this.statesByElement.get(t);if(i){for(let r of i.values())if(r.namespaceId){let o=this._fetchNamespace(r.namespaceId);o&&e.add(o)}}return e}trigger(t,e,i,r){if(al(e)){let o=this._fetchNamespace(t);if(o)return o.trigger(e,i,r),!0}return!1}insertNode(t,e,i,r){if(!al(e))return;let o=e[Jt];if(o&&o.setForRemoval){o.setForRemoval=!1,o.setForMove=!0;let s=this.collectedLeaveElements.indexOf(e);s>=0&&this.collectedLeaveElements.splice(s,1)}if(t){let s=this._fetchNamespace(t);s&&s.insertNode(e,i)}r&&this.collectEnterElement(e)}collectEnterElement(t){this.collectedEnterElements.push(t)}markElementAsDisabled(t,e){e?this.disabledNodes.has(t)||(this.disabledNodes.add(t),jt(t,Hm)):this.disabledNodes.has(t)&&(this.disabledNodes.delete(t),Qr(t,Hm))}removeNode(t,e,i){if(al(e)){let r=t?this._fetchNamespace(t):null;r?r.removeNode(e,i):this.markElementAsRemoved(t,e,!1,i);let o=this.namespacesByHostElement.get(e);o&&o.id!==t&&o.removeNode(e,i)}else this._onRemovalComplete(e,i)}markElementAsRemoved(t,e,i,r,o){this.collectedLeaveElements.push(e),e[Jt]={namespaceId:t,setForRemoval:r,hasAnimation:i,removedBeforeQueried:!1,previousTriggersValues:o}}listen(t,e,i,r,o){return al(e)?this._fetchNamespace(t).listen(e,i,r,o):()=>{}}_buildInstruction(t,e,i,r,o){return t.transition.build(this.driver,t.element,t.fromState.value,t.toState.value,i,r,t.fromState.options,t.toState.options,e,o)}destroyInnerAnimations(t){let e=this.driver.query(t,ps,!0);e.forEach(i=>this.destroyActiveAnimationsForElement(i)),this.playersByQueriedElement.size!=0&&(e=this.driver.query(t,nl,!0),e.forEach(i=>this.finishActiveQueriedAnimationOnElement(i)))}destroyActiveAnimationsForElement(t){let e=this.playersByElement.get(t);e&&e.forEach(i=>{i.queued?i.markedForDestroy=!0:i.destroy()})}finishActiveQueriedAnimationOnElement(t){let e=this.playersByQueriedElement.get(t);e&&e.forEach(i=>i.finish())}whenRenderingDone(){return new Promise(t=>{if(this.players.length)return zn(this.players).onDone(()=>t());t()})}processLeaveNode(t){let e=t[Jt];if(e&&e.setForRemoval){if(t[Jt]=$_,e.namespaceId){this.destroyInnerAnimations(t);let i=this._fetchNamespace(e.namespaceId);i&&i.clearElementCache(t)}this._onRemovalComplete(t,e.setForRemoval)}t.classList?.contains(Hm)&&this.markElementAsDisabled(t,!1),this.driver.query(t,ST,!0).forEach(i=>{this.markElementAsDisabled(i,!1)})}flush(t=-1){let e=[];if(this.newHostElements.size&&(this.newHostElements.forEach((i,r)=>this._balanceNamespaceList(i,r)),this.newHostElements.clear()),this.totalAnimations&&this.collectedEnterElements.length)for(let i=0;i<this.collectedEnterElements.length;i++){let r=this.collectedEnterElements[i];jt(r,MT)}if(this._namespaceList.length&&(this.totalQueuedPlayers||this.collectedLeaveElements.length)){let i=[];try{e=this._flushAnimations(i,t)}finally{for(let r=0;r<i.length;r++)i[r]()}}else for(let i=0;i<this.collectedLeaveElements.length;i++){let r=this.collectedLeaveElements[i];this.processLeaveNode(r)}if(this.totalQueuedPlayers=0,this.collectedEnterElements.length=0,this.collectedLeaveElements.length=0,this._flushFns.forEach(i=>i()),this._flushFns=[],this._whenQuietFns.length){let i=this._whenQuietFns;this._whenQuietFns=[],e.length?zn(e).onDone(()=>{i.forEach(r=>r())}):i.forEach(r=>r())}}reportError(t){throw w_(t)}_flushAnimations(t,e){let i=new vs,r=[],o=new Map,s=[],a=new Map,c=new Map,l=new Map,d=new Set;this.disabledNodes.forEach(x=>{d.add(x);let M=this.driver.query(x,IT,!0);for(let k=0;k<M.length;k++)d.add(M[k])});let u=this.bodyNode,g=Array.from(this.statesByElement.keys()),f=L_(g,this.collectedEnterElements),v=new Map,E=0;f.forEach((x,M)=>{let k=Pm+E++;v.set(M,k),x.forEach(te=>jt(te,k))});let D=[],I=new Set,ee=new Set;for(let x=0;x<this.collectedLeaveElements.length;x++){let M=this.collectedLeaveElements[x],k=M[Jt];k&&k.setForRemoval&&(D.push(M),I.add(M),k.hasAnimation?this.driver.query(M,TT,!0).forEach(te=>I.add(te)):ee.add(M))}let be=new Map,_e=L_(g,Array.from(I));_e.forEach((x,M)=>{let k=tl+E++;be.set(M,k),x.forEach(te=>jt(te,k))}),t.push(()=>{f.forEach((x,M)=>{let k=v.get(M);x.forEach(te=>Qr(te,k))}),_e.forEach((x,M)=>{let k=be.get(M);x.forEach(te=>Qr(te,k))}),D.forEach(x=>{this.processLeaveNode(x)})});let Ct=[],Be=[];for(let x=this._namespaceList.length-1;x>=0;x--)this._namespaceList[x].drainQueuedTransitions(e).forEach(k=>{let te=k.player,He=k.element;if(Ct.push(te),this.collectedEnterElements.length){let Ze=He[Jt];if(Ze&&Ze.setForMove){if(Ze.previousTriggersValues&&Ze.previousTriggersValues.has(k.triggerName)){let vi=Ze.previousTriggersValues.get(k.triggerName),It=this.statesByElement.get(k.element);if(It&&It.has(k.triggerName)){let Ps=It.get(k.triggerName);Ps.value=vi,It.set(k.triggerName,Ps)}}te.destroy();return}}let rn=!u||!this.driver.containsElement(u,He),bt=be.get(He),Wn=v.get(He),xe=this._buildInstruction(k,i,Wn,bt,rn);if(xe.errors&&xe.errors.length){Be.push(xe);return}if(rn){te.onStart(()=>pi(He,xe.fromStyles)),te.onDestroy(()=>Xt(He,xe.toStyles)),r.push(te);return}if(k.isFallbackTransition){te.onStart(()=>pi(He,xe.fromStyles)),te.onDestroy(()=>Xt(He,xe.toStyles)),r.push(te);return}let Rp=[];xe.timelines.forEach(Ze=>{Ze.stretchStartingKeyframe=!0,this.disabledNodes.has(Ze.element)||Rp.push(Ze)}),xe.timelines=Rp,i.append(He,xe.timelines);let BE={instruction:xe,player:te,element:He};s.push(BE),xe.queriedElements.forEach(Ze=>vt(a,Ze,[]).push(te)),xe.preStyleProps.forEach((Ze,vi)=>{if(Ze.size){let It=c.get(vi);It||c.set(vi,It=new Set),Ze.forEach((Ps,Wl)=>It.add(Wl))}}),xe.postStyleProps.forEach((Ze,vi)=>{let It=l.get(vi);It||l.set(vi,It=new Set),Ze.forEach((Ps,Wl)=>It.add(Wl))})});if(Be.length){let x=[];Be.forEach(M=>{x.push(D_(M.triggerName,M.errors))}),Ct.forEach(M=>M.destroy()),this.reportError(x)}let Fe=new Map,Ne=new Map;s.forEach(x=>{let M=x.element;i.has(M)&&(Ne.set(M,M),this._beforeAnimationBuild(x.player.namespaceId,x.instruction,Fe))}),r.forEach(x=>{let M=x.element;this._getPreviousPlayers(M,!1,x.namespaceId,x.triggerName,null).forEach(te=>{vt(Fe,M,[]).push(te),te.destroy()})});let ot=D.filter(x=>V_(x,c,l)),xt=new Map;P_(xt,this.driver,ee,l,Yt).forEach(x=>{V_(x,c,l)&&ot.push(x)});let nn=new Map;f.forEach((x,M)=>{P_(nn,this.driver,new Set(x),c,fs)}),ot.forEach(x=>{let M=xt.get(x),k=nn.get(x);xt.set(x,new Map([...M?.entries()??[],...k?.entries()??[]]))});let Gl=[],Np=[],kp={};s.forEach(x=>{let{element:M,player:k,instruction:te}=x;if(i.has(M)){if(d.has(M)){k.onDestroy(()=>Xt(M,te.toStyles)),k.disabled=!0,k.overrideTotalTime(te.totalTime),r.push(k);return}let He=kp;if(Ne.size>1){let bt=M,Wn=[];for(;bt=bt.parentNode;){let xe=Ne.get(bt);if(xe){He=xe;break}Wn.push(bt)}Wn.forEach(xe=>Ne.set(xe,He))}let rn=this._buildAnimation(k.namespaceId,te,Fe,o,nn,xt);if(k.setRealPlayer(rn),He===kp)Gl.push(k);else{let bt=this.playersByElement.get(He);bt&&bt.length&&(k.parentPlayer=zn(bt)),r.push(k)}}else pi(M,te.fromStyles),k.onDestroy(()=>Xt(M,te.toStyles)),Np.push(k),d.has(M)&&r.push(k)}),Np.forEach(x=>{let M=o.get(x.element);if(M&&M.length){let k=zn(M);x.setRealPlayer(k)}}),r.forEach(x=>{x.parentPlayer?x.syncPlayerEvents(x.parentPlayer):x.destroy()});for(let x=0;x<D.length;x++){let M=D[x],k=M[Jt];if(Qr(M,tl),k&&k.hasAnimation)continue;let te=[];if(a.size){let rn=a.get(M);rn&&rn.length&&te.push(...rn);let bt=this.driver.query(M,nl,!0);for(let Wn=0;Wn<bt.length;Wn++){let xe=a.get(bt[Wn]);xe&&xe.length&&te.push(...xe)}}let He=te.filter(rn=>!rn.destroyed);He.length?OT(this,M,He):this.processLeaveNode(M)}return D.length=0,Gl.forEach(x=>{this.players.push(x),x.onDone(()=>{x.destroy();let M=this.players.indexOf(x);this.players.splice(M,1)}),x.play()}),Gl}afterFlush(t){this._flushFns.push(t)}afterFlushAnimationsDone(t){this._whenQuietFns.push(t)}_getPreviousPlayers(t,e,i,r,o){let s=[];if(e){let a=this.playersByQueriedElement.get(t);a&&(s=a)}else{let a=this.playersByElement.get(t);if(a){let c=!o||o==gs;a.forEach(l=>{l.queued||!c&&l.triggerName!=r||s.push(l)})}}return(i||r)&&(s=s.filter(a=>!(i&&i!=a.namespaceId||r&&r!=a.triggerName))),s}_beforeAnimationBuild(t,e,i){let r=e.triggerName,o=e.element,s=e.isRemovalTransition?void 0:t,a=e.isRemovalTransition?void 0:r;for(let c of e.timelines){let l=c.element,d=l!==o,u=vt(i,l,[]);this._getPreviousPlayers(l,d,s,a,e.toState).forEach(f=>{let v=f.getRealPlayer();v.beforeDestroy&&v.beforeDestroy(),f.destroy(),u.push(f)})}pi(o,e.fromStyles)}_buildAnimation(t,e,i,r,o,s){let a=e.triggerName,c=e.element,l=[],d=new Set,u=new Set,g=e.timelines.map(v=>{let E=v.element;d.add(E);let D=E[Jt];if(D&&D.removedBeforeQueried)return new $n(v.duration,v.delay);let I=E!==c,ee=PT((i.get(E)||AT).map(Fe=>Fe.getRealPlayer())).filter(Fe=>{let Ne=Fe;return Ne.element?Ne.element===E:!1}),be=o.get(E),_e=s.get(E),Ct=Nm(this._normalizer,v.keyframes,be,_e),Be=this._buildPlayer(v,Ct,ee);if(v.subTimeline&&r&&u.add(E),I){let Fe=new bs(t,a,E);Fe.setRealPlayer(Be),l.push(Fe)}return Be});l.forEach(v=>{vt(this.playersByQueriedElement,v.element,[]).push(v),v.onDone(()=>kT(this.playersByQueriedElement,v.element,v))}),d.forEach(v=>jt(v,Lm));let f=zn(g);return f.onDestroy(()=>{d.forEach(v=>Qr(v,Lm)),Xt(c,e.toStyles)}),u.forEach(v=>{vt(r,v,[]).push(f)}),f}_buildPlayer(t,e,i){return e.length>0?this.driver.animate(t.element,e,t.duration,t.delay,t.easing,i):new $n(t.duration,t.delay)}},bs=class{namespaceId;triggerName;element;_player=new $n;_containsRealPlayer=!1;_queuedCallbacks=new Map;destroyed=!1;parentPlayer=null;markedForDestroy=!1;disabled=!1;queued=!0;totalTime=0;constructor(t,e,i){this.namespaceId=t,this.triggerName=e,this.element=i}setRealPlayer(t){this._containsRealPlayer||(this._player=t,this._queuedCallbacks.forEach((e,i)=>{e.forEach(r=>Xc(t,i,void 0,r))}),this._queuedCallbacks.clear(),this._containsRealPlayer=!0,this.overrideTotalTime(t.totalTime),this.queued=!1)}getRealPlayer(){return this._player}overrideTotalTime(t){this.totalTime=t}syncPlayerEvents(t){let e=this._player;e.triggerCallback&&t.onStart(()=>e.triggerCallback("start")),t.onDone(()=>this.finish()),t.onDestroy(()=>this.destroy())}_queueEvent(t,e){vt(this._queuedCallbacks,t,[]).push(e)}onDone(t){this.queued&&this._queueEvent("done",t),this._player.onDone(t)}onStart(t){this.queued&&this._queueEvent("start",t),this._player.onStart(t)}onDestroy(t){this.queued&&this._queueEvent("destroy",t),this._player.onDestroy(t)}init(){this._player.init()}hasStarted(){return this.queued?!1:this._player.hasStarted()}play(){!this.queued&&this._player.play()}pause(){!this.queued&&this._player.pause()}restart(){!this.queued&&this._player.restart()}finish(){this._player.finish()}destroy(){this.destroyed=!0,this._player.destroy()}reset(){!this.queued&&this._player.reset()}setPosition(t){this.queued||this._player.setPosition(t)}getPosition(){return this.queued?0:this._player.getPosition()}triggerCallback(t){let e=this._player;e.triggerCallback&&e.triggerCallback(t)}};function kT(n,t,e){let i=n.get(t);if(i){if(i.length){let r=i.indexOf(e);i.splice(r,1)}i.length==0&&n.delete(t)}return i}function RT(n){return n??null}function al(n){return n&&n.nodeType===1}function FT(n){return n=="start"||n=="done"}function O_(n,t){let e=n.style.display;return n.style.display=t??"none",e}function P_(n,t,e,i,r){let o=[];e.forEach(c=>o.push(O_(c)));let s=[];i.forEach((c,l)=>{let d=new Map;c.forEach(u=>{let g=t.computeStyle(l,u,r);d.set(u,g),(!g||g.length==0)&&(l[Jt]=NT,s.push(l))}),n.set(l,d)});let a=0;return e.forEach(c=>O_(c,o[a++])),s}function L_(n,t){let e=new Map;if(n.forEach(a=>e.set(a,[])),t.length==0)return e;let i=1,r=new Set(t),o=new Map;function s(a){if(!a)return i;let c=o.get(a);if(c)return c;let l=a.parentNode;return e.has(l)?c=l:r.has(l)?c=i:c=s(l),o.set(a,c),c}return t.forEach(a=>{let c=s(a);c!==i&&e.get(c).push(a)}),e}function jt(n,t){n.classList?.add(t)}function Qr(n,t){n.classList?.remove(t)}function OT(n,t,e){zn(e).onDone(()=>n.processLeaveNode(t))}function PT(n){let t=[];return z_(n,t),t}function z_(n,t){for(let e=0;e<n.length;e++){let i=n[e];i instanceof Wr?z_(i.players,t):t.push(i)}}function LT(n,t){let e=Object.keys(n),i=Object.keys(t);if(e.length!=i.length)return!1;for(let r=0;r<e.length;r++){let o=e[r];if(!t.hasOwnProperty(o)||n[o]!==t[o])return!1}return!0}function V_(n,t,e){let i=e.get(n);if(!i)return!1;let r=t.get(n);return r?i.forEach(o=>r.add(o)):t.set(n,i),e.delete(n),!0}var Zr=class{_driver;_normalizer;_transitionEngine;_timelineEngine;_triggerCache={};onRemovalComplete=(t,e)=>{};constructor(t,e,i){this._driver=e,this._normalizer=i,this._transitionEngine=new Jm(t.body,e,i),this._timelineEngine=new Ym(t.body,e,i),this._transitionEngine.onRemovalComplete=(r,o)=>this.onRemovalComplete(r,o)}registerTrigger(t,e,i,r,o){let s=t+"-"+r,a=this._triggerCache[s];if(!a){let c=[],l=[],d=B_(this._driver,o,c,l);if(c.length)throw m_(r,c);a=DT(r,d,this._normalizer),this._triggerCache[s]=a}this._transitionEngine.registerTrigger(e,r,a)}register(t,e){this._transitionEngine.register(t,e)}destroy(t,e){this._transitionEngine.destroy(t,e)}onInsert(t,e,i,r){this._transitionEngine.insertNode(t,e,i,r)}onRemove(t,e,i){this._transitionEngine.removeNode(t,e,i)}disableAnimations(t,e){this._transitionEngine.markElementAsDisabled(t,e)}process(t,e,i,r){if(i.charAt(0)=="@"){let[o,s]=km(i),a=r;this._timelineEngine.command(o,e,s,a)}else this._transitionEngine.trigger(t,e,i,r)}listen(t,e,i,r,o){if(i.charAt(0)=="@"){let[s,a]=km(i);return this._timelineEngine.listen(s,e,a,o)}return this._transitionEngine.listen(t,e,i,r,o)}flush(t=-1){this._transitionEngine.flush(t)}get players(){return[...this._transitionEngine.players,...this._timelineEngine.players]}whenRenderingDone(){return this._transitionEngine.whenRenderingDone()}afterFlushAnimationsDone(t){this._transitionEngine.afterFlushAnimationsDone(t)}};function VT(n,t){let e=null,i=null;return Array.isArray(t)&&t.length?(e=$m(t[0]),t.length>1&&(i=$m(t[t.length-1]))):t instanceof Map&&(e=$m(t)),e||i?new jT(n,e,i):null}var jT=(()=>{class n{_element;_startStyles;_endStyles;static initialStylesByElement=new WeakMap;_state=0;_initialStyles;constructor(e,i,r){this._element=e,this._startStyles=i,this._endStyles=r;let o=n.initialStylesByElement.get(e);o||n.initialStylesByElement.set(e,o=new Map),this._initialStyles=o}start(){this._state<1&&(this._startStyles&&Xt(this._element,this._startStyles,this._initialStyles),this._state=1)}finish(){this.start(),this._state<2&&(Xt(this._element,this._initialStyles),this._endStyles&&(Xt(this._element,this._endStyles),this._endStyles=null),this._state=1)}destroy(){this.finish(),this._state<3&&(n.initialStylesByElement.delete(this._element),this._startStyles&&(pi(this._element,this._startStyles),this._endStyles=null),this._endStyles&&(pi(this._element,this._endStyles),this._endStyles=null),Xt(this._element,this._initialStyles),this._state=3)}}return n})();function $m(n){let t=null;return n.forEach((e,i)=>{BT(i)&&(t=t||new Map,t.set(i,e))}),t}function BT(n){return n==="display"||n==="position"}var pl=class{element;keyframes;options;_specialStyles;_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_duration;_delay;_initialized=!1;_finished=!1;_started=!1;_destroyed=!1;_finalKeyframe;_originalOnDoneFns=[];_originalOnStartFns=[];domPlayer=null;time=0;parentPlayer=null;currentSnapshot=new Map;constructor(t,e,i,r){this.element=t,this.keyframes=e,this.options=i,this._specialStyles=r,this._duration=i.duration,this._delay=i.delay||0,this.time=this._duration+this._delay}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this._buildPlayer()&&this._preparePlayerBeforeStart()}_buildPlayer(){if(this._initialized)return this.domPlayer;this._initialized=!0;let t=this.keyframes,e=this._triggerWebAnimation(this.element,t,this.options);if(!e)return this._onFinish(),null;this.domPlayer=e,this._finalKeyframe=t.length?t[t.length-1]:new Map;let i=()=>this._onFinish();return e.addEventListener("finish",i),this.onDestroy(()=>{e.removeEventListener("finish",i)}),e}_preparePlayerBeforeStart(){this._delay?this._resetDomPlayerState():this.domPlayer?.pause()}_convertKeyframesToObject(t){let e=[];return t.forEach(i=>{e.push(Object.fromEntries(i))}),e}_triggerWebAnimation(t,e,i){let r=this._convertKeyframesToObject(e);try{return t.animate(r,i)}catch(o){return null}}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}play(){let t=this._buildPlayer();t&&(this.hasStarted()||(this._onStartFns.forEach(e=>e()),this._onStartFns=[],this._started=!0,this._specialStyles&&this._specialStyles.start()),t.play())}pause(){this.init(),this.domPlayer?.pause()}finish(){this.init(),this.domPlayer&&(this._specialStyles&&this._specialStyles.finish(),this._onFinish(),this.domPlayer.finish())}reset(){this._resetDomPlayerState(),this._destroyed=!1,this._finished=!1,this._started=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}_resetDomPlayerState(){this.domPlayer?.cancel()}restart(){this.reset(),this.play()}hasStarted(){return this._started}destroy(){this._destroyed||(this._destroyed=!0,this._resetDomPlayerState(),this._onFinish(),this._specialStyles&&this._specialStyles.destroy(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}setPosition(t){this.domPlayer||this.init(),this.domPlayer&&(this.domPlayer.currentTime=t*this.time)}getPosition(){return this.domPlayer?+(this.domPlayer.currentTime??0)/this.time:this._initialized?1:0}get totalTime(){return this._delay+this._duration}beforeDestroy(){let t=new Map;this.hasStarted()&&this._finalKeyframe.forEach((i,r)=>{r!=="offset"&&t.set(r,this._finished?i:rl(this.element,r))}),this.currentSnapshot=t}triggerCallback(t){let e=t==="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},hl=class{validateStyleProperty(t){return!0}validateAnimatableStyleProperty(t){return!0}containsElement(t,e){return Rm(t,e)}getParentElement(t){return el(t)}query(t,e,i){return Fm(t,e,i)}computeStyle(t,e,i){return rl(t,e)}animate(t,e,i,r,o,s=[]){let a=r==0?"both":"forwards",c={duration:i,delay:r,fill:a};o&&(c.easing=o);let l=new Map,d=s.filter(f=>f instanceof pl);S_(i,r)&&d.forEach(f=>{f.currentSnapshot.forEach((v,E)=>l.set(E,v))});let u=x_(e).map(f=>new Map(f));u=M_(t,u,l);let g=VT(t,u);return new pl(t,u,c,g)}};var cl="@",G_="@.disabled",gl=class{namespaceId;delegate;engine;_onDestroy;\u0275type=0;constructor(t,e,i,r){this.namespaceId=t,this.delegate=e,this.engine=i,this._onDestroy=r}get data(){return this.delegate.data}destroyNode(t){this.delegate.destroyNode?.(t)}destroy(){this.engine.destroy(this.namespaceId,this.delegate),this.engine.afterFlushAnimationsDone(()=>{queueMicrotask(()=>{this.delegate.destroy()})}),this._onDestroy?.()}createElement(t,e){return this.delegate.createElement(t,e)}createComment(t){return this.delegate.createComment(t)}createText(t){return this.delegate.createText(t)}appendChild(t,e){this.delegate.appendChild(t,e),this.engine.onInsert(this.namespaceId,e,t,!1)}insertBefore(t,e,i,r=!0){this.delegate.insertBefore(t,e,i),this.engine.onInsert(this.namespaceId,e,t,r)}removeChild(t,e,i,r){if(r){this.delegate.removeChild(t,e,i,r);return}this.parentNode(e)&&this.engine.onRemove(this.namespaceId,e,this.delegate)}selectRootElement(t,e){return this.delegate.selectRootElement(t,e)}parentNode(t){return this.delegate.parentNode(t)}nextSibling(t){return this.delegate.nextSibling(t)}setAttribute(t,e,i,r){this.delegate.setAttribute(t,e,i,r)}removeAttribute(t,e,i){this.delegate.removeAttribute(t,e,i)}addClass(t,e){this.delegate.addClass(t,e)}removeClass(t,e){this.delegate.removeClass(t,e)}setStyle(t,e,i,r){this.delegate.setStyle(t,e,i,r)}removeStyle(t,e,i){this.delegate.removeStyle(t,e,i)}setProperty(t,e,i){e.charAt(0)==cl&&e==G_?this.disableAnimations(t,!!i):this.delegate.setProperty(t,e,i)}setValue(t,e){this.delegate.setValue(t,e)}listen(t,e,i,r){return this.delegate.listen(t,e,i,r)}disableAnimations(t,e){this.engine.disableAnimations(t,e)}},ep=class extends gl{factory;constructor(t,e,i,r,o){super(e,i,r,o),this.factory=t,this.namespaceId=e}setProperty(t,e,i){e.charAt(0)==cl?e.charAt(1)=="."&&e==G_?(i=i===void 0?!0:!!i,this.disableAnimations(t,i)):this.engine.process(this.namespaceId,t,e.slice(1),i):this.delegate.setProperty(t,e,i)}listen(t,e,i,r){if(e.charAt(0)==cl){let o=HT(t),s=e.slice(1),a="";return s.charAt(0)!=cl&&([s,a]=UT(s)),this.engine.listen(this.namespaceId,o,s,a,c=>{let l=c._data||-1;this.factory.scheduleListenerCallback(l,i,c)})}return this.delegate.listen(t,e,i,r)}};function HT(n){switch(n){case"body":return document.body;case"document":return document;case"window":return window;default:return n}}function UT(n){let t=n.indexOf("."),e=n.substring(0,t),i=n.slice(t+1);return[e,i]}var vl=class{delegate;engine;_zone;_currentId=0;_microtaskId=1;_animationCallbacksBuffer=[];_rendererCache=new Map;_cdRecurDepth=0;constructor(t,e,i){this.delegate=t,this.engine=e,this._zone=i,e.onRemovalComplete=(r,o)=>{o?.removeChild(null,r)}}createRenderer(t,e){let r=this.delegate.createRenderer(t,e);if(!t||!e?.data?.animation){let l=this._rendererCache,d=l.get(r);if(!d){let u=()=>l.delete(r);d=new gl("",r,this.engine,u),l.set(r,d)}return d}let o=e.id,s=e.id+"-"+this._currentId;this._currentId++,this.engine.register(s,t);let a=l=>{Array.isArray(l)?l.forEach(a):this.engine.registerTrigger(o,s,t,l.name,l)};return e.data.animation.forEach(a),new ep(this,s,r,this.engine)}begin(){this._cdRecurDepth++,this.delegate.begin&&this.delegate.begin()}_scheduleCountTask(){queueMicrotask(()=>{this._microtaskId++})}scheduleListenerCallback(t,e,i){if(t>=0&&t<this._microtaskId){this._zone.run(()=>e(i));return}let r=this._animationCallbacksBuffer;r.length==0&&queueMicrotask(()=>{this._zone.run(()=>{r.forEach(o=>{let[s,a]=o;s(a)}),this._animationCallbacksBuffer=[]})}),r.push([e,i])}end(){this._cdRecurDepth--,this._cdRecurDepth==0&&this._zone.runOutsideAngular(()=>{this._scheduleCountTask(),this.engine.flush(this._microtaskId)}),this.delegate.end&&this.delegate.end()}whenRenderingDone(){return this.engine.whenRenderingDone()}componentReplaced(t){this.engine.flush(),this.delegate.componentReplaced?.(t)}};var zT=(()=>{class n extends Zr{constructor(e,i,r){super(e,i,r)}ngOnDestroy(){this.flush()}static \u0275fac=function(i){return new(i||n)(T(X),T(tr),T(nr))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})();function GT(){return new ll}function WT(){return new vl(p(ds),p(Zr),p(P))}var W_=[{provide:nr,useFactory:GT},{provide:Zr,useClass:zT},{provide:ct,useFactory:WT}],Zz=[{provide:tr,useClass:tp},{provide:Mr,useValue:"NoopAnimations"},...W_],qT=[{provide:tr,useFactory:()=>new hl},{provide:Mr,useFactory:()=>"BrowserAnimations"},...W_];function q_(){return ui("NgEagerAnimations"),[...qT]}var ip="Service workers are disabled or not supported by this browser",Yr=class{serviceWorker;worker;registration;events;constructor(t,e){if(this.serviceWorker=t,!t)this.worker=this.events=this.registration=new Q(i=>i.error(new _(5601,!1)));else{let i=null,r=new Y;this.worker=new Q(l=>(i!==null&&l.next(i),r.subscribe(d=>l.next(d))));let o=()=>{let{controller:l}=t;l!==null&&(i=l,r.next(i))};t.addEventListener("controllerchange",o),o(),this.registration=this.worker.pipe(Si(()=>t.getRegistration().then(l=>{if(!l)throw new _(5601,!1);return l})));let s=new Y;this.events=s.asObservable();let a=l=>{let{data:d}=l;d?.type&&s.next(d)};t.addEventListener("message",a),e?.get(Qt,null,{optional:!0})?.onDestroy(()=>{t.removeEventListener("controllerchange",o),t.removeEventListener("message",a)})}}postMessage(t,e){return new Promise(i=>{this.worker.pipe(Cn(1)).subscribe(r=>{r.postMessage(C({action:t},e)),i()})})}postMessageWithOperation(t,e,i){let r=this.waitForOperationCompleted(i),o=this.postMessage(t,e);return Promise.all([o,r]).then(([,s])=>s)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(t){let e;return typeof t=="string"?e=i=>i.type===t:e=i=>t.includes(i.type),this.events.pipe(Mt(e))}nextEventOfType(t){return this.eventsOfType(t).pipe(Cn(1))}waitForOperationCompleted(t){return new Promise((e,i)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(Mt(r=>r.nonce===t),Cn(1),ie(r=>{if(r.result!==void 0)return r.result;throw new Error(r.error)})).subscribe({next:e,error:i})})}get isEnabled(){return!!this.serviceWorker}},Q_=(()=>{class n{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new Y;constructor(e){if(this.sw=e,!e.isEnabled){this.messages=wn,this.notificationClicks=wn,this.notificationCloses=wn,this.pushSubscriptionChanges=wn,this.subscription=wn;return}this.messages=this.sw.eventsOfType("PUSH").pipe(ie(r=>r.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(ie(r=>r.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(ie(r=>r.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(ie(r=>r.data)),this.pushManager=this.sw.registration.pipe(ie(r=>r.pushManager));let i=this.pushManager.pipe(Si(r=>r.getSubscription()));this.subscription=new Q(r=>{let o=i.subscribe(r),s=this.subscriptionChanges.subscribe(r);return()=>{o.unsubscribe(),s.unsubscribe()}})}requestSubscription(e){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(ip));let i={userVisibleOnly:!0},r=this.decodeBase64(e.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),o=new Uint8Array(new ArrayBuffer(r.length));for(let s=0;s<r.length;s++)o[s]=r.charCodeAt(s);return i.applicationServerKey=o,new Promise((s,a)=>{this.pushManager.pipe(Si(c=>c.subscribe(i)),Cn(1)).subscribe({next:c=>{this.subscriptionChanges.next(c),s(c)},error:a})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(ip));let e=i=>{if(i===null)throw new _(5602,!1);return i.unsubscribe().then(r=>{if(!r)throw new _(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((i,r)=>{this.subscription.pipe(Cn(1),Si(e)).subscribe({next:i,error:r})})}decodeBase64(e){return atob(e)}static \u0275fac=function(i){return new(i||n)(T(Yr))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),Z_=(()=>{class n{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(e){if(this.sw=e,!e.isEnabled){this.versionUpdates=wn,this.unrecoverable=wn;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(ip));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let e=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:e},e).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new _(5601,!1));let e=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:e},e)}static \u0275fac=function(i){return new(i||n)(T(Yr))};static \u0275prov=L({token:n,factory:n.\u0275fac})}return n})(),Y_=new w("");function KT(){let n=p(_s);if(!("serviceWorker"in navigator&&n.enabled!==!1))return;let t=p(Y_),e=p(P),i=p(Qt);e.runOutsideAngular(()=>{let r=navigator.serviceWorker,o=()=>r.controller?.postMessage({action:"INITIALIZE"});r.addEventListener("controllerchange",o),i.onDestroy(()=>{r.removeEventListener("controllerchange",o)})}),e.runOutsideAngular(()=>{let r,{registrationStrategy:o}=n;if(typeof o=="function")r=new Promise(s=>o().subscribe(()=>s()));else{let[s,...a]=(o||"registerWhenStable:30000").split(":");switch(s){case"registerImmediately":r=Promise.resolve();break;case"registerWithDelay":r=K_(+a[0]||0);break;case"registerWhenStable":r=Promise.race([i.whenStable(),K_(+a[0])]);break;default:throw new _(5600,!1)}}r.then(()=>{i.destroyed||navigator.serviceWorker.register(t,{scope:n.scope,updateViaCache:n.updateViaCache,type:n.type}).catch(s=>console.error(Jn(5604,!1)))})})}function K_(n){return new Promise(t=>setTimeout(t,n))}function QT(){let n=p(_s),t=p(Se),e=!0;return new Yr(e&&n.enabled!==!1?navigator.serviceWorker:void 0,t)}var _s=class{enabled;updateViaCache;type;scope;registrationStrategy};function ZT(n,t={}){return ni([Q_,Z_,{provide:Y_,useValue:n},{provide:_s,useValue:t},{provide:Yr,useFactory:QT},Hf(KT)])}var X_=(()=>{class n{static register(e,i={}){return{ngModule:n,providers:[ZT(e,i)]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({providers:[Q_,Z_]})}return n})();var a0=(()=>{class n{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||n)(H(dt),H(re))};static \u0275dir=z({type:n})}return n})(),YT=(()=>{class n extends a0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qi(n)))(r||n)}})();static \u0275dir=z({type:n,features:[et]})}return n})(),Ml=new w("");var XT={provide:Ml,useExisting:_t(()=>Tl),multi:!0};function JT(){let n=Lt()?Lt().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var eA=new w(""),Tl=(()=>{class n extends a0{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!JT())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||n)(H(dt),H(re),H(eA,8))};static \u0275dir=z({type:n,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&q("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[nt([XT]),et]})}return n})();function ap(n){return n==null||cp(n)===0}function cp(n){return n==null?null:Array.isArray(n)||typeof n=="string"?n.length:n instanceof Set?n.size:null}var to=new w(""),lp=new w(""),tA=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Is=class{static min(t){return nA(t)}static max(t){return iA(t)}static required(t){return c0(t)}static requiredTrue(t){return rA(t)}static email(t){return oA(t)}static minLength(t){return sA(t)}static maxLength(t){return aA(t)}static pattern(t){return cA(t)}static nullValidator(t){return bl()}static compose(t){return p0(t)}static composeAsync(t){return h0(t)}};function nA(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e<n?{min:{min:n,actual:t.value}}:null}}function iA(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e>n?{max:{max:n,actual:t.value}}:null}}function c0(n){return ap(n.value)?{required:!0}:null}function rA(n){return n.value===!0?null:{required:!0}}function oA(n){return ap(n.value)||tA.test(n.value)?null:{email:!0}}function sA(n){return t=>{let e=t.value?.length??cp(t.value);return e===null||e===0?null:e<n?{minlength:{requiredLength:n,actualLength:e}}:null}}function aA(n){return t=>{let e=t.value?.length??cp(t.value);return e!==null&&e>n?{maxlength:{requiredLength:n,actualLength:e}}:null}}function cA(n){if(!n)return bl;let t,e;return typeof n=="string"?(e="",n.charAt(0)!=="^"&&(e+="^"),e+=n,n.charAt(n.length-1)!=="$"&&(e+="$"),t=new RegExp(e)):(e=n.toString(),t=n),i=>{if(ap(i.value))return null;let r=i.value;return t.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function bl(n){return null}function l0(n){return n!=null}function d0(n){return Vr(n)?En(n):n}function u0(n){let t={};return n.forEach(e=>{t=e!=null?C(C({},t),e):t}),Object.keys(t).length===0?null:t}function f0(n,t){return t.map(e=>e(n))}function lA(n){return!n.validate}function m0(n){return n.map(t=>lA(t)?t:e=>t.validate(e))}function p0(n){if(!n)return null;let t=n.filter(l0);return t.length==0?null:function(e){return u0(f0(e,t))}}function dp(n){return n!=null?p0(m0(n)):null}function h0(n){if(!n)return null;let t=n.filter(l0);return t.length==0?null:function(e){let i=f0(e,t).map(d0);return uo(i).pipe(ie(u0))}}function up(n){return n!=null?h0(m0(n)):null}function J_(n,t){return n===null?[t]:Array.isArray(n)?[...n,t]:[n,t]}function g0(n){return n._rawValidators}function v0(n){return n._rawAsyncValidators}function rp(n){return n?Array.isArray(n)?n:[n]:[]}function _l(n,t){return Array.isArray(n)?n.includes(t):n===t}function e0(n,t){let e=rp(t);return rp(n).forEach(r=>{_l(e,r)||e.push(r)}),e}function t0(n,t){return rp(t).filter(e=>!_l(n,e))}var El=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=dp(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=up(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,e){return this.control?this.control.hasError(t,e):!1}getError(t,e){return this.control?this.control.getError(t,e):null}},ir=class extends El{name;get formDirective(){return null}get path(){return null}};var Es="VALID",yl="INVALID",Xr="PENDING",ws="DISABLED",hi=class{},wl=class extends hi{value;source;constructor(t,e){super(),this.value=t,this.source=e}},Cs=class extends hi{pristine;source;constructor(t,e){super(),this.pristine=t,this.source=e}},xs=class extends hi{touched;source;constructor(t,e){super(),this.touched=t,this.source=e}},Jr=class extends hi{status;source;constructor(t,e){super(),this.status=t,this.source=e}},Dl=class extends hi{source;constructor(t){super(),this.source=t}},eo=class extends hi{source;constructor(t){super(),this.source=t}};function y0(n){return(Al(n)?n.validators:n)||null}function dA(n){return Array.isArray(n)?dp(n):n||null}function b0(n,t){return(Al(t)?t.asyncValidators:n)||null}function uA(n){return Array.isArray(n)?up(n):n||null}function Al(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function fA(n,t,e){let i=n.controls;if(!(t?Object.keys(i):i).length)throw new _(1e3,"");if(!_0(i,e))throw new _(1001,"")}function mA(n,t,e){n._forEachChild((i,r)=>{if(e[r]===void 0)throw new _(-1002,"")})}var Cl=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=ge(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,e){this._assignValidators(t),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return ht(this.statusReactive)}set status(t){ht(()=>this.statusReactive.set(t))}_status=Re(()=>this.statusReactive());statusReactive=ge(void 0);get valid(){return this.status===Es}get invalid(){return this.status===yl}get pending(){return this.status===Xr}get disabled(){return this.status===ws}get enabled(){return this.status!==ws}errors;get pristine(){return ht(this.pristineReactive)}set pristine(t){ht(()=>this.pristineReactive.set(t))}_pristine=Re(()=>this.pristineReactive());pristineReactive=ge(!0);get dirty(){return!this.pristine}get touched(){return ht(this.touchedReactive)}set touched(t){ht(()=>this.touchedReactive.set(t))}_touched=Re(()=>this.touchedReactive());touchedReactive=ge(!1);get untouched(){return!this.touched}_events=new Y;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(e0(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(e0(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(t0(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(t0(t,this._rawAsyncValidators))}hasValidator(t){return _l(this._rawValidators,t)}hasAsyncValidator(t){return _l(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let e=this.touched===!1;this.touched=!0;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(G(C({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new xs(!0,i))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(t))}markAsUntouched(t={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:i})}),t.onlySelf||this._parent?._updateTouched(t,i),e&&t.emitEvent!==!1&&this._events.next(new xs(!1,i))}markAsDirty(t={}){let e=this.pristine===!0;this.pristine=!1;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(G(C({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new Cs(!1,i))}markAsPristine(t={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,i),e&&t.emitEvent!==!1&&this._events.next(new Cs(!0,i))}markAsPending(t={}){this.status=Xr;let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Jr(this.status,e)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(G(C({},t),{sourceControl:e}))}disable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=ws,this.errors=null,this._forEachChild(r=>{r.disable(G(C({},t),{onlySelf:!0}))}),this._updateValue();let i=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new wl(this.value,i)),this._events.next(new Jr(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(G(C({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=Es,this._forEachChild(i=>{i.enable(G(C({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(G(C({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(t,e){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Es||this.status===Xr)&&this._runAsyncValidator(i,t.emitEvent)}let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new wl(this.value,e)),this._events.next(new Jr(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(G(C({},t),{sourceControl:e}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ws:Es}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,e){if(this.asyncValidator){this.status=Xr,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:t!==!1};let i=d0(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,e={}){this.errors=t,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(t){let e=t;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(t,e){let i=e?this.get(e):this;return i?.errors?i.errors[t]:null}hasError(t,e){return!!this.getError(t,e)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,e,i){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||i)&&this._events.next(new Jr(this.status,e)),this._parent&&this._parent._updateControlsErrors(t,e,i)}_initObservables(){this.valueChanges=new ye,this.statusChanges=new ye}_calculateStatus(){return this._allControlsDisabled()?ws:this.errors?yl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Xr)?Xr:this._anyControlsHaveStatus(yl)?yl:Es}_anyControlsHaveStatus(t){return this._anyControls(e=>e.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,t.onlySelf||this._parent?._updatePristine(t,e),r&&this._events.next(new Cs(this.pristine,e))}_updateTouched(t={},e){this.touched=this._anyControlsTouched(),this._events.next(new xs(this.touched,e)),t.onlySelf||this._parent?._updateTouched(t,e)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Al(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=dA(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=uA(this._rawAsyncValidators)}_updateHasRequiredValidator(){ht(()=>this._hasRequired.set(this.hasValidator(Is.required)))}};function _0(n,t){return Object.hasOwn(n,t)}function pA(n){return n.tagName==="INPUT"||n.tagName==="SELECT"||n.tagName==="TEXTAREA"}function hA(n,t,e,i){switch(e){case"name":n.setAttribute(t,e,i);break;case"disabled":case"readonly":case"required":i?n.setAttribute(t,e,""):n.removeAttribute(t,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?n.setAttribute(t,e,i.toString()):n.removeAttribute(t,e);break}}var op=class{kind;context;control;message;constructor({kind:t,context:e,control:i}){this.kind=t,this.context=e,this.control=i}};var gA=(()=>{class n{_validator=bl;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):bl,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,features:[Ft]})}return n})();var vA={provide:to,useExisting:_t(()=>E0),multi:!0};var E0=(()=>{class n extends gA{required;inputName="required";normalizeInput=pe;createValidator=e=>c0;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qi(n)))(r||n)}})();static \u0275dir=z({type:n,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&Me("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[nt([vA]),et]})}return n})();var yA=new w(""),Nl=new w("",{factory:()=>fp}),fp="always";function bA(n,t){return[...t.path,n]}function n0(n,t,e=fp){mp(n,t),t.valueAccessor.writeValue(n.value),(n.disabled||e==="always")&&t.valueAccessor.setDisabledState?.(n.disabled),EA(n,t),DA(n,t),wA(n,t),_A(n,t)}function i0(n,t,e=!0){let i=()=>{};t?.valueAccessor?.registerOnChange(i),t?.valueAccessor?.registerOnTouched(i),Il(n,t),n&&(t._invokeOnDestroyCallbacks(),n._registerOnCollectionChange(()=>{}))}function xl(n,t){n.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(t)})}function _A(n,t){if(t.valueAccessor.setDisabledState){let e=i=>{t.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(e),t._registerOnDestroy(()=>{n._unregisterOnDisabledChange(e)})}}function mp(n,t){let e=g0(n);t.validator!==null?n.setValidators(J_(e,t.validator)):typeof e=="function"&&n.setValidators([e]);let i=v0(n);t.asyncValidator!==null?n.setAsyncValidators(J_(i,t.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();xl(t._rawValidators,r),xl(t._rawAsyncValidators,r)}function Il(n,t){let e=!1;if(n!==null){if(t.validator!==null){let r=g0(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.validator);o.length!==r.length&&(e=!0,n.setValidators(o))}}if(t.asyncValidator!==null){let r=v0(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.asyncValidator);o.length!==r.length&&(e=!0,n.setAsyncValidators(o))}}}let i=()=>{};return xl(t._rawValidators,i),xl(t._rawAsyncValidators,i),e}function EA(n,t){t.valueAccessor.registerOnChange(e=>{n._pendingValue=e,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&w0(n,t)})}function wA(n,t){t.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&w0(n,t),n.updateOn!=="submit"&&n.markAsTouched()})}function w0(n,t){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function DA(n,t){let e=(i,r)=>{t.valueAccessor.writeValue(i),r&&t.viewToModelUpdate(i)};n.registerOnChange(e),t._registerOnDestroy(()=>{n._unregisterOnChange(e)})}function D0(n,t){n==null,mp(n,t)}function CA(n,t){return Il(n,t)}function xA(n,t){if(!n.hasOwnProperty("model"))return!1;let e=n.model;return e.isFirstChange()?!0:!Object.is(t,e.currentValue)}function IA(n){return Object.getPrototypeOf(n.constructor)===YT}function C0(n,t){n._syncPendingControls(),t.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function SA(n,t){if(!t)return null;Array.isArray(t);let e,i,r;return t.forEach(o=>{o.constructor===Tl?e=o:IA(o)?i=o:r=o}),r||i||e||null}function MA(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}var TA={provide:yA,useFactory:()=>{let n=p(gi,{self:!0});return{setParseErrors:t=>{n.setParseErrorSource(t)},set onReset(t){n.onReset=t}}}},gi=class extends El{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(t){this.userOnReset=t,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof eo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=SA(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(t,e,i){super(),this.injector=t,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(fn)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let t=this.injector?.get(vn);if(!this.control||!t)return;let e=t.markForCheck.bind(t);this.subscription=new ke,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof eo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(t){!t.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!t.customControl||(this.isCustomControlBased=!0,t.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),t.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=pA(t.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof E0))}ngControlUpdate(t,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,t.setCustomControlModelInput(i.value)),this.bindControlProperty(t,r,"touched",i.touched),this.bindControlProperty(t,r,"dirty",i.dirty),this.bindControlProperty(t,r,"valid",i.valid),this.bindControlProperty(t,r,"invalid",i.invalid),this.bindControlProperty(t,r,"pending",i.pending),this.bindControlProperty(t,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(t,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);t.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(t,e,i,r){if(e[i]===r)return;e[i]=r;let o=t.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&hA(this.renderer,t.nativeElement,i,r)}_convertErrors(t){if(t===null)return[];let e=this.control;return Object.entries(t).map(([i,r])=>new op({context:r,kind:i,control:e}))}setParseErrorSource(t){if(t===void 0)return;let e=null,i=Re(()=>{let r=t();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),zi(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(t){this.parseErrorsValidator&&(t?.removeValidators(this.parseErrorsValidator),t?.updateValueAndValidity({emitEvent:!1}))}},sp=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var x0=(()=>{class n extends sp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||n)(H(gi,2))};static \u0275dir=z({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&K("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[et]})}return n})();var Sl=class extends Cl{constructor(t,e,i){super(y0(e),b0(i,e)),this.controls=t,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,e){let i=this._find(t);return i||(this.controls[t]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(t,e,i={}){this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(t,e={}){let i=this._find(t);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(t,e,i={}){let r=this._find(t);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[t],e&&this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(t){return this._find(t)?.enabled===!0}setValue(t,e={}){ht(()=>{mA(this,!0,t),Object.keys(t).forEach(i=>{fA(this,!0,i),this.controls[i].setValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(t,e={}){t!=null&&(Object.keys(t).forEach(i=>{let r=this._find(i);r&&r.patchValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(t={},e={}){this._forEachChild((i,r)=>{i.reset(t?t[r]:null,G(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new eo(this))}getRawValue(){return this._reduceChildren({},(t,e,i)=>(t[i]=e.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&t(i,e)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&t(i))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(t,e){let i=t;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return _0(this.controls,t)?this.controls[t]:null}};var AA={provide:ir,useExisting:_t(()=>pp)},Ds=Promise.resolve(),pp=(()=>{class n extends ir{callSetDisabledState;get submitted(){return ht(this.submittedReactive)}_submitted=Re(()=>this.submittedReactive());submittedReactive=ge(!1);_directives=new Set;form;ngSubmit=new ye;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Sl({},dp(e),up(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ds.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ds.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ds.then(()=>{let i=this._findContainer(e.path),r=new Sl({});D0(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ds.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Ds.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),C0(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Dl(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||n)(H(to,10),H(lp,10),H(Nl,8))};static \u0275dir=z({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&q("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[nt([AA]),et]})}return n})();function r0(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function o0(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var I0=class extends Cl{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,e,i){super(y0(e),b0(i,e)),this._applyFormState(t),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Al(e)&&(e.nonNullable||e.initialValueIsDefault)&&(o0(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,e={}){ht(()=>{this.value=this._pendingValue=t,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(t,e={}){this.setValue(t,e)}reset(t=this.defaultValue,e={}){this._applyFormState(t),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new eo(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){r0(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){r0(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){o0(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var NA=n=>n instanceof I0;var kA={provide:gi,useExisting:_t(()=>hp)},s0=Promise.resolve(),hp=(()=>{class n extends gi{_changeDetectorRef;callSetDisabledState;control=new I0;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new ye;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),xA(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,n0(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,n0(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){s0.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&pe(i);s0.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?bA(e,this._parent):[e]}static \u0275fac=function(i){return new(i||n)(H(ir,9),H(to,10),H(lp,10),H(Ml,10),H(vn,8),H(Nl,8),H(Se,8),H(dt,8))};static \u0275dir=z({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[nt([kA,TA]),et,Ft,$f(null)]})}return n})();var RA=(()=>{class n extends ir{callSetDisabledState;get submitted(){return ht(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Re(()=>this._submittedReactive());_submittedReactive=ge(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Il(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){i0(e.control||null,e,!1),MA(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,C0(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Dl(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(i0(i||null,e),NA(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);D0(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&CA(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){mp(this.form,this),this._oldForm&&Il(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||n)(H(to,10),H(lp,10),H(Nl,8))};static \u0275dir=z({type:n,features:[et,Ft]})}return n})();var FA={provide:ir,useExisting:_t(()=>gp)},gp=(()=>{class n extends RA{form=null;ngSubmit=new ye;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qi(n)))(r||n)}})();static \u0275dir=z({type:n,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&q("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[nt([FA]),et]})}return n})();var OA=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({})}return n})();var S0=(()=>{class n{static withConfig(e){return{ngModule:n,providers:[{provide:Nl,useValue:e.callSetDisabledState??fp}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[OA]})}return n})();function Ss(n){return n.buttons===0||n.detail===0}function Ms(n){let t=n.touches&&n.touches[0]||n.changedTouches&&n.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var vp;function M0(){if(vp==null){let n=typeof document<"u"?document.head:null;vp=!!(n&&(n.createShadowRoot||n.attachShadow))}return vp}function yp(n){if(M0()){let t=n.getRootNode?n.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function en(n){return n.composedPath?n.composedPath()[0]:n.target}var bp;try{bp=typeof Intl<"u"&&Intl.v8BreakIterator}catch(n){bp=!1}var rt=(()=>{class n{_platformId=p(Ui);isBrowser=this._platformId?Ub(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||bp)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var Ts;function T0(){if(Ts==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Ts=!0}))}finally{Ts=Ts||!1}return Ts}function no(n){return T0()?n:!!n.capture}function tn(n){return n instanceof re?n.nativeElement:n}var A0=new w("cdk-input-modality-detector-options"),N0={ignoreKeys:[18,17,224,91,16]},k0=650,_p={passive:!0,capture:!0},R0=(()=>{class n{_platform=p(rt);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Ci(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=en(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<k0||(this._modality.next(Ss(e)?"keyboard":"mouse"),this._mostRecentTarget=en(e))};_onTouchstart=e=>{if(Ms(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=en(e)};constructor(){let e=p(P),i=p(X),r=p(A0,{optional:!0});if(this._options=C(C({},N0),r),this.modalityDetected=this._modality.pipe(yd(1)),this.modalityChanged=this.modalityDetected.pipe(pd()),this._platform.isBrowser){let o=p(ct).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,_p),o.listen(i,"mousedown",this._onMousedown,_p),o.listen(i,"touchstart",this._onTouchstart,_p)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})(),As=(function(n){return n[n.IMMEDIATE=0]="IMMEDIATE",n[n.EVENTUAL=1]="EVENTUAL",n})(As||{}),F0=new w("cdk-focus-monitor-default-options"),kl=no({passive:!0,capture:!0}),rr=(()=>{class n{_ngZone=p(P);_platform=p(rt);_inputModalityDetector=p(R0);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=p(X);_stopInputModalityDetector=new Y;constructor(){let e=p(F0,{optional:!0});this._detectionMode=e?.detectionMode||As.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=en(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=tn(e);if(!this._platform.isBrowser||r.nodeType!==1)return st();let o=yp(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new Y,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=tn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=tn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===As.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===As.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?k0:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=en(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,kl),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,kl)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Mi(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,kl),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,kl),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var Rl=new WeakMap,Bt=(()=>{class n{_appRef;_injector=p(Se);_environmentInjector=p(We);load(e){let i=this._appRef=this._appRef||this._injector.get(Qt),r=Rl.get(i);r||(r={loaders:new Set,refs:[]},Rl.set(i,r),i.onDestroy(()=>{Rl.get(i)?.refs.forEach(o=>o.destroy()),Rl.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(ab(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var O0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2})}return n})(),Fl;function LA(){if(Fl===void 0&&(Fl=null,typeof window<"u")){let n=window;n.trustedTypes!==void 0&&(Fl=n.trustedTypes.createPolicy("angular#components",{createHTML:t=>t}))}return Fl}function io(n){return LA()?.createHTML(n)||n}var P0=new Set,or,Ep=(()=>{class n{_platform=p(rt);_nonce=p($i,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):jA}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&VA(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();function VA(n,t){if(!P0.has(n))try{or||(or=document.createElement("style"),t&&or.setAttribute("nonce",t),or.setAttribute("type","text/css"),document.head.appendChild(or)),or.sheet&&(or.sheet.insertRule(`@media ${n} {body{ }}`,0),P0.add(n))}catch(e){console.error(e)}}function jA(n){return{matches:n==="all"||n==="",media:n,addListener:()=>{},removeListener:()=>{}}}var BA=(()=>{class n{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var L0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({providers:[BA]})}return n})();var V0=new Map,Ht=class n{_appId=p(Hi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,e=!1){this._appId!=="ng"&&(t+=this._appId);let i=V0.get(t);return i===void 0?i=0:i++,V0.set(t,i),`${t}${e?n._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})};var ro,j0=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function wp(){if(ro)return ro;if(typeof document!="object"||!document)return ro=new Set(j0),ro;let n=document.createElement("input");return ro=new Set(j0.filter(t=>(n.setAttribute("type",t),n.type===t))),ro}var HA=new w("MATERIAL_ANIMATIONS"),B0=null;function UA(){return p(HA,{optional:!0})?.animationsDisabled||p(Mr,{optional:!0})==="NoopAnimations"?"di-disabled":(B0??=p(Ep).matchMedia("(prefers-reduced-motion)").matches,B0?"reduced-motion":"enabled")}function Ut(){return UA()!=="enabled"}function oo(n){return n!=null&&`${n}`!="false"}var $t=(function(n){return n[n.FADING_IN=0]="FADING_IN",n[n.VISIBLE=1]="VISIBLE",n[n.FADING_OUT=2]="FADING_OUT",n[n.HIDDEN=3]="HIDDEN",n})($t||{}),Dp=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=$t.HIDDEN;constructor(t,e,i,r=!1){this._renderer=t,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},H0=no({passive:!0,capture:!0}),Cp=class{_events=new Map;addHandler(t,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),t.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,H0)})}removeHandler(t,e,i){let r=this._events.get(t);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,H0)))}_delegateEventHandler=t=>{let e=en(t);e&&this._events.get(t.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(t))})}},Ns={enterDuration:225,exitDuration:150},zA=800,U0=no({passive:!0,capture:!0}),$0=["mousedown","touchstart"],z0=["mouseup","mouseleave","touchend","touchcancel"],GA=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2})}return n})(),ks=class n{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Cp;constructor(t,e,i,r,o){this._target=t,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=tn(i)),o&&o.get(Bt).load(GA)}fadeInRipple(t,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},Ns),i.animation);i.centered&&(t=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||WA(t,e,r),a=t-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),g=u.transitionProperty,f=u.transitionDuration,v=g==="none"||f==="0s"||f==="0s, 0s"||r.width===0&&r.height===0,E=new Dp(this,d,i,v);d.style.transform="scale3d(1, 1, 1)",E.state=$t.FADING_IN,i.persistent||(this._mostRecentTransientRipple=E);let D=null;return!v&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let I=()=>{D&&(D.fallbackTimer=null),clearTimeout(be),this._finishRippleTransition(E)},ee=()=>this._destroyRipple(E),be=setTimeout(ee,l+100);d.addEventListener("transitionend",I),d.addEventListener("transitioncancel",ee),D={onTransitionEnd:I,onTransitionCancel:ee,fallbackTimer:be}}),this._activeRipples.set(E,D),(v||!l)&&this._finishRippleTransition(E),E}fadeOutRipple(t){if(t.state===$t.FADING_OUT||t.state===$t.HIDDEN)return;let e=t.element,i=C(C({},Ns),t.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",t.state=$t.FADING_OUT,(t._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let e=tn(t);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,$0.forEach(i=>{n._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{z0.forEach(e=>{this._triggerElement.addEventListener(e,this,U0)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===$t.FADING_IN?this._startFadeOutTransition(t):t.state===$t.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let e=t===this._mostRecentTransientRipple,{persistent:i}=t.config;t.state=$t.VISIBLE,!i&&(!e||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let e=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=$t.HIDDEN,e!==null&&(t.element.removeEventListener("transitionend",e.onTransitionEnd),t.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),t.element.remove()}_onMousedown(t){let e=Ss(t),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+zA;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!Ms(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=t.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let e=t.state===$t.VISIBLE||t.config.terminateOnPointerUp&&t.state===$t.FADING_IN;!t.config.persistent&&e&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&($0.forEach(e=>n._eventManager.removeHandler(e,t,this)),this._pointerUpEventsRegistered&&(z0.forEach(e=>t.removeEventListener(e,this,U0)),this._pointerUpEventsRegistered=!1))}};function WA(n,t,e){let i=Math.max(Math.abs(n-e.left),Math.abs(n-e.right)),r=Math.max(Math.abs(t-e.top),Math.abs(t-e.bottom));return Math.sqrt(i*i+r*r)}var Rs=new w("mat-ripple-global-options"),G0=(()=>{class n{_elementRef=p(re);_animationsDisabled=Ut();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=p(P),i=p(rt),r=p(Rs,{optional:!0}),o=p(Se);this._globalOptions=r||{},this._rippleRenderer=new ks(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&K("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return n})();var qA={capture:!0},KA=["focus","mousedown","mouseenter","touchstart"],xp="mat-ripple-loader-uninitialized",Ip="mat-ripple-loader-class-name",W0="mat-ripple-loader-centered",Ol="mat-ripple-loader-disabled",Pl=(()=>{class n{_document=p(X);_animationsDisabled=Ut();_globalRippleOptions=p(Rs,{optional:!0});_platform=p(rt);_ngZone=p(P);_injector=p(Se);_eventCleanups;_hosts=new Map;constructor(){let e=p(ct).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>KA.map(i=>e.listen(this._document,i,this._onInteraction,qA)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(xp,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Ip))&&e.setAttribute(Ip,i.className||""),i.centered&&e.setAttribute(W0,""),i.disabled&&e.setAttribute(Ol,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Ol,""):e.removeAttribute(Ol)}_onInteraction=e=>{let i=en(e);if(i instanceof HTMLElement){let r=i.closest(`[${xp}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Ip)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ns.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ns.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Ol),rippleConfig:{centered:e.hasAttribute(W0),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new ks(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(xp)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var sr=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2})}return n})();var QA=new w("MAT_BUTTON_CONFIG");function q0(n){return n==null?void 0:Hr(n)}var K0=(()=>{class n{_elementRef=p(re);_ngZone=p(P);_animationsDisabled=Ut();_config=p(QA,{optional:!0});_focusMonitor=p(rr);_cleanupClick;_renderer=p(dt);_rippleLoader=p(Pl);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=nb(!1,{transform:pe});constructor(){p(Bt).load(sr);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(Me("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Dt(r.color?"mat-"+r.color:""),K("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",pe],disabled:[2,"disabled","disabled",pe],ariaDisabled:[2,"aria-disabled","ariaDisabled",pe],disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe],tabIndex:[2,"tabIndex","tabIndex",q0],_tabindex:[2,"tabindex","_tabindex",q0],showProgress:[1,"showProgress"]}})}return n})();var ZA=new w("cdk-dir-doc",{providedIn:"root",factory:()=>p(X)}),YA=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function Q0(n){let t=n?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?YA.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var Ll=(()=>{class n{get value(){return this.valueSignal()}valueSignal=ge("ltr");change=new ye;constructor(){let e=p(ZA,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(Q0(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var ze=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({})}return n})();var Vl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[ze]})}return n})();var XA=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],JA=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function eN(n,t){n&1&&(Ot(0,"div",2),ce(1,3),Pt())}var Z0=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Y0=(()=>{class n extends K0{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=tN(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Z0.get(this._appearance):null,o=Z0.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[et],ngContentSelectors:JA,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Qe(XA),hn(0,"span",0),ce(1),Ot(2,"span",1),ce(3,1),Pt(),ce(4,2),we(5,eN,2,0,"div",2),hn(6,"span",3)(7,"span",4)),i&2&&(K("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),b(5),De(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2})}return n})();function tN(n){return n.hasAttribute("mat-raised-button")?"elevated":n.hasAttribute("mat-stroked-button")?"outlined":n.hasAttribute("mat-flat-button")?"filled":n.hasAttribute("mat-button")?"text":null}var X0=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[Vl,ze]})}return n})();var iN=["*"];var rN=new w("MAT_CARD_CONFIG"),J0=(()=>{class n{appearance;constructor(){let e=p(rN,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&K("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:iN,decls:1,vars:0,template:function(i,r){i&1&&(Qe(),ce(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2})}return n})();var eE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[ze]})}return n})();var jl=(()=>{class n{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var Bl=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(t,e,i,r,o){this._defaultMatcher=t,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let t=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==t&&(this.errorState=o,this._stateChanges.next())}};var Sp=class{_box;_destroyed=new Y;_resizeSubject=new Y;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new Q(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),i.unsubscribe(),this._elementObservables.delete(t)}}).pipe(Mt(e=>e.some(i=>i.target===t)),vd({bufferSize:1,refCount:!0}),Mi(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},tE=(()=>{class n{_cleanupErrorListener;_observers=new Map;_ngZone=p(P);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Sp(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var sN=["notch"],aN=["*"],nE=["iconPrefixContainer"],iE=["textPrefixContainer"],rE=["iconSuffixContainer"],oE=["textSuffixContainer"],cN=["textField"],lN=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],dN=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function uN(n,t){n&1&&tt(0,"span",21)}function fN(n,t){if(n&1&&(h(0,"label",20),ce(1,1),we(2,uN,1,0,"span",21),m()),n&2){let e=O(2);N("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),Me("for",e._control.disableAutomaticLabeling?null:e._control.id),b(2),De(!e.hideRequiredMarker&&e._control.required?2:-1)}}function mN(n,t){if(n&1&&we(0,fN,3,5,"label",20),n&2){let e=O();De(e._hasFloatingLabel()?0:-1)}}function pN(n,t){n&1&&tt(0,"div",7)}function hN(n,t){}function gN(n,t){if(n&1&&ae(0,hN,0,0,"ng-template",13),n&2){O(2);let e=mi(1);N("ngTemplateOutlet",e)}}function vN(n,t){if(n&1&&(h(0,"div",9),we(1,gN,1,1,null,13),m()),n&2){let e=O();N("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),b(),De(e._forceDisplayInfixLabel()?-1:1)}}function yN(n,t){n&1&&(h(0,"div",10,2),ce(2,2),m())}function bN(n,t){n&1&&(h(0,"div",11,3),ce(2,3),m())}function _N(n,t){}function EN(n,t){if(n&1&&ae(0,_N,0,0,"ng-template",13),n&2){O();let e=mi(1);N("ngTemplateOutlet",e)}}function wN(n,t){n&1&&(h(0,"div",14,4),ce(2,4),m())}function DN(n,t){n&1&&(h(0,"div",15,5),ce(2,5),m())}function CN(n,t){n&1&&tt(0,"div",16)}function xN(n,t){n&1&&(h(0,"div",18),ce(1,6),m())}function IN(n,t){if(n&1&&(h(0,"mat-hint",22),y(1),m()),n&2){let e=O(2);N("id",e._hintLabelId),b(),S(e.hintLabel)}}function SN(n,t){if(n&1&&(h(0,"div",19),we(1,IN,2,2,"mat-hint",22),ce(2,7),tt(3,"div",23),ce(4,8),m()),n&2){let e=O();b(),De(e.hintLabel?1:-1)}}var Fs=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["mat-label"]]})}return n})(),MN=new w("MatError");var Mp=(()=>{class n{align="start";id=p(Ht).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(gn("id",r.id),Me("align",null),K("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return n})(),TN=new w("MatPrefix");var AN=new w("MatSuffix");var fE=new w("FloatingLabelParent"),sE=(()=>{class n{_elementRef=p(re);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=p(tE);_ngZone=p(P);_parent=p(fE);_resizeSubscription=new ke;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return NN(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return n})();function NN(n){let t=n;if(t.offsetParent!==null)return t.scrollWidth;let e=t.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var aE="mdc-line-ripple--active",Hl="mdc-line-ripple--deactivating",cE=(()=>{class n{_elementRef=p(re);_cleanupTransitionEnd;constructor(){let e=p(P),i=p(dt);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Hl),e.add(aE)}deactivate(){this._elementRef.nativeElement.classList.add(Hl)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Hl);e.propertyName==="opacity"&&r&&i.remove(aE,Hl)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return n})(),lE=(()=>{class n{_elementRef=p(re);_ngZone=p(P);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ln(sN,5),i&2){let o;ue(o=fe())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:aN,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Qe(),hn(0,"div",1),Ot(1,"div",2,0),ce(3),Pt(),hn(4,"div",3))},encapsulation:2})}return n})(),Tp=(()=>{class n{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n})}return n})();var Ap=new w("MatFormField"),kN=new w("MAT_FORM_FIELD_DEFAULT_OPTIONS"),dE="fill",RN="auto",uE="fixed",FN="translateY(-50%)",Ul=(()=>{class n{_elementRef=p(re);_changeDetectorRef=p(vn);_platform=p(rt);_idGenerator=p(Ht);_ngZone=p(P);_defaults=p(kN,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Jo("iconPrefixContainer");_textPrefixContainerSignal=Jo("textPrefixContainer");_iconSuffixContainerSignal=Jo("iconSuffixContainer");_textSuffixContainerSignal=Jo("textSuffixContainer");_prefixSuffixContainers=Re(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=ib(Fs);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=oo(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||RN}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||dE;this._appearanceSignal.set(i)}_appearanceSignal=ge(dE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||uE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||uE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new Y;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ut();constructor(){let e=this._defaults,i=p(Ll);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),zi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Re(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(bd([void 0,void 0]),ie(()=>[i.errorState,i.userAriaDescribedBy]),hd(),Mt(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Mi(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),fo(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){sb({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Re(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,v=`var(--mat-mdc-form-field-label-transform, ${FN} translateX(${f}))`,E=s+a+c+l;return[v,E]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Dc(o,r._labelChild,Fs,5),jr(o,Tp,5)(o,TN,5)(o,AN,5)(o,MN,5)(o,Mp,5)),i&2){xc();let s;ue(s=fe())&&(r._formFieldControl=s.first),ue(s=fe())&&(r._prefixChildren=s),ue(s=fe())&&(r._suffixChildren=s),ue(s=fe())&&(r._errorChildren=s),ue(s=fe())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Cc(r._iconPrefixContainerSignal,nE,5)(r._textPrefixContainerSignal,iE,5)(r._iconSuffixContainerSignal,rE,5)(r._textSuffixContainerSignal,oE,5),Ln(cN,5)(nE,5)(iE,5)(rE,5)(oE,5)(sE,5)(lE,5)(cE,5)),i&2){xc(4);let o;ue(o=fe())&&(r._textField=o.first),ue(o=fe())&&(r._iconPrefixContainer=o.first),ue(o=fe())&&(r._textPrefixContainer=o.first),ue(o=fe())&&(r._iconSuffixContainer=o.first),ue(o=fe())&&(r._textSuffixContainer=o.first),ue(o=fe())&&(r._floatingLabel=o.first),ue(o=fe())&&(r._notchedOutline=o.first),ue(o=fe())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&K("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[nt([{provide:Ap,useExisting:n},{provide:fE,useExisting:n}])],ngContentSelectors:dN,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Qe(lN),ae(0,mN,1,1,"ng-template",null,0,Zo),h(2,"div",6,1),q("click",function(s){return r._control.onContainerClick(s)}),we(4,pN,1,0,"div",7),h(5,"div",8),we(6,vN,2,2,"div",9),we(7,yN,3,0,"div",10),we(8,bN,3,0,"div",11),h(9,"div",12),we(10,EN,1,1,null,13),ce(11),m(),we(12,wN,3,0,"div",14),we(13,DN,3,0,"div",15),m(),we(14,CN,1,0,"div",16),m(),h(15,"div",17),we(16,xN,2,0,"div",18)(17,SN,5,1,"div",19),m()),i&2){let o;b(2),K("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),b(2),De(!r._hasOutline()&&!r._control.disabled?4:-1),b(2),De(r._hasOutline()?6:-1),b(),De(r._hasIconPrefix?7:-1),b(),De(r._hasTextPrefix?8:-1),b(2),De(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),b(2),De(r._hasTextSuffix?12:-1),b(),De(r._hasIconSuffix?13:-1),b(),De(r._hasOutline()?-1:14),b(),K("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();b(),De((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[sE,lE,gm,cE,Mp],styles:[`.mdc-text-field {
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
`],encapsulation:2})}return n})();var ON=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],PN=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function LN(n,t){n&1&&(h(0,"span",3),ce(1,1),m())}function VN(n,t){n&1&&(h(0,"span",6),ce(1,2),m())}var jN=new w("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),mE=new w("MatChipAvatar"),pE=new w("MatChipTrailingIcon"),hE=new w("MatChipEdit"),gE=new w("MatChipRemove"),yE=new w("MatChip"),bE=(()=>{class n{_elementRef=p(re);_parentChip=p(yE);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){p(Bt).load(sr),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(Me("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),K("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",pe],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:Hr(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return n})(),BN=(()=>{class n extends bE{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qi(n)))(r||n)}})();static \u0275dir=z({type:n,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&q("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Me("tabindex",r._getTabindex()),K("mdc-evolution-chip__action--presentational",!1))},features:[et]})}return n})();var _E=(()=>{class n{_changeDetectorRef=p(vn);_elementRef=p(re);_tagName=p(tb);_ngZone=p(P);_focusMonitor=p(rr);_globalRippleOptions=p(Rs,{optional:!0});_document=p(X);_onFocus=new Y;_onBlur=new Y;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Ut();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=p(Ht).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new ye;destroyed=new ye;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=p(Pl);_injector=p(Se);constructor(){let e=p(Bt);e.load(sr),e.load(O0),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=fo(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe(),this.destroyed.emit({chip:this}),this.destroyed.complete()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&jr(o,mE,5)(o,hE,5)(o,pE,5)(o,gE,5)(o,mE,5)(o,pE,5)(o,hE,5)(o,gE,5),i&2){let s;ue(s=fe())&&(r.leadingIcon=s.first),ue(s=fe())&&(r.editIcon=s.first),ue(s=fe())&&(r.trailingIcon=s.first),ue(s=fe())&&(r.removeIcon=s.first),ue(s=fe())&&(r._allLeadingIcons=s),ue(s=fe())&&(r._allTrailingIcons=s),ue(s=fe())&&(r._allEditIcons=s),ue(s=fe())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Ln(BN,5),i&2){let o;ue(o=fe())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&q("keydown",function(s){return r._handleKeydown(s)}),i&2&&(gn("id",r.id),Me("role",r.role)("aria-label",r.ariaLabel),Dt("mat-"+(r.color||"primary")),K("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",pe],highlighted:[2,"highlighted","highlighted",pe],disableRipple:[2,"disableRipple","disableRipple",pe],disabled:[2,"disabled","disabled",pe]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[nt([{provide:yE,useExisting:n}])],ngContentSelectors:PN,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Qe(ON),tt(0,"span",0),h(1,"span",1)(2,"span",2),we(3,LN,2,0,"span",3),h(4,"span",4),ce(5),tt(6,"span",5),m()()(),we(7,VN,2,0,"span",6)),i&2&&(b(3),De(r.leadingIcon?3:-1),b(4),De(r._hasTrailingIcon()?7:-1))},dependencies:[bE],styles:[`.mdc-evolution-chip,
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
`],encapsulation:2})}return n})();var EE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({providers:[jl,{provide:jN,useValue:{separatorKeyCodes:[13]}}],imports:[Vl,ze]})}return n})();var Os=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[L0,Ul,ze]})}return n})();function wE(n){return Error(`Unable to find icon with the name "${n}"`)}function $N(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function DE(n){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${n}".`)}function CE(n){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${n}".`)}var Gn=class{url;svgText;options;svgElement=null;constructor(t,e,i){this.url=t,this.svgText=e,this.options=i}},IE=(()=>{class n{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Gn(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(lt.HTML,r);if(!s)throw CE(r);let a=io(s);return this._addSvgIconConfig(e,i,new Gn("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Gn(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(lt.HTML,i);if(!o)throw CE(i);let s=io(o);return this._addSvgIconSetConfig(e,new Gn("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(lt.RESOURCE_URL,e);if(!i)throw DE(e);let r=this._cachedIconsByUrl.get(i);return r?st($l(r)):this._loadSvgIconFromConfig(new Gn(e,null)).pipe(ho(o=>this._cachedIconsByUrl.set(i,o)),ie(o=>$l(o)))}getNamedSvgIcon(e,i=""){let r=xE(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):ud(wE(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?st($l(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ie(i=>$l(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return st(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Dn(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(lt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),st(null)})));return uo(o).pipe(ie(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw wE(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(ho(i=>e.svgText=i),ie(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?st(null):this._fetchIcon(e).pipe(ho(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(io("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(io("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw $N();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(lt.RESOURCE_URL,i);if(!s)throw DE(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ie(l=>io(l)),mo(()=>this._inProgressUrlFetches.delete(s)),po());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(xE(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return zN(o)?new Gn(o.url,null,o.options):new Gn(o,null)}}static \u0275fac=function(i){return new(i||n)(T(Gr,8),T(Im),T(X,8),T(ft))};static \u0275prov=L({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function $l(n){return n.cloneNode(!0)}function xE(n,t){return n+":"+t}function zN(n){return!!(n.url&&n.options)}var GN=["*"],WN=new w("MAT_ICON_DEFAULT_OPTIONS"),qN=new w("mat-icon-location",{providedIn:"root",factory:()=>{let n=p(X),t=n?n.location:null;return{getPathname:()=>t?t.pathname+t.search:""}}}),SE=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],KN=SE.map(n=>`[${n}]`).join(", "),QN=/^url\(['"]?#(.*?)['"]?\)$/,ME=(()=>{class n{_elementRef=p(re);_iconRegistry=p(IE);_location=p(qN);_errorHandler=p(ft);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ke.EMPTY;constructor(){let e=p(new Br("aria-hidden"),{optional:!0}),i=p(WN,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(KN),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)SE.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(QN):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Cn(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(Me("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Dt(r.color?"mat-"+r.color:""),K("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",pe],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:GN,decls:1,vars:0,template:function(i,r){i&1&&(Qe(),ce(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2})}return n})(),TE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[ze]})}return n})();var YN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2})}return n})(),XN={passive:!0},AE=(()=>{class n{_platform=p(rt);_ngZone=p(P);_renderer=p(ct).createRenderer(null,null);_styleLoader=p(Bt);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return xi;this._styleLoader.load(YN);let i=tn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new Y,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,XN)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=tn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=J({token:n,factory:n.\u0275fac})}return n})();var NE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({})}return n})();var kE=new w("MAT_INPUT_VALUE_ACCESSOR");var JN=["button","checkbox","file","hidden","image","radio","range","reset","submit"],ek=new w("MAT_INPUT_CONFIG"),RE=(()=>{class n{_elementRef=p(re);_platform=p(rt);ngControl=p(gi,{optional:!0,self:!0});_autofillMonitor=p(AE);_ngZone=p(P);_formField=p(Ap,{optional:!0});_renderer=p(dt);_uid=p(Ht).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=p(ek,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new Y;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=oo(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Is.required)??!1}set required(e){this._required=oo(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&wp().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=oo(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>wp().has(e));constructor(){let e=p(pp,{optional:!0}),i=p(gp,{optional:!0}),r=p(jl),o=p(kE,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?Ro(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Bl(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&zi(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){JN.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=z({type:n,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&q("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(gn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),Me("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),K("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe]},exportAs:["matInput"],features:[nt([{provide:Tp,useExisting:n}]),Ft]})}return n})(),FE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[Os,Os,NE,ze]})}return n})();var OE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[ze]})}return n})();var nk=["*"],PE=(()=>{class n{labelPosition="after";static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:nk,decls:1,vars:0,template:function(i,r){i&1&&(Qe(),ce(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2})}return n})();var ik=["switch"],rk=["*"];function ok(n,t){n&1&&(h(0,"span",11),Ao(),h(1,"svg",13),tt(2,"path",14),m(),h(3,"svg",15),tt(4,"path",16),m()())}var sk=new w("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),zl=class{source;checked;constructor(t,e){this.source=t,this.checked=e}},ak=(()=>{class n{_elementRef=p(re);_focusMonitor=p(rr);_changeDetectorRef=p(vn);defaults=p(sk);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new zl(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Ut();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new ye;toggleChange=new ye;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){p(Bt).load(sr);let e=p(new Br("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=p(Ht).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new zl(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=de({type:n,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&Ln(ik,5),i&2){let o;ue(o=fe())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(gn("id",r.id),Me("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),Dt(r.color?"mat-"+r.color:""),K("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",pe],color:"color",disabled:[2,"disabled","disabled",pe],disableRipple:[2,"disableRipple","disableRipple",pe],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Hr(e)],checked:[2,"checked","checked",pe],hideIcon:[2,"hideIcon","hideIcon",pe],disabledInteractive:[2,"disabledInteractive","disabledInteractive",pe]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[nt([{provide:Ml,useExisting:_t(()=>n),multi:!0},{provide:to,useExisting:n,multi:!0}]),Ft],ngContentSelectors:rk,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(Qe(),h(0,"div",1)(1,"button",2,0),q("click",function(){return r._handleClick()}),tt(3,"div",3)(4,"span",4),h(5,"span",5)(6,"span",6)(7,"span",7),tt(8,"span",8),m(),h(9,"span",9),tt(10,"span",10),m(),we(11,ok,5,0,"span",11),m()()(),h(12,"label",12),q("click",function(s){return s.stopPropagation()}),ce(13),m()()),i&2){let o=mi(2);N("labelPosition",r.labelPosition),b(),K("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),N("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),Me("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),b(9),N("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),b(),De(r.hideIcon?-1:11),b(),N("for",r.buttonId),Me("id",r._labelId)}},dependencies:[G0,PE],styles:[`.mdc-switch {
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
`],encapsulation:2})}return n})(),LE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Z({type:n});static \u0275inj=W({imports:[ak,ze]})}return n})();var ck="https://aamdb-840c9-default-rtdb.firebaseio.com",VE=(()=>{class n{constructor(e,i){this.http=e,this.zone=i,this.user=ge({name:"Admin",role:"ADMIN"})}firebaseUrl(e){return`${ck}/${e}.json`}loadState(){return this.http.get(this.firebaseUrl("aam")).pipe(ie(e=>this.normalizeState(e)),Dn(()=>st(this.emptyState())))}saveChecklist(e){return this.http.put(this.firebaseUrl("aam/checklist"),e).pipe(Dn(()=>st(null)))}saveTodayMeal(e){return this.http.put(this.firebaseUrl("aam/todayMeal"),e).pipe(Dn(()=>st(null)))}listenChecklist(e){let i=new EventSource(this.firebaseUrl("aam/checklist")),r=o=>{if(!o.data)return;let s=JSON.parse(o.data);Array.isArray(s.data)&&this.zone.run(()=>e(s.data))};return i.onmessage=r,i.addEventListener("put",r),i.addEventListener("patch",r),i.onerror=()=>i.close(),()=>i.close()}loginAs(e){this.user.set({name:e==="MOM"?"Mom":"Admin",role:e})}normalizeState(e){let i=this.emptyState();return{checklist:e?.checklist??i.checklist,todayMeal:e?.todayMeal??i.todayMeal,weeklyPlan:e?.weeklyPlan??i.weeklyPlan,recipes:e?.recipes??i.recipes,medicines:e?.medicines??i.medicines,monthlyHealth:e?.monthlyHealth??i.monthlyHealth,stats:e?.stats??i.stats,notifications:e?.notifications??i.notifications,updatedAt:e?.updatedAt}}emptyState(){return{checklist:[],todayMeal:[],weeklyPlan:[],recipes:[],medicines:[],monthlyHealth:{},stats:{daily:0,weekly:0,meals:0,tablets:0,missedTablets:0,missedMeals:0,water:0,walking:0,painTrend:[]},notifications:[]}}static{this.\u0275fac=function(i){return new(i||n)(T(Gr),T(P))}}static{this.\u0275prov=L({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function dk(n,t){if(n&1){let e=ut();h(0,"button",11),q("click",function(){let r=Ve(e).$implicit,o=O();return je(o.active.set(r.key))}),h(1,"mat-icon"),y(2),m(),h(3,"span"),y(4),m()()}if(n&2){let e=t.$implicit,i=O();K("active",i.active()===e.key),b(2),S(e.icon),b(2),S(e.label)}}function uk(n,t){if(n&1){let e=ut();h(0,"div",12)(1,"button",11),q("click",function(){Ve(e);let r=O();return je(r.switchRole("mom"))}),h(2,"mat-icon"),y(3,"home"),m(),h(4,"span"),y(5,"Mom home"),m()(),h(6,"button",11),q("click",function(){Ve(e);let r=O();return je(r.switchRole("admin"))}),h(7,"mat-icon"),y(8,"admin_panel_settings"),m(),h(9,"span"),y(10,"Admin"),m()()()}}function fk(n,t){if(n&1){let e=ut();h(0,"mat-form-field",36)(1,"mat-label"),y(2,"Reason missed"),m(),h(3,"input",37),q("ngModelChange",function(r){Ve(e);let o=O().$implicit,s=O(2);return je(s.setMissedReason(o,r))}),m(),Wo(),m()}if(n&2){let e=O().$implicit;b(3),N("ngModel",e.missedReason||""),qo()}}function mk(n,t){if(n&1){let e=ut();h(0,"mat-card",33)(1,"div",21)(2,"div")(3,"h2"),y(4),m(),h(5,"p"),y(6),m()()(),h(7,"div",22)(8,"span"),y(9),m(),h(10,"b"),y(11),m(),h(12,"span"),y(13),m()(),h(14,"button",34),q("click",function(){let r=Ve(e).$implicit,o=O(2);return je(o.markDone(r))}),y(15),m(),ae(16,fk,4,1,"mat-form-field",35),m()}if(n&2){let e=t.$implicit,i=t.index,r=O(2);N("ngClass",r.taskTone(e,i)),b(4),S(e.label),b(2),S(e.detail),b(3),S(r.windowLabel(e)),b(2),S(e.status),b(2),S(e.completedAt||"Not done"),b(),N("disabled",!r.canMarkNow(e)||e.status==="Missed"),b(),S(r.buttonText(e)),b(),N("ngIf",e.status==="Missed")}}function pk(n,t){if(n&1&&(h(0,"article",38)(1,"strong"),y(2),m(),h(3,"span"),y(4),m()()),n&2){let e=t.$implicit;b(2),S(e.meal),b(2),S(e.menu)}}function hk(n,t){n&1&&(h(0,"p",39),y(1,"No meal data in Firebase yet."),m())}function gk(n,t){if(n&1){let e=ut();h(0,"section",13)(1,"div",14)(2,"mat-card",15)(3,"strong"),y(4),Ic(5,"date"),m(),h(6,"span"),y(7),Ic(8,"date"),m()(),h(9,"mat-card",16)(10,"strong"),y(11),m(),h(12,"span"),y(13,"Today"),m()(),h(14,"mat-card",17)(15,"strong"),y(16,"Monthly sugar"),m(),h(17,"span"),y(18),m()()(),h(19,"h1"),y(20,"Dashboard"),m(),h(21,"div",18),ae(22,mk,17,9,"mat-card",19),h(23,"mat-card",20)(24,"div",21)(25,"div")(26,"h2"),y(27,"Today's Meal"),m(),h(28,"p"),y(29),m()()(),h(30,"div",22)(31,"span"),y(32,"All day"),m(),h(33,"b"),y(34),m(),h(35,"span"),y(36),m()(),h(37,"button",23),q("click",function(){Ve(e);let r=O();return je(r.active.set("today"))}),y(38,"Open Meals"),m()()(),h(39,"div",24)(40,"mat-card",25)(41,"div",26)(42,"h2"),y(43,"Today's Meal"),m(),h(44,"button",27),q("click",function(){Ve(e);let r=O();return je(r.active.set("today"))}),y(45,"Change Menu"),m()(),ae(46,pk,5,2,"article",28)(47,hk,2,0,"p",29),m(),h(48,"mat-card",30)(49,"h2"),y(50,"Health Updates"),m(),h(51,"div",31)(52,"span")(53,"b"),y(54),m(),h(55,"em"),y(56,"Meals"),m()(),h(57,"span")(58,"b"),y(59),m(),h(60,"em"),y(61,"Tabs"),m()(),h(62,"span")(63,"b"),y(64),m(),h(65,"em"),y(66,"Water"),m()(),h(67,"span")(68,"b"),y(69),m(),h(70,"em"),y(71,"Walk"),m()()(),h(72,"div",32)(73,"span"),y(74),m(),h(75,"span"),y(76),m()()()()()}if(n&2){let e=O();b(4),S(Sc(5,25,e.today,"EEEE")),b(3),S(Sc(8,28,e.today,"MMM d, y")),b(4),Ce("",e.completion(),"%"),b(7),S(e.monthlyHealth.notes),b(4),N("ngForOf",e.dashboardItems()),b(7),S(e.todayMeal[0]?.menu||"Meal plan loading"),b(5),Ce("",e.todayMeal.length," meals"),b(2),Ce("",e.todayTotals().calories," cal"),b(10),N("ngForOf",e.todayMeal),b(),N("ngIf",!e.todayMeal.length),b(5),Vn("height",e.graphHeight(e.stats.meals)),b(2),Ce("",e.stats.meals,"%"),b(3),Vn("height",e.graphHeight(e.stats.tablets)),b(2),Ce("",e.stats.tablets,"%"),b(3),Vn("height",e.graphHeight(e.stats.water)),b(2),Ce("",e.stats.water,"%"),b(3),Vn("height",e.graphHeight(e.stats.walking)),b(2),Ce("",e.stats.walking,"%"),b(5),Qo("Sugar ",e.monthlyHealth.fastingSugar||"-","/",e.monthlyHealth.ppSugar||"-"),b(2),Ce("BP ",e.monthlyHealth.bp||"-")}}function vk(n,t){if(n&1&&(h(0,"mat-card",46)(1,"span"),y(2),m(),h(3,"strong"),y(4),m()()),n&2){let e=t.$implicit;Dt(e.tone),b(2),S(e.label),b(2),S(e.value)}}function yk(n,t){if(n&1&&(h(0,"article",47)(1,"strong"),y(2),m(),h(3,"span"),y(4),m()()),n&2){let e=t.$implicit;b(2),S(e.label),b(2),Qo("",e.status," ",e.completedAt?"- "+e.completedAt:"")}}function bk(n,t){if(n&1&&(h(0,"article")(1,"mat-icon"),y(2,"warning"),m(),h(3,"span"),y(4),m()()),n&2){let e=t.$implicit;b(4),Ce("",e.label," missed")}}function _k(n,t){if(n&1&&(h(0,"article")(1,"mat-icon"),y(2,"schedule"),m(),h(3,"span"),y(4),m()()),n&2){let e=t.$implicit;b(4),S(e)}}function Ek(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Dashboard"),m(),h(3,"div",40),ae(4,vk,5,4,"mat-card",41),m(),h(5,"div",24)(6,"mat-card",42)(7,"h2"),y(8,"Today report"),m(),ae(9,yk,5,3,"article",43),m(),h(10,"mat-card",44)(11,"h2"),y(12,"Alerts"),m(),ae(13,bk,5,1,"article",45)(14,_k,5,1,"article",45),h(15,"article")(16,"mat-icon"),y(17,"warning"),m(),h(18,"span"),y(19,"No activity today after lunch"),m()(),h(20,"article")(21,"mat-icon"),y(22,"warning"),m(),h(23,"span"),y(24,"Pain level above 7"),m()(),h(25,"article")(26,"mat-icon"),y(27,"warning"),m(),h(28,"span"),y(29),m()()()()()),n&2){let e=O();b(4),N("ngForOf",e.adminCards()),b(5),N("ngForOf",e.checklist()),b(4),N("ngForOf",e.missedItems()),b(),N("ngForOf",e.timeAlerts()),b(15),S(e.monthlyHealth.notes)}}function wk(n,t){if(n&1&&(h(0,"span")(1,"b"),y(2),m(),y(3),m()),n&2){let e=t.$implicit;b(2),S(e.value),b(),S(e.label)}}function Dk(n,t){if(n&1){let e=ut();h(0,"button",11),q("click",function(){let r=Ve(e).$implicit,o=O(2).$implicit,s=O(2);return je(s.chooseMenu(o,r))}),h(1,"strong"),y(2),m(),h(3,"span"),y(4),m()()}if(n&2){let e=t.$implicit;b(2),S(e.name),b(2),S(e.vegetable)}}function Ck(n,t){if(n&1){let e=ut();h(0,"div",56)(1,"mat-form-field",57)(2,"mat-label"),y(3,"Search menu"),m(),h(4,"input",37),q("ngModelChange",function(r){Ve(e);let o=O(3);return je(o.menuSearch.set(r))}),m(),Wo(),m(),ae(5,Dk,5,2,"button",58),m()}if(n&2){let e=O(3);b(4),N("ngModel",e.menuSearch()),qo(),b(),N("ngForOf",e.menuMatches())}}function xk(n,t){if(n&1){let e=ut();h(0,"mat-card",50)(1,"div",26)(2,"h2"),y(3),m(),h(4,"button",27),q("click",function(){let r=Ve(e).$implicit,o=O(2);return je(o.startMenuChange(r))}),y(5,"Change Menu"),m()(),h(6,"p",51),y(7),m(),h(8,"div",52)(9,"span"),y(10,"Curry"),m(),h(11,"b"),y(12),m()(),h(13,"div",52)(14,"span"),y(15,"Portion"),m(),h(16,"b"),y(17),m()(),h(18,"div",53),ae(19,wk,4,2,"span",45),m(),h(20,"p",54),y(21),m(),ae(22,Ck,6,2,"div",55),h(23,"button",23),q("click",function(){let r=Ve(e).$implicit,o=O(2);return je(o.openRecipeForMeal(r))}),y(24,"Recipe"),m()()}if(n&2){let e=t.$implicit,i=O(2);b(3),S(e.meal),b(4),S(e.menu),b(5),S(e.curry),b(5),S(e.portion),b(2),N("ngForOf",i.macroList(e)),b(2),S(e.alternative),b(),N("ngIf",i.replacingMeal()===e.meal)}}function Ik(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Today's Meal"),m(),h(3,"div",48),ae(4,xk,25,7,"mat-card",49),m()()),n&2){let e=O();b(4),N("ngForOf",e.todayMeal)}}function Sk(n,t){if(n&1){let e=ut();h(0,"mat-card",61)(1,"h2"),y(2),m(),h(3,"div")(4,"strong"),y(5,"Breakfast"),m(),h(6,"span"),y(7),m()(),h(8,"div")(9,"strong"),y(10,"Lunch"),m(),h(11,"span"),y(12),m()(),h(13,"div")(14,"strong"),y(15,"Dinner"),m(),h(16,"span"),y(17),m()(),h(18,"p",54),y(19),m(),h(20,"button",27),q("click",function(){let r=Ve(e).$implicit,o=O(2);return o.search.set(r.curry),o.selectedRecipe.set(null),je(o.active.set("recipes"))}),y(21,"Recipe"),m()()}if(n&2){let e=t.$implicit;b(2),S(e.day),b(5),S(e.breakfast),b(5),S(e.lunch),b(5),S(e.dinner),b(2),S(e.alternative)}}function Mk(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Weekly Meal Plan"),m(),h(3,"div",59),ae(4,Sk,22,5,"mat-card",60),m()()),n&2){let e=O();b(4),N("ngForOf",e.rotatedWeeklyPlan())}}function Tk(n,t){if(n&1&&(h(0,"li"),y(1),m()),n&2){let e=t.$implicit;b(),S(e)}}function Ak(n,t){if(n&1){let e=ut();h(0,"mat-card",64)(1,"button",65),q("click",function(){Ve(e);let r=O(2);return je(r.selectedRecipe.set(null))}),h(2,"mat-icon"),y(3,"arrow_back"),m()(),h(4,"h2"),y(5),m(),h(6,"p")(7,"b"),y(8,"Ingredients:"),m(),y(9),m(),h(10,"ol"),ae(11,Tk,2,1,"li",45),m(),h(12,"p")(13,"b"),y(14,"Oil:"),m(),y(15),m(),h(16,"p")(17,"b"),y(18,"Salt:"),m(),y(19),m(),h(20,"p")(21,"b"),y(22,"Note:"),m(),y(23),m(),h(24,"p")(25,"b"),y(26,"Serving:"),m(),y(27),m()()}if(n&2){let e=O(2);b(5),S(e.selectedRecipe()?.name),b(4),Ce(" ",e.selectedRecipe()?.ingredients),b(2),N("ngForOf",e.selectedRecipe()?.steps),b(4),Ce(" ",e.selectedRecipe()?.oil),b(4),Ce(" ",e.selectedRecipe()?.salt),b(4),Ce(" ",e.selectedRecipe()?.diabetes),b(4),Ce(" ",e.selectedRecipe()?.serving)}}function Nk(n,t){if(n&1){let e=ut();h(0,"article",68)(1,"strong"),y(2),m(),h(3,"span"),y(4),m(),h(5,"mat-chip"),y(6),m(),h(7,"button",23),q("click",function(){let r=Ve(e).$implicit,o=O(3);return je(o.selectedRecipe.set(r))}),y(8,"View Recipe"),m()()}if(n&2){let e=t.$implicit;b(2),S(e.name),b(2),S(e.vegetable),b(2),S(e.type)}}function kk(n,t){n&1&&(h(0,"p",39),y(1,"No recipes in Firebase yet."),m())}function Rk(n,t){if(n&1&&(h(0,"div",66),ae(1,Nk,9,3,"article",67),m(),ae(2,kk,2,0,"p",29)),n&2){let e=O(2);b(),N("ngForOf",e.filteredRecipes()),b(),N("ngIf",!e.filteredRecipes().length)}}function Fk(n,t){if(n&1){let e=ut();h(0,"section",13)(1,"div",62)(2,"h1"),y(3,"Recipes"),m(),h(4,"mat-form-field",57)(5,"mat-label"),y(6,"Search"),m(),h(7,"input",37),q("ngModelChange",function(r){Ve(e);let o=O();return o.search.set(r),je(o.selectedRecipe.set(null))}),m(),Wo(),m()(),ae(8,Ak,28,7,"mat-card",63)(9,Rk,3,2,"ng-template",null,0,Zo),m()}if(n&2){let e=mi(10),i=O();b(7),N("ngModel",i.search()),qo(),b(),N("ngIf",i.selectedRecipe())("ngIfElse",e)}}function Ok(n,t){if(n&1&&(h(0,"mat-card",70)(1,"h2"),y(2),m(),h(3,"p"),y(4),m(),h(5,"div",52)(6,"span"),y(7,"Time"),m(),h(8,"b"),y(9),m()(),h(10,"div",52)(11,"span"),y(12,"Food"),m(),h(13,"b"),y(14),m()(),h(15,"div",52)(16,"span"),y(17,"Status"),m(),h(18,"b"),y(19),m()(),h(20,"button",71),y(21,"Taken"),m()()),n&2){let e=t.$implicit,i=O(2);Dt(i.statusClass(e.status)),b(2),S(e.name),b(2),S(e.dosage),b(5),S(e.time),b(5),S(e.food),b(5),S(e.status)}}function Pk(n,t){n&1&&(h(0,"p",39),y(1,"No medicine reminders in Firebase yet."),m())}function Lk(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Medicine Reminder"),m(),h(3,"div",48),ae(4,Ok,22,7,"mat-card",69),m(),ae(5,Pk,2,0,"p",29),m()),n&2){let e=O();b(4),N("ngForOf",e.medicines),b(),N("ngIf",!e.medicines.length)}}function Vk(n,t){if(n&1&&(h(0,"span"),y(1),m()),n&2){let e=t.$implicit;Vn("height",e*10,"%"),b(),S(e)}}function jk(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Reports / Stats"),m(),h(3,"div",40)(4,"mat-card",72)(5,"span"),y(6,"Daily completion"),m(),h(7,"strong"),y(8),m()(),h(9,"mat-card",73)(10,"span"),y(11,"Weekly completion"),m(),h(12,"strong"),y(13),m()(),h(14,"mat-card",74)(15,"span"),y(16,"Meals completed"),m(),h(17,"strong"),y(18),m()(),h(19,"mat-card",75)(20,"span"),y(21,"Tablets completed"),m(),h(22,"strong"),y(23),m()(),h(24,"mat-card",76)(25,"span"),y(26,"Missed tablets"),m(),h(27,"strong"),y(28),m()(),h(29,"mat-card",76)(30,"span"),y(31,"Missed meals"),m(),h(32,"strong"),y(33),m()(),h(34,"mat-card",72)(35,"span"),y(36,"Water completion"),m(),h(37,"strong"),y(38),m()(),h(39,"mat-card",73)(40,"span"),y(41,"Walking completion"),m(),h(42,"strong"),y(43),m()()(),h(44,"mat-card",42)(45,"h2"),y(46,"Pain trend"),m(),h(47,"div",77),ae(48,Vk,2,3,"span",78),m()()()),n&2){let e=O();b(8),Ce("",e.stats.daily,"%"),b(5),Ce("",e.stats.weekly,"%"),b(5),Ce("",e.stats.meals,"%"),b(5),Ce("",e.stats.tablets,"%"),b(5),S(e.stats.missedTablets),b(5),S(e.stats.missedMeals),b(5),Ce("",e.stats.water,"%"),b(5),Ce("",e.stats.walking,"%"),b(5),N("ngForOf",e.stats.painTrend)}}function Bk(n,t){if(n&1&&(h(0,"article")(1,"mat-icon"),y(2,"notifications"),m(),h(3,"span"),y(4),m()()),n&2){let e=t.$implicit;b(4),S(e)}}function Hk(n,t){if(n&1&&(h(0,"section",13)(1,"h1"),y(2,"Notifications"),m(),h(3,"mat-card",44),ae(4,Bk,5,1,"article",45),m()()),n&2){let e=O();b(4),N("ngForOf",e.notifications)}}function Uk(n,t){if(n&1){let e=ut();h(0,"button",11),q("click",function(){let r=Ve(e).$implicit,o=O();return je(o.active.set(r.key))}),h(1,"mat-icon"),y(2),m(),h(3,"span"),y(4),m()()}if(n&2){let e=t.$implicit,i=O();K("active",i.active()===e.key),b(2),S(e.icon),b(2),S(e.label)}}var jE=(()=>{class n{constructor(e){this.aam=e,this.active=ge("mom"),this.role=ge("mom"),this.today=new Date,this.search=ge(""),this.menuSearch=ge(""),this.replacingMeal=ge(null),this.selectedRecipe=ge(null),this.adminMenuOpen=ge(!1),this.checklist=ge([]),this.recipes=ge([]),this.todayMeal=[],this.weeklyPlan=[],this.medicines=[],this.monthlyHealth={},this.stats={daily:0,weekly:0,meals:0,tablets:0,missedTablets:0,missedMeals:0,water:0,walking:0,painTrend:[]},this.notifications=[],this.notified=new Set,this.windows={breakfast:{start:420,end:600,label:"7 AM - 10 AM"},lunch:{start:720,end:840,label:"12 PM - 2 PM"},dinner:{start:1140,end:1260,label:"7 PM - 9 PM"},"morning-tablet":{start:480,end:600,label:"8 AM - 10 AM"},"afternoon-tablet":{start:780,end:900,label:"1 PM - 3 PM"},"night-tablet":{start:1200,end:1320,label:"8 PM - 10 PM"},walking:{start:1020,end:1200,label:"5 PM - 8 PM"},water:{start:420,end:1320,label:"7 AM - 10 PM"}},this.completion=Re(()=>{let i=this.checklist();return Math.round(i.filter(r=>r.status==="Done").length/Math.max(i.length,1)*100)}),this.missedItems=Re(()=>this.checklist().filter(i=>i.status==="Missed")),this.dashboardItems=Re(()=>["breakfast","lunch","dinner","morning-tablet","afternoon-tablet","night-tablet","walking","water"].map(r=>this.checklist().find(o=>o.id===r||o.label.toLowerCase().replace(/\s+/g,"-")===r)).filter(r=>!!r)),this.filteredRecipes=Re(()=>{let i=this.search().toLowerCase().trim();return this.recipes().filter(r=>`${r.vegetable} ${r.name} ${r.type}`.toLowerCase().includes(i))}),this.menuMatches=Re(()=>{let i=this.menuSearch().toLowerCase().trim();return(i?this.recipes().filter(o=>`${o.name} ${o.vegetable}`.toLowerCase().includes(i)):this.recipes()).slice(0,8)}),this.timeAlerts=Re(()=>this.checklist().filter(i=>i.status==="Pending"&&this.isExpired(i)).map(i=>`${i.label} not marked during ${this.windowLabel(i)}`)),this.adminCards=Re(()=>[{label:"Today completion",value:`${this.completion()}%`,tone:"blue"},...this.checklist().filter(i=>["Breakfast","Lunch","Dinner","Morning Tablet","Afternoon Tablet","Night Tablet","Walking"].includes(i.label)).map(i=>({label:i.label,value:i.status,tone:this.tone(i.status)})),{label:"Pain level",value:"8 / 10",tone:"rose"},{label:"Missed items",value:`${this.missedItems().length}`,tone:"yellow"}]),this.recipeGroups=Re(()=>{let i=this.search().toLowerCase(),r=new Map;for(let o of this.recipes().filter(s=>`${s.vegetable} ${s.name}`.toLowerCase().includes(i)))r.set(o.vegetable,[...r.get(o.vegetable)??[],o]);return[...r.entries()].map(([o,s])=>({vegetable:o,recipes:s}))}),this.momNav=[{key:"mom",label:"Dashboard",icon:"home"},{key:"today",label:"Today's Meal",icon:"restaurant"},{key:"weekly",label:"Weekly Plan",icon:"calendar_view_week"},{key:"recipes",label:"Recipes",icon:"menu_book"},{key:"reminders",label:"Reminders",icon:"notifications_active"}],this.adminNav=[{key:"admin",label:"Dashboard",icon:"space_dashboard"},{key:"stats",label:"Weekly Stats",icon:"bar_chart"},{key:"notifications",label:"Notifications",icon:"notifications"}]}ngOnInit(){this.aam.loginAs("MOM"),this.aam.loadState().subscribe(e=>{this.applyChecklist(e.checklist),this.todayMeal=e.todayMeal,this.weeklyPlan=e.weeklyPlan,this.recipes.set(e.recipes),this.medicines=e.medicines,this.monthlyHealth=e.monthlyHealth,this.stats=e.stats,this.notifications=e.notifications,this.setupNotifications()}),this.stopChecklistListener=this.aam.listenChecklist(e=>this.applyChecklist(e))}ngOnDestroy(){this.stopChecklistListener?.(),this.notificationTimer&&clearInterval(this.notificationTimer)}currentNav(){return this.role()==="admin"?this.adminNav:this.momNav}switchRole(e){this.role.set(e),this.aam.loginAs(e==="mom"?"MOM":"ADMIN"),this.active.set(e==="mom"?"mom":"admin"),this.adminMenuOpen.set(!1)}markDone(e){this.canMarkNow(e)&&this.setStatus(e,"Done")}setStatus(e,i){let r=new Date().toLocaleTimeString([],{hour:"numeric",minute:"2-digit"});this.checklist.update(o=>o.map(s=>s.id===e.id?G(C({},s),{status:i,completedAt:i==="Done"?r:void 0,missedReason:i==="Done"?void 0:s.missedReason}):s)),this.aam.saveChecklist(this.checklist()).subscribe()}setMissedReason(e,i){this.checklist.update(r=>r.map(o=>o.id===e.id?G(C({},o),{missedReason:i}):o)),this.aam.saveChecklist(this.checklist()).subscribe()}statusClass(e){return e.toLowerCase()}bar(e){return`${Math.max(5,e)}%`}todayTotals(){return this.todayMeal.reduce((e,i)=>this.addNutrition(e,this.mealNutrition(i)),this.emptyNutrition())}rotatedWeeklyPlan(){if(!this.weeklyPlan.length)return[];let e=this.weekNumber(this.today);return this.weeklyPlan.map((i,r,o)=>{let s=o[(r+e)%o.length],a=o[(r+e+2)%o.length],c=o[(r+e+4)%o.length];return G(C({},i),{breakfast:c.breakfast,lunch:s.lunch,dinner:a.dinner,curry:s.curry,alternative:`This week swap: ${i.alternative}`})})}graphHeight(e,i=100){return`${Math.max(8,Math.min(100,e/i*100))}%`}buttonText(e){return e.status==="Done"?"Done":this.isBeforeWindow(e)?this.windowLabel(e):this.isExpired(e)?"Missed":"Mark Done"}canMarkNow(e){let i=this.windows[e.id];if(!i)return!0;let r=this.minutesNow();return r>=i.start&&r<=i.end}windowLabel(e){return this.windows[e.id]?.label??e.time}taskTone(e,i){return e.status==="Done"?"done":e.status==="Missed"?"missed":["meal-one","meal-two","meal-three","tablet-one","tablet-two","tablet-three","walk-tone","water-tone"][i]??"pending"}openRecipeForMeal(e){let i=this.findRecipeForMeal(e);this.selectedRecipe.set(i??null),this.search.set(i?.name??e.curry??e.menu),this.active.set("recipes")}startMenuChange(e){this.replacingMeal.set(e.meal),this.menuSearch.set(e.curry||e.menu)}chooseMenu(e,i){this.todayMeal=this.todayMeal.map(r=>r.meal===e.meal?G(C({},r),{menu:this.menuForMeal(r.meal,i.name),curry:i.name,alternative:r.menu,nutrition:this.recipeNutrition(i,r.meal)}):r),this.replacingMeal.set(null),this.menuSearch.set(""),this.aam.saveTodayMeal(this.todayMeal).subscribe()}mealNutrition(e){return e.nutrition??this.estimateNutrition(`${e.meal} ${e.menu} ${e.curry} ${e.portion}`)}macroList(e){let i=this.mealNutrition(e);return[{label:"Carbs",value:`${i.carbs}g`},{label:"Protein",value:`${i.protein}g`},{label:"Fibre",value:`${i.fibre}g`},{label:"Fat",value:`${i.fat}g`}]}applyChecklist(e){let i=this.enforceTimeWindows(e);this.checklist.set(i),JSON.stringify(i)!==JSON.stringify(e)&&this.aam.saveChecklist(i).subscribe()}setupNotifications(){"Notification"in window&&(Notification.permission==="default"&&Notification.requestPermission(),this.notificationTimer&&clearInterval(this.notificationTimer),this.notificationTimer=setInterval(()=>this.sendDueNotifications(),6e4),this.sendDueNotifications())}sendDueNotifications(){if(!(!("Notification"in window)||Notification.permission!=="granted"))for(let e of this.checklist())e.status!=="Pending"||!this.isExpired(e)||this.notified.has(e.id)||(this.notified.add(e.id),new Notification(`AAM reminder: ${e.label}`,{body:`Please mark it done or add a missed reason. Time: ${this.windowLabel(e)}`}))}findRecipeForMeal(e){let i=`${e.curry} ${e.menu}`.toLowerCase();return this.recipes().find(r=>i.includes(r.name.toLowerCase()))??this.recipes().find(r=>i.includes(r.vegetable.toLowerCase()))??this.recipes().find(r=>r.name.toLowerCase().includes((e.curry||"").toLowerCase()))}menuForMeal(e,i){let r=e.toLowerCase();return r.includes("breakfast")?`${i} + 1 egg or curd`:r.includes("dinner")?`2 chapatis + ${i}`:`1 cup rice + ${i} + curd salad`}recipeNutrition(e,i){return this.estimateNutrition(`${i} ${e.name} ${e.vegetable} ${e.ingredients}`)}estimateNutrition(e){let i=e.toLowerCase(),r=this.emptyNutrition();return i.includes("rice")&&this.addInPlace(r,{calories:205,carbs:45,protein:4,fibre:1,fat:1}),i.includes("chapati")&&this.addInPlace(r,{calories:220,carbs:36,protein:7,fibre:6,fat:6}),i.includes("idli")&&this.addInPlace(r,{calories:130,carbs:28,protein:4,fibre:2,fat:1}),(i.includes("dosa")||i.includes("pesarattu"))&&this.addInPlace(r,{calories:180,carbs:28,protein:6,fibre:3,fat:5}),(i.includes("oats")||i.includes("upma"))&&this.addInPlace(r,{calories:190,carbs:30,protein:6,fibre:5,fat:5}),i.includes("egg")&&this.addInPlace(r,{calories:78,carbs:1,protein:6,fibre:0,fat:5}),i.includes("chicken")&&this.addInPlace(r,{calories:180,carbs:2,protein:26,fibre:1,fat:7}),i.includes("paneer")&&this.addInPlace(r,{calories:210,carbs:6,protein:13,fibre:1,fat:15}),(i.includes("dal")||i.includes("pappu")||i.includes("rajma")||i.includes("chana"))&&this.addInPlace(r,{calories:160,carbs:24,protein:10,fibre:7,fat:3}),r.calories===0&&this.addInPlace(r,{calories:120,carbs:16,protein:4,fibre:5,fat:4}),(i.includes("curry")||i.includes("pulusu")||i.includes("fry")||i.includes("vegetable"))&&this.addInPlace(r,{calories:90,carbs:12,protein:3,fibre:5,fat:4}),r}emptyNutrition(){return{calories:0,carbs:0,protein:0,fibre:0,fat:0}}addNutrition(e,i){return{calories:e.calories+i.calories,carbs:e.carbs+i.carbs,protein:e.protein+i.protein,fibre:e.fibre+i.fibre,fat:e.fat+i.fat}}addInPlace(e,i){e.calories+=i.calories,e.carbs+=i.carbs,e.protein+=i.protein,e.fibre+=i.fibre,e.fat+=i.fat}weekNumber(e){let i=new Date(e.getFullYear(),0,1);return Math.ceil(((e.getTime()-i.getTime())/864e5+i.getDay()+1)/7)}enforceTimeWindows(e){return e.map(i=>{let r=this.windows[i.id];return r?i.status==="Done"?this.completedWithinWindow(i,r)?i:G(C({},i),{status:"Missed",completedAt:void 0}):this.isExpired(i)?G(C({},i),{status:"Missed",completedAt:void 0}):i:i})}isBeforeWindow(e){let i=this.windows[e.id];return!!(i&&this.minutesNow()<i.start)}isExpired(e){let i=this.windows[e.id];return!!(i&&this.minutesNow()>i.end)}completedWithinWindow(e,i){if(!e.completedAt)return!1;let r=this.parseTime(e.completedAt);return r>=i.start&&r<=i.end}parseTime(e){let i=e.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);if(!i)return-1;let r=Number(i[1]),o=Number(i[2]),s=i[3].toUpperCase();return s==="PM"&&r!==12&&(r+=12),s==="AM"&&r===12&&(r=0),r*60+o}minutesNow(){let e=new Date;return e.getHours()*60+e.getMinutes()}tone(e){return e==="Done"||e==="Taken"?"lime":e==="Missed"?"rose":"yellow"}static{this.\u0275fac=function(i){return new(i||n)(H(VE))}}static{this.\u0275cmp=de({type:n,selectors:[["aam-root"]],decls:21,vars:11,consts:[["recipeList",""],[1,"app-shell"],[1,"topbar"],["type","button",1,"logo-button",3,"click"],[1,"desktop-nav"],["type","button",3,"active","click",4,"ngFor","ngForOf"],[1,"admin-switch"],["type","button","aria-label","Open admin menu",1,"hamburger-button",3,"click"],["class","switch-menu",4,"ngIf"],["class","page",4,"ngIf"],[1,"bottom-nav"],["type","button",3,"click"],[1,"switch-menu"],[1,"page"],[1,"summary-row"],[1,"soft-card","date-card"],[1,"soft-card","percent-card"],[1,"soft-card","health-card"],[1,"card-grid","task-grid"],["class","soft-card task-card",3,"ngClass",4,"ngFor","ngForOf"],[1,"soft-card","task-card","today-task-card"],[1,"task-head"],[1,"task-meta"],["mat-flat-button","",1,"primary-button",3,"click"],[1,"two-column"],[1,"soft-card","meal-summary-card"],[1,"section-title"],["mat-stroked-button","",3,"click"],["class","mini-row meal-summary-row",4,"ngFor","ngForOf"],["class","empty-note",4,"ngIf"],[1,"soft-card","graph-card"],[1,"mini-graph"],[1,"health-line"],[1,"soft-card","task-card",3,"ngClass"],["mat-flat-button","",1,"primary-button",3,"click","disabled"],["class","reason-field","appearance","outline",4,"ngIf"],["appearance","outline",1,"reason-field"],["matInput","",3,"ngModelChange","ngModel"],[1,"mini-row","meal-summary-row"],[1,"empty-note"],[1,"card-grid","admin-grid"],["class","soft-card stat-card",3,"class",4,"ngFor","ngForOf"],[1,"soft-card"],["class","mini-row",4,"ngFor","ngForOf"],[1,"soft-card","alert-card"],[4,"ngFor","ngForOf"],[1,"soft-card","stat-card"],[1,"mini-row"],[1,"card-grid"],["class","soft-card meal-card",4,"ngFor","ngForOf"],[1,"soft-card","meal-card"],[1,"meal-menu"],[1,"pill-line"],[1,"macro-grid"],[1,"note"],["class","menu-picker",4,"ngIf"],[1,"menu-picker"],["appearance","outline"],["type","button",3,"click",4,"ngFor","ngForOf"],[1,"week-list"],["class","soft-card week-card",4,"ngFor","ngForOf"],[1,"soft-card","week-card"],[1,"page-head"],["class","soft-card recipe-detail inline-detail",4,"ngIf","ngIfElse"],[1,"soft-card","recipe-detail","inline-detail"],["type","button",1,"close",3,"click"],[1,"recipe-cards","recipe-list"],["class","recipe-card",4,"ngFor","ngForOf"],[1,"recipe-card"],["class","soft-card medicine-card",3,"class",4,"ngFor","ngForOf"],[1,"soft-card","medicine-card"],["mat-flat-button","",1,"primary-button"],[1,"soft-card","stat-card","blue"],[1,"soft-card","stat-card","lime"],[1,"soft-card","stat-card","pink"],[1,"soft-card","stat-card","yellow"],[1,"soft-card","stat-card","rose"],[1,"trend"],[3,"height",4,"ngFor","ngForOf"]],template:function(i,r){i&1&&(h(0,"main",1)(1,"header",2)(2,"button",3),q("click",function(){return r.active.set(r.role()==="admin"?"admin":"mom")}),y(3,"AAM"),m(),h(4,"nav",4),ae(5,dk,5,4,"button",5),m(),h(6,"div",6)(7,"button",7),q("click",function(){return r.adminMenuOpen.set(!r.adminMenuOpen())}),h(8,"mat-icon"),y(9,"menu"),m()(),ae(10,uk,11,0,"div",8),m()(),ae(11,gk,77,31,"section",9)(12,Ek,30,5,"section",9)(13,Ik,5,1,"section",9)(14,Mk,5,1,"section",9)(15,Fk,11,3,"section",9)(16,Lk,6,2,"section",9)(17,jk,49,9,"section",9)(18,Hk,5,1,"section",9),h(19,"nav",10),ae(20,Uk,5,4,"button",5),m()()),i&2&&(b(5),N("ngForOf",r.currentNav()),b(5),N("ngIf",r.adminMenuOpen()),b(),N("ngIf",r.active()==="mom"),b(),N("ngIf",r.active()==="admin"),b(),N("ngIf",r.active()==="today"),b(),N("ngIf",r.active()==="weekly"),b(),N("ngIf",r.active()==="recipes"),b(),N("ngIf",r.active()==="reminders"),b(),N("ngIf",r.active()==="stats"),b(),N("ngIf",r.active()==="notifications"),b(2),N("ngForOf",r.currentNav()))},dependencies:[Wc,pm,Gc,hm,S0,Tl,x0,hp,X0,Y0,eE,J0,EE,_E,Os,Ul,Fs,TE,ME,FE,RE,OE,LE,vm],styles:["[_nghost-%COMP%]{--light-pink: #FFDBDF;--rose-pink: #F791A9;--soft-yellow: #FFE797;--lime-pastel: #DDDD7B;--ice-blue: #E0F2F4;--muted-blue: #BAD6DA;--cream: #fffdf7;--ink: #312a26;--soft-shadow: 0 14px 34px rgba(49, 42, 38, .11);display:block;min-height:100vh;background:radial-gradient(circle at top left,rgba(255,219,223,.58),transparent 32rem),radial-gradient(circle at top right,rgba(224,242,244,.72),transparent 28rem),var(--cream);color:var(--ink);font-family:Arial,Helvetica Neue,sans-serif}.app-shell[_ngcontent-%COMP%]{min-height:100vh;padding-bottom:92px}.topbar[_ngcontent-%COMP%]{position:sticky;top:0;z-index:10;display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px clamp(16px,4vw,40px);background:#fffdf7e0;-webkit-backdrop-filter:blur(16px);backdrop-filter:blur(16px)}.logo-button[_ngcontent-%COMP%]{border:0;background:transparent;color:var(--ink);cursor:pointer;font:inherit;font-size:32px;font-weight:800;letter-spacing:0}.admin-switch[_ngcontent-%COMP%]{position:relative}.hamburger-button[_ngcontent-%COMP%]{display:grid;width:46px;height:46px;place-items:center;border:0;border-radius:50%;background:#fff;color:var(--ink);cursor:pointer;box-shadow:var(--soft-shadow)}.switch-menu[_ngcontent-%COMP%]{position:absolute;top:calc(100% + 8px);right:0;z-index:30;display:grid;min-width:168px;gap:4px;padding:8px;border-radius:8px;background:#fff;box-shadow:var(--soft-shadow)}.switch-menu[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{display:grid;grid-template-columns:24px 1fr;gap:8px;align-items:center;border:0;border-radius:8px;padding:10px;background:transparent;color:var(--ink);cursor:pointer;font:inherit;text-align:left}.desktop-nav[_ngcontent-%COMP%]{display:flex;gap:8px;padding:8px;border-radius:999px;background:#fff;box-shadow:var(--soft-shadow)}.desktop-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%], .bottom-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:8px;border:0;border-radius:999px;padding:10px 14px;background:transparent;color:var(--ink);cursor:pointer;font:inherit}.desktop-nav[_ngcontent-%COMP%]   button.active[_ngcontent-%COMP%], .bottom-nav[_ngcontent-%COMP%]   button.active[_ngcontent-%COMP%]{background:var(--soft-yellow)}.soft-action[_ngcontent-%COMP%], .primary-button[_ngcontent-%COMP%]{border-radius:999px!important;background:var(--rose-pink)!important;color:var(--ink)!important;box-shadow:none!important}.page[_ngcontent-%COMP%]{width:min(1180px,100% - 28px);margin:0 auto}.plain-link[_ngcontent-%COMP%]{width:max-content;margin:0 auto;border:0;background:transparent;color:var(--ink);cursor:pointer;font:inherit;text-decoration:underline}.page[_ngcontent-%COMP%]{display:grid;gap:18px;padding:18px 0 28px}.narrow[_ngcontent-%COMP%]{max-width:520px}h1[_ngcontent-%COMP%], h2[_ngcontent-%COMP%], p[_ngcontent-%COMP%]{margin:0}h1[_ngcontent-%COMP%]{font-size:clamp(34px,7vw,58px);line-height:1}h2[_ngcontent-%COMP%]{font-size:19px;line-height:1.15}.soft-card[_ngcontent-%COMP%]{border:0!important;border-radius:8px!important;background:#ffffffe6!important;box-shadow:var(--soft-shadow)!important;color:var(--ink)}.summary-row[_ngcontent-%COMP%], .card-grid[_ngcontent-%COMP%], .two-column[_ngcontent-%COMP%], .week-list[_ngcontent-%COMP%]{display:grid;gap:16px}.summary-row[_ngcontent-%COMP%]{grid-template-columns:1.1fr .8fr 1.4fr}.date-card[_ngcontent-%COMP%], .percent-card[_ngcontent-%COMP%], .health-card[_ngcontent-%COMP%]{padding:20px}.date-card[_ngcontent-%COMP%]{background:var(--ice-blue)!important}.percent-card[_ngcontent-%COMP%]{background:var(--soft-yellow)!important}.health-card[_ngcontent-%COMP%]{background:var(--light-pink)!important}.date-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .percent-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .health-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .stat-card[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;font-size:34px;line-height:1}.card-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(3,minmax(0,1fr))}.task-grid[_ngcontent-%COMP%]{align-items:stretch}.task-card[_ngcontent-%COMP%], .meal-card[_ngcontent-%COMP%], .medicine-card[_ngcontent-%COMP%], .recipe-group[_ngcontent-%COMP%], .stat-card[_ngcontent-%COMP%], .form-card[_ngcontent-%COMP%]{min-width:0;padding:14px;overflow-wrap:anywhere;word-break:normal}.task-card[_ngcontent-%COMP%]{display:grid;gap:10px;min-height:164px;align-content:space-between}.task-head[_ngcontent-%COMP%]{display:flex;justify-content:space-between;gap:12px}.task-head[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .note[_ngcontent-%COMP%]{margin-top:8px;color:#312a26b8;line-height:1.25;font-size:14px}.window-note[_ngcontent-%COMP%]{margin:-4px 0 0;color:#312a26ad;font-size:13px;line-height:1.2}.task-meta[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,minmax(0,auto));gap:8px;align-items:center;justify-content:space-between;padding:8px 10px;border-radius:8px;background:#fffdf7b8;font-size:14px}.task-meta[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .task-meta[_ngcontent-%COMP%]   b[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]   b[_ngcontent-%COMP%]{min-width:0;overflow-wrap:anywhere}.pill-line[_ngcontent-%COMP%]{grid-template-columns:minmax(64px,auto) minmax(0,1fr)}.pill-line[_ngcontent-%COMP%]   b[_ngcontent-%COMP%], .mini-row[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .week-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .task-head[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .meal-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .medicine-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{min-width:0;overflow-wrap:anywhere}.done[_ngcontent-%COMP%], .taken[_ngcontent-%COMP%], .lime[_ngcontent-%COMP%]{background:var(--lime-pastel)!important}.pending[_ngcontent-%COMP%], .yellow[_ngcontent-%COMP%]{background:var(--soft-yellow)!important}.meal-one[_ngcontent-%COMP%]{background:#ffdde5!important}.meal-two[_ngcontent-%COMP%]{background:#ffe797!important}.meal-three[_ngcontent-%COMP%]{background:#dceeed!important}.tablet-one[_ngcontent-%COMP%]{background:#e0f2f4!important}.tablet-two[_ngcontent-%COMP%]{background:#e9e09f!important}.tablet-three[_ngcontent-%COMP%]{background:#f2d6df!important}.walk-tone[_ngcontent-%COMP%]{background:#d8eac1!important}.water-tone[_ngcontent-%COMP%]{background:#cfe9f1!important}.today-task-card[_ngcontent-%COMP%]{background:#f8ead0!important}.missed[_ngcontent-%COMP%], .rose[_ngcontent-%COMP%]{background:var(--light-pink)!important}.pink[_ngcontent-%COMP%]{background:var(--rose-pink)!important}.blue[_ngcontent-%COMP%]{background:var(--ice-blue)!important}.two-column[_ngcontent-%COMP%]{grid-template-columns:minmax(0,1fr) minmax(280px,.9fr)}.section-title[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:10px}.section-title[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{min-width:0;overflow-wrap:anywhere}.section-title[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex:0 0 auto}.mini-row[_ngcontent-%COMP%], .alert-card[_ngcontent-%COMP%]   article[_ngcontent-%COMP%]{display:grid;grid-template-columns:minmax(92px,auto) minmax(0,1fr);gap:14px;padding:12px 0}.alert-card[_ngcontent-%COMP%]   article[_ngcontent-%COMP%]{grid-template-columns:28px minmax(0,1fr);align-items:center}.meal-summary-card[_ngcontent-%COMP%]{overflow:hidden}.meal-summary-row[_ngcontent-%COMP%]{grid-template-columns:138px minmax(0,1fr);align-items:start}.admin-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}.stat-card[_ngcontent-%COMP%]{min-height:112px}.stat-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:block;margin-bottom:22px}.week-card[_ngcontent-%COMP%]{display:grid;gap:12px;padding:14px}.week-card[_ngcontent-%COMP%]   div[_ngcontent-%COMP%]{display:grid;grid-template-columns:110px 1fr;gap:12px}.page-head[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:16px}.recipe-groups[_ngcontent-%COMP%]{display:grid;gap:16px}.recipe-cards[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin-top:14px}.recipe-card[_ngcontent-%COMP%]{display:grid;gap:10px;padding:14px;border-radius:8px;background:var(--ice-blue);min-width:0;overflow-wrap:anywhere}.recipe-list[_ngcontent-%COMP%]{margin-top:0}.empty-note[_ngcontent-%COMP%]{padding:16px;border-radius:8px;background:#e0f2f4b8}.recipe-detail[_ngcontent-%COMP%]{z-index:1;display:grid;width:min(760px,100%);max-height:none;overflow:auto;gap:12px;padding:22px;background:#fff!important}.inline-detail[_ngcontent-%COMP%]{position:static}.meal-menu[_ngcontent-%COMP%]{min-height:42px;line-height:1.3;overflow-wrap:anywhere}.macro-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}.macro-grid[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:grid;gap:2px;min-width:0;padding:8px;border-radius:8px;background:#fffdf7c7;font-size:12px;overflow-wrap:anywhere}.macro-grid[_ngcontent-%COMP%]   b[_ngcontent-%COMP%]{font-size:15px}.menu-picker[_ngcontent-%COMP%]{display:grid;gap:8px;padding:10px;border-radius:8px;background:#ffffffb8}.menu-picker[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;border:0;border-radius:8px;padding:10px;background:var(--ice-blue);color:var(--ink);cursor:pointer;font:inherit;text-align:left}.menu-picker[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%], .menu-picker[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{min-width:0;overflow-wrap:anywhere}.reason-field[_ngcontent-%COMP%]{width:100%}.graph-card[_ngcontent-%COMP%]{display:grid;gap:14px}.mini-graph[_ngcontent-%COMP%]{display:flex;align-items:end;gap:10px;height:150px;padding-top:8px}.mini-graph[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:grid;flex:1;min-width:0;min-height:28px;align-content:start;justify-items:center;gap:4px;padding:8px 4px;border-radius:8px 8px 4px 4px;background:var(--muted-blue);font-size:12px}.mini-graph[_ngcontent-%COMP%]   em[_ngcontent-%COMP%]{font-style:normal}.health-line[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:8px}.health-line[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{padding:8px 10px;border-radius:8px;background:#fffdf7c7}.close[_ngcontent-%COMP%]{justify-self:end;border:0;background:transparent;cursor:pointer}.trend[_ngcontent-%COMP%]{display:flex;align-items:end;gap:12px;height:180px;padding-top:18px}.trend[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:grid;width:38px;min-height:24px;place-items:start center;border-radius:20px 20px 8px 8px;background:var(--muted-blue)}.bottom-nav[_ngcontent-%COMP%]{position:fixed;right:14px;bottom:14px;left:14px;z-index:12;display:none;grid-template-columns:repeat(auto-fit,minmax(64px,1fr));gap:4px;padding:8px;border-radius:26px;background:#fffffff0;box-shadow:var(--soft-shadow)}.bottom-nav[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{flex-direction:column;gap:2px;padding:8px 4px;font-size:12px}@media(max-width:820px){.desktop-nav[_ngcontent-%COMP%]{display:none}.bottom-nav[_ngcontent-%COMP%]{display:grid}.summary-row[_ngcontent-%COMP%], .two-column[_ngcontent-%COMP%], .page-head[_ngcontent-%COMP%]{grid-template-columns:1fr}.page-head[_ngcontent-%COMP%]{display:grid}.card-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:560px){.topbar[_ngcontent-%COMP%]{padding:12px 14px}.card-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}.macro-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(2,minmax(0,1fr))}.mini-row[_ngcontent-%COMP%], .week-card[_ngcontent-%COMP%]   div[_ngcontent-%COMP%], .task-meta[_ngcontent-%COMP%], .pill-line[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr}}"]})}}return n})();xm(jE,{providers:[q_(),lm(),Ta(X_.register("ngsw-worker.js",{enabled:!cb()}))]}).catch(n=>console.error(n));
