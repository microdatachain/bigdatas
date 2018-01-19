/**
 * @fileoverview 
 * @author creator | creator
 * @version 0.1
 */
(function(){

    /**
     * @constructor
     * @param {Object}  
     * @author xs
     */
    var $vdt = function(config){
		/**
		* 
		* @type {Object}
		*/	
        var me = this;
		
		/**
		* , 
		* @type {Object}
		*/			
        this.opt = config;

		/**
		* 
		* @type {String}
		*/		
		this.focFun = function(node) {
			me.setState(node, 1);
		};
		/**
		* 
		* @type {String}
		*/			
        this.errFun = function(node) {
			me.setState(node, 2);
		};
		/**
		* 
		* @type {String}
		*/
        this.finFun = function(node) {
			me.setState(node, 0);
		};

		/**
		* 
		* @type {String}
		*/
		var splitSign = ':';
		
		/**
		 * 
         * @param {Object} 
         * @param {Num}  0- 1- 2-
		 */
		this.setState = function(node, type) {
			var input = node;
			while(node && node != document) {
				//tagName
				if(node.tagName){
					if(node.tagName.toLowerCase() == "span" && hasClass(node, "input")) {
						switch(type) {
							case 0:
								$removeClassName(node, "inputGreen");
								$removeClassName(node, "inputRed");
								return;
							case 1:
								$removeClassName(node, "inputRed");
								$addClassName(node, "inputGreen");
								return;
							case 2:
								$removeClassName(node, "inputGreen");
								$addClassName(node, "inputRed");
								return;
							default:
								$removeClassName(node, "inputGreen");
								$removeClassName(node, "inputRed");
								return;
						}
					}
				}
				node = node.parentNode;
			}
		};
		
	   /**
		 * ()
         * @param {Object} 
         * @param {Object} Validator 
		 */
		this.defaultRegFn = function(e, v){
			//trace( 'regFlag:'+ this.regFlag +' ||' +this.reg + '|this.reg.test('+e.value+'):' + this.reg.test(e.value));
			if(this.regFlag) {
				return this.reg.test(e.value) ? this.msg : '';
			}else {
				return !this.reg.test(e.value) ? this.msg : '';
			}
        };

	   /**
		 * ()
         * @param {Object} 
         * @param {Object} Validator 
		 */
		this.defaultRangeFn = function(e, v){
			var len = e.value.getBytes();
			if(!len) return '';
			var alt = (e.alt || e.getAttribute("alt"));
			var range =  /{(.+?)}/.exec(alt)[1];
			var l = range.split('-')[0];
			var r = range.split('-')[1];
			if(len < l || len > r) return this.msg.replace('{range}', range);
			return '';
        };

	   /**
		 * ([])
         * @param {Object} 
         * @param {Object} Validator 
		 */
		this.defaultSameFn = function(e, v){
			var val = e.value;
			if(!val) return '';
			var alt = (e.alt || e.getAttribute("alt"));
			var id =  /{(.+?)}/.exec(alt)[1];
			if(!$(id)) return '';
			return ($(id).value != val)?this.msg:'';
        };
        
        /**
         * 
         * @param {String} id
         */
        this.init = function(id){
			var fm = $(id);
			if(!fm) {
				alert(' [' + id + '] ');
				return;
			}
            var fe = fm.elements;
            var fl = fe.length;
            for (var i = 0; i < fl; i++) {
                var cur = fe[i];
                var alt = ((cur.alt || cur.getAttribute("alt")) + '');
                if (alt.indexOf(splitSign) != -1) {
                    cur.onfocus = this.chkFocus;
                    cur.onblur = this.check;
					/**
					 * 
					 */
					if (cur.id.toLowerCase() == "password") {
						cur.onkeydown = this.chkKeyboard;
						cur.onkeyup = this.chkKeyboard;
					}
					if (cur.id.toLowerCase() == "door") {
						cur.value = "";
						cur.style.color = "#999999";
					}
					/**
					 * 
					 * @type {Object}
					 */
                    cur.v = this;
                }
            }
			fm.v = this;
            fm.onsubmit = this.checkSubmit;
        };


        /**
         * 
         * @param {String} alt
         * @param {Object} 
         */
		this.getCur = function(c, conf){
			var cur = conf[c];
			if (c.indexOf('')!=-1){
				cur = conf[''];
				cur.fn = this.defaultRangeFn;
			}
			if (c.indexOf('')!=-1){
				cur = conf[''];
				cur.fn = this.defaultSameFn;
			}
			if(!cur) return null;
			if (!cur.fn) cur.fn = this.defaultRegFn;
			return cur;
		};
        
        /**
         * altconfig
         * @param {Object} fireFox ()
         * @param {Object} 
         */		
        this.check = function(e, el){
            var el = el || this;
            var v = el.v;
            var o = v.opt;
            var alt = el.alt || el.getAttribute('alt');
            var args = alt.split(splitSign)[1].split('/');
            var l = args.length;
            for (var i = 0; i < l; i++) {
                var c = args[i];
				var cur = v.getCur(c, o);
                if (cur) {
					try {
						trace(cur.fn);
						var result = cur.fn(el, v);
						if(result == 'custom'){
							break;
						}
						if(result){
							v.showErr(el, result);
							if(result == 'clear') continue;
							return false;
						}
					}catch (e) {
						return false;
					}
                }
            }
			if(result == 'clear') {
				v.showErr(el, result);
			}else {
				v.showErr(el,'hide');
			}
			return true;
        };

        /**
         * 
         */
        this.chkFocus = function(){
			if(this.type == 'password' || this.type == 'text') {
				me.focFun(this);
			}
            var el = this;
            var v = el.v;
			var cur = v.opt;
            alt = el.alt || el.getAttribute('alt');
			try{
				if(alt.indexOf('focusFn')!= -1){
					//alert(cur.focusFn);
					if(cur.focusFn) cur.focusFn(el, v);
				}
				try {
					me.showErr(el, "clear");
					if (el.id == "password") {
						if (!$isVisible($("passW"))) $show($("passW"));
					}
					if (el.id == "door") {
						if (el.value == "") {
							el.value = "";
							el.style.color = "#000000";
						}
						if (!$isVisible($("door_img"))) {
							$show($("door_img"));
							con_code();
						}
					}
				}catch (e) {}
			}catch(e){ alert(e.description);}
        };
		
		/**
         * 
         */
		this.chkKeyboard = function(){
            var el = this;
            var v = el.v;
			var cur = v.opt;
            alt = el.alt || el.getAttribute('alt');
			try{
				if(alt.indexOf('keyFn') != -1) {
					var arg = /keyFn{([^}].+?)}/.exec(alt);
					arg = (arg == null) ? false : arg[1];
					var args = arg.split('/');
					var l = args.length;
					for (var i = 0; i < l; i++) {
						var c = args[i];
						var cur = v.getCur(c, cur);
						if (cur) {
							try {
								trace(cur.fn);
								if(cur.fn) cur.fn(el, v);
							}catch (e) {
								return false;
							}
						}
					}
				}
			}catch(e){ alert(e.description);}
        };

        /**
         * 
         */
        this.checkSubmit = function(e){
			var fm = this;
			var v   = fm.v;
			var fe = fm.elements;
			var fl = fe.length;
			var flag = true;
			try{
				for (var i = 0; i < fl; i++) {
						var cur = fe[i];
						var alt = ((cur.alt || cur.getAttribute("alt")) + '');
						if (alt.indexOf(splitSign) != -1){
							if (!$isVisible(cur) && cur.id != "selectQid") continue;
							if (v.opt.submitFn){
								if(v.opt.submitFn(cur)){
									flag = true;
									continue;
								}
							}
							if (!v.check(e, cur)) {
								flag = false;
							}
						}
				}
			}catch(e){ 
				flag = false;
			};
			if(!flag){
				alert("");
				flag = false;
			}
			return flag;
        };
		
        /**
         * 
         */					
        this.showErr = function(e, msg){
			var alt	= (e.alt || e.getAttribute("alt")).split(splitSign);
			var name = alt[0];
			var args   = alt[1];
			var msg = msg.replace('{name}', name);
			/**
	         *  alt
	         */
			var eid, errArea ;
			if(args.indexOf('errArea')!=-1) var eid = /errArea{(.+?)}/.exec(alt)[1];
			try {
				$(eid).innerHTML = "";
				e.errNode = null;
			}catch (e) {}
			errArea = $(eid) ? $(eid) : e.parentNode;
            if (!e.errNode) {
                var etips = $C('span');
                errArea.appendChild(etips);
                e.errNode = etips;
            }else {
                var etips = e.errNode;
            }
	        /**
	         *  msg
	         */
			switch(msg) {
				case "hide":
					etips.innerHTML = '<span class="inputacc"><span class="yes"></span></span>';
					if(e.type == 'password' || e.type == 'text') {
						this.finFun(e);
					}
					return;
				case "custom":
					etips.innerHTML = "";
					//errArea.removeChild(e.errNode);
					//e.errNode = null;
					return;
				case "clear":
					etips.innerHTML = "";
					return;
				default:
					etips.innerHTML = '<span class="inputacc link"><span class="error"></span><span class="red">'+ msg +'</span></span>';
					if(e.type == 'password' || e.type == 'text') {
						this.errFun(e);
						try {
							if (e.id == "password") $hide($("passW"));
						}catch (e) {}
					}
					return;
			}
        };
    };

	/**
	 *  
	 */	
    if (window.Validator == null) window.Validator = $vdt;
})();
