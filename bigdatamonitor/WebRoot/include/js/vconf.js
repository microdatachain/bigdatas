/**
 * @fileoverview  validator.js
 * @author creator
**/
(function(){

	/**
	* SSO check user exist
	* author creator
	* date 2007.5.8
	* last modify by creator
	*/
	var checkUrl = "/signup/check_user.php";
	var checkDomainUrl = "/signup/check_domain.php";
	var memberYes = "";
	var memberNo = "";
	var error = "";
	var defer = "";
	var type1 = "freemail",type2 = "vipmail",type3 = "sinauser",type4 = "2008mail";
	
	/* ajax engine */
	function ajaxCheck(url,from,name, callBack) {
		var XHR;
		var date = new Date();
		var parameter = "from=" + from + "&name=" + name + "&timeStamp= " + date.getTime();
		try {
			try{
				XHR=new ActiveXObject("Microsoft.XMLHTTP");
			}catch(e){
					try{
						XHR=new XMLHttpRequest();
					} catch (e){ }
			}
			XHR.open("POST",url);
			XHR.setRequestHeader("Content-Type","application/x-www-form-urlencoded");
			XHR.onreadystatechange = function(){
				if(XHR.readyState==4) {
					if(XHR.status==200) {
						if(callBack) callBack(from,XHR.responseText);
					}
				}
			}
			XHR.send(parameter);
		}catch (e) {
			alert(e.toString());
		}
	}
	
	/* checkUserExist */
	function checkUserExist(from,name, callBack) {
		ajaxCheck(checkUrl,from,name, callBack);
	}				
	function checkDomainExist(from,name,callBack) {
		ajaxCheck(checkDomainUrl,from,name,callBack);
	}

	/**
	 * 
	 */	
	function checkUsername(e, v) {
		checkUserExist('othermail', e.value, function(from, responseText){
			if(from == 'othermail'){
				var msg = "";
				if(responseText.search("no")>=0){
					msg = 'hide';
				}else if(responseText.search("yes")>=0){
					msg = '******************';
				}else{
					msg = '';
				}
				v.showErr(e, msg);
			}
		});
	}

	/**
	 * 
	 */	
	function checkFreemail(e, v) {
		checkUserExist('freemail', e.value, function(from, responseText){
			if(from == 'freemail'){
				var msg = "";
				if(responseText.search("no")>=0){
					msg = 'hide';
				}else if(responseText.search("yes")>=0){
					msg = '';
				}else{
					msg = '';
				}
				v.showErr(e, msg);
			}
		});
	}

	/**
	 *   Ajax 
	 */
	function checkDomain(e, v) {
		checkDomainExist('sinauser', e.value, function(from, responseText){
			if(from == 'sinauser'){
				var msg = "";
				if(responseText.search("no")>=0){
					msg = 'hide';
				}else if(responseText.search("yes")>=0){
					msg = '';
				}else{
					msg = '';
				}
				v.showErr(e, msg);
			}
		});
	}
	
	/**
	 *  1
	 */
	function CharMode(iN) {
		if (iN >= 65 && iN <= 90) return 2;
		if (iN >= 97 && iN <= 122) return 4;
		else return 1;
	}
	
	/**
	 *  2
	 */
	function bitTotal(num) {
		var modes = 0;
		for (var i=0;i<3;i++) {
			if (num & 1) modes++;
			num >>>= 1;
		}
		return modes;
	}
	
	
	
	/**
	 *  
	 */
	var conf = {
		/**
		 *  
		 */
		//'submitFn': function(el){
			
		//},
		/**
		 *  
		 *  HTML
		 */
		'focusFn': function(el, v){
			var alt = el.alt;
			var arg = /focusFn{([^}].+?)}/.exec(alt);
			arg = (arg == null) ? false : arg[1];
			$removeClassName($(arg), 'hide');
		},
		'': {						
			msg: '{name}{range}'
		},
		'': {						
			msg: '{name}'
		},
		"": {
			msg: '{name}',
			reg: /./
		},
		"sel": {
			msg: '{name}',
			reg: /./
		},
		"": {
			msg: '{name}',
			reg: /[^\d]+/
		},
		"": {
			msg: '{name}',
			reg: /^[^\d]+$/
		},
		"": {
			msg: '{name}',
			reg: /^[^ ]+$/
		},
		"": {
			msg: '',
			reg: /^[0-9a-z][_.0-9a-z-]{0,31}@([0-9a-z][0-9a-z-]{0,30}\.){1,4}[a-z]{2,4}$/
		},
		"": {
			msg: '{name}',
			reg: /^1(3\d{1}|5[389])\d{8}$/
		},
		"": {
			msg: '{name}',
			reg: /^(d){5,18}$/
		},
		"": {
			msg: '{name}',
			reg: /[A-Z]/,
			regFlag: true
		},
		"": {
			msg: '{name}',
			reg: /[\uFF00-\uFFFF]/,
			regFlag: true
		},
		"": {
			msg: '',
			reg: /(^\s+)|(\s+$)/,
			regFlag: true
		},
		"": {
			msg: '{name}',
			reg: />|<|,|\[|\]|\{|\}|\?|\/|\+|=|\||\'|\\|\"|:|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\(|\)|`/i ,
			regFlag : true
		},
		"pwd": {
			msg: '',
			reg: />|<|\+|,|\[|\]|\{|\}|\/|=|\||\'|\\|\"|:|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\(|\)|`/i,
			regFlag : true
		},
		"": {
			msg: '{name}',
			reg: />|<|,|\[|\]|\{|\}|\?|\/|\+|=|\||\'|\\|\"|:|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\(|\)|\-|\|\.|`/i ,
			regFlag : true
		},
		"": {
			msg: '{name} . ',
			reg: />|<|,|\[|\]|\{|\}|\?|\/|\+|=|\||\'|\\|\"|:|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\(|\)|\-|\|`/i ,
			regFlag : true
		},
		"": {
			msg: '{name} :/,.() ',
			reg: />|<|\[|\]|\{|\}|\?|\+|=|\||\'|\\|\"|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\-|\|`/i ,
			regFlag : true
		},
		"": {
			msg: '{name},:> . ',
			reg: /<|,|\[|\]|\{|\}|\?|\/|\+|=|\||\'|\\|\"|:|;|\~|\!|\@|\#|\*|\$|\%|\^|\&|\(|\)|\-|\|`/i ,
			regFlag : true
		},
		"": {
			msg: '{name}',
			reg: /[\u4E00-\u9FA5]/i,
			regFlag : true
		},
		"": {
			msg: '{name}',
			reg: /[^a-zA-Z\.·\u4E00-\u9FA5\uFE30-\uFFA0]/i,
			regFlag : true
		},
		"": {
			msg: '',
			fn:  function(e, v){
				var val = e.value;
				return (val.slice(val.length-1)=="_") ? this.msg : '';
			}
		},
		"": {
			msg: '',
			reg: /(^_+)|(_+$)/,
			regFlag: true
		},
		"": {
			msg: '',
			fn:  function(e, v){
				var val = e.value;
				return (val.search("_") >= 0) ? this.msg : '';
			}
		},
		"": {
			fn:  function(e, v){
				if(!e.value){
					e.style.background = '';
					return 'custom';
				}else { 
					return ''; 
				}
			}
		},
		"": {
			msg: '',
			reg: /[^0-9a-zA-Z]/i,
			regFlag : true
		},
		"": {
			msg: '',
			reg: /[^0-9]/i,
			regFlag : true
		},
		"": {
			msg: '',
			reg: /[^0-9a-zA-Z\u4E00-\u9FA5]/,
			regFlag : true
		},
		"": {
			msg: '',
			reg: /[^0-9a-zA-Z\u4E00-\u9FA5\_\ ]/,
			regFlag : true
		},
		"": {
			msg: '{name}',
			fn: function(e,v) {
				switch (e.type.toLowerCase()) {
					case 'checkbox':
						return e.checked ? 'clear' : this.msg;
					case 'radio':
						var radioes = document.getElementsByName(e.name);
						for(var i=0; i<radioes.length; i++) {
							if(radioes[i].checked) return 'clear';
						}
						return this.msg;
					default:
						return 'clear';
				}
			}
		},
			"": {
			msg: '{name}',
			fn: function(e,v) {
				switch (e.type.toLowerCase()) {
					case 'select-one':
							return e.value ? 'clear': this.msg;
					default:
						return 'clear';
				}
			}
		},
		"": {
			msg: '{name}',
			fn: function(e,v) {
				switch (e.type.toLowerCase()) {
					case 'checkbox':
						return e.checked ? 'clear' : this.msg;
					case 'radio':
						var radioes = document.getElementsByName(e.name);
						for(var i=0; i<radioes.length; i++) {
							if(radioes[i].checked) return 'clear';
						}
						return this.msg;
					default:
						return 'clear';
				}
			}
		},
		"": {
			fn: function(e,v) {
				for (var i=1;i<=3;i++) {
					try {
						$removeClassName($("passW" + i), "passWcurr");
					}catch (e) {}
				}
				var password = e.value;
				var Modes = 0;
				var n = password.length;
				for (var i=0;i<n;i++) {
					Modes |= CharMode(password.charCodeAt(i));
				}
				var btotal = bitTotal(Modes);
				if (n >= 10) btotal++;
				switch(btotal) {
					case 1:
						try {
							$addClassName($("passW1"), "passWcurr");
						}catch (e) {}
						return;
					case 2:
						try {
							$addClassName($("passW2"), "passWcurr");
						}catch (e) {}
						return;
					case 3:
						try {
							$addClassName($("passW3"), "passWcurr");
						}catch (e) {}
						return;
					case 4:
						try {
							$addClassName($("passW3"), "passWcurr");
						}catch (e) {}
						return;
					default:
						return;
				}
			}
		},
		"": {
			fn: function(e,v) {
				if (/^[0-9a-zA-Z]/.test(e.value)) {
					if (/[^0-9a-zA-Z]/.test(e.value)) return "";
				}else if (/^[\u4E00-\u9FA5]/.test(e.value)) {
					if (/[^\u4E00-\u9FA5]/.test(e.value)) return "";
				}
				return "";
			}
		},
		"": { fn: checkUsername },
		"": { fn: checkFreemail },
		"": { fn: checkDomain }
	}
	
	//conf
	if (window.$vconf == null) window.$vconf = conf;
})();
