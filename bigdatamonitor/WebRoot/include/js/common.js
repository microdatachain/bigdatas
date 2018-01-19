/**
 * @fileoverview  
 * @author xs | creator
 */

/**
 *  debug box
 */
function trace(txt) {
	return;
	if(!$('debugbox')) {
		var debug = $C('textarea');
		debug.id = 'debugbox';
		debug.rows = '20';
		debug.cols = '150';
		document.body.appendChild(debug);
	}else{
		var debug = $('debugbox');
	}
	debug.value += (txt + ' \n--------------------------------------------------\n\n');
}

/**
 * Functionbind
 * @param {Object} 
 */
Function.prototype.bind = function(object) {
	var __method = this;
	return function() {
		__method.apply(object, arguments);
	}
};

/**
 * StringgetBytes(2)
 */
String.prototype.getBytes = function() {
	return this.replace(/[^\x00-\xff]/ig,"oo").length;
};

/**
 * 
 * @method $addEvent2
 * @param {String} elm id
 * @param {Function} func 
 * @param {String} evType :click, mouseover
 * @global $addEvent2
 * @update 2008-2-28
 * @author	creator
 * @example
 * 		//testElealert "clicked"
 * 		$addEvent2("testEle",function () {
 * 			alert("clicked")
 * 		},'click');
 */
function $addEvent2(elm, func, evType, useCapture) {
	var elm = $(elm);
	if(!elm) return;
	var useCapture = useCapture || false;
	var evType = evType || 'click';
	if (elm.addEventListener) {
		elm.addEventListener(evType, func, useCapture);
		return true;
	}
	else if (elm.attachEvent) {
		var r = elm.attachEvent('on' + evType, func);
		return true;
	}
	else {
		elm['on' + evType] = func;
	}
};

/**
 * IEcontains
 * @example obj.contains(obj);
 */
if(!(/msie/).test(navigator.userAgent.toLowerCase())) {
	if(typeof(HTMLElement) != "undefined") {
		HTMLElement.prototype.contains = function (obj) {
			while(obj != null && typeof(obj.tagName) != "undefind") {
				if(obj == this) return true;
				obj = obj.parentNode;
			}
			return false;
		};
	}
}

/**
 * document.getElementById 
 * @param {String} id
 */
function $(oID) {
	var node = typeof oID == "string" ? document.getElementById(oID) : oID;
	if (node != null) {
		return node;
	}
	return null;
}

/**
 * document.createElement 
 * @param {String} tagName
 */
function $C(tagName){
    return document.createElement(tagName);
}

/**
 * 
 * @param {String}  
 * @param {Array}   
 * @param {Boolean}   ()
 */
function $inArr(key, arr, same) {
	if(!same) return arr.join(' ').indexOf(key) != -1;
	for (var i=0, l = arr.length; i< l ; i++) if(arr[i]==key) return true;
	return false;
}

/**
 *  className
 * @param {Object} className
 * @param {String}   className
 */
function $addClassName(el, cls) {
	var el = $(el);
	if(!el) return;
	var clsNames = el.className.split(/\s+/);
	if(!$inArr(cls, clsNames, true)) el.className += ' '+cls;
};

/**
 *  className
 * @param {Object} className
 * @param {String}   className
 */
function $removeClassName(el, cls) {
	var el = $(el);
	if(!el) return;
	el.className = el.className.replace(new RegExp("(^|\\s+)" + cls + "(\\s+|$)"), ' ');
};

/**
 *  className
 * @param {Object} className
 * @param {String}   className
 */
function hasClass(node, className) {
	var elementClassName = node.className;
	return (elementClassName.length > 0 && (elementClassName == className || new RegExp("(^|\\s)" + className + "(\\s|$)").test(elementClassName)));
}

/**
 * Left
 * @param {Object} 
 */
function getLeft(node) {
	var obj = node;
	var aLeft = obj.offsetLeft;
	while(obj = obj.offsetParent) {
		aLeft += obj.offsetLeft;
	}
	return aLeft;
}

/**
 * Top
 * @param {Object} 
 */
function getTop(node) {
	var obj = node;
	var aTop = obj.offsetTop;
	while(obj = obj.offsetParent) {
		aTop += obj.offsetTop;
	}
	return aTop;
}

/**
 * 
 * @param {Object} 
 * @param {String}  
 */
function GetCurrentStyle(el, prop) {
	if (el.currentStyle) {
		return el.currentStyle[prop];
	}else if (window.getComputedStyle) {
		prop = prop.replace(/([A-Z])/g, "-$1");
		prop = prop.toLowerCase();
		return window.getComputedStyle(el, "").getPropertyValue(prop);
	}
	return null;
}

/**
 * HTML/
 * @param {String} id
 * @param {String}  display  ( 'none' , 'block', '');
 */
function $toggle(el, flag){
	var el = $(el);
	if(!el) return;
	var curState = GetCurrentStyle(el, 'display');
	if(typeof flag == 'undefined') flag = (curState == 'none' ? 'block' : 'none');
	el.style.display = flag;
}

/**
 * HTML
 * @param {String} id
 */
function $show(el){
	$toggle(el, '');
}

/**
 * HTML
 * @param {String} id
 */
function $hide(el){
	$toggle(el, 'none');
}

/**
 * 
 * @param {Object} 
 */
function $isVisible(el){
	while(el && el != document) {
		if (GetCurrentStyle(el, 'visibility') == 'hidden' || GetCurrentStyle(el, 'display') == 'none') return false;
		el = el.parentNode;
    }
	return true;
}

/**
 * 
 * @param {Function} 
 */
function onReady(fn) {
	if (typeof fn != "function") return;
	if (window.addEventListener) {
		window.addEventListener("load", fn, false);
	}else {
		window.attachEvent("onload", fn);
	}
}