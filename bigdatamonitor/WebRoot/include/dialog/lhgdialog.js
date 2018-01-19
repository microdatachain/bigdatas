/*!
 * lhgcore Dialog Plugin v4.1.1
 * Date: 2012-02-14 14:07:00
 * http://code.google.com/p/lhgdialog/
 * Copyright 2009-2012 LiHuiGang
 */

;(function( $, window, undefined ){
var _ie = !!window.ActiveXObject,
    _ie6 = _ie && !window.XMLHttpRequest,
	_noop = function(){},
	_count = 0,
	_rurl = /^url:/,
	_box, onKeyDown,
	_expando = 'JDG' + (new Date).getTime(),
	document = window.document,

/*!
 * _path lhgdialog.js
 * _args lhgdialog.jsurllhgdialog.js?self=true&skin=aero?
 */
_args, _path = (function( script, i, me )
{
    var l = script.length;
	
	for( ; i < l; i++ )
	{
		me = !!document.querySelector ?
		    script[i].src : script[i].getAttribute('src',4);
		
		if( me.substr(me.lastIndexOf('/')).indexOf('lhgdialog') !== -1 )
		    break;
	}
	
	me = me.split('?'); _args = me[1];
	
	return me[0].substr( 0, me[0].lastIndexOf('/') + 1 );
})(document.getElementsByTagName('script'),0),        

/*!
 * url
 * @param  {String}
 * @return {String||null}
 * @demo lhgdialog.js?skin=aero | _getArgs('skin') => 'aero'
 */
_getArgs = function( name )
{
    if( _args )
	{
	    var p = _args.split('&'), i = 0, l = p.length, a;
		for( ; i < l; i++ )
		{
		    a = p[i].split('=');
			if( name === a[0] ) return a[1];
		}
	}
	return null;
},

/*!  default */
_skin = _getArgs('skin') || 'sany',

/*!  lhgdialog  window  document  */
_doc, _top = (function(w)
{
	try{
	    _doc = w['top'].document;  // |
		_doc.getElementsByTagName; // chrome 
	}catch(e){
	    _doc = w.document; return w;
	}
	
	// selftrue
	if( _getArgs('self') === 'true' ||
	    _doc.getElementsByTagName('frameset').length > 0 )
	{
	    _doc = w.document; return w;
	}
	return w['top'];
})(window),

_root = _doc.documentElement, _$doc = $(_doc),
_$top = $(_top), _$html = $(_doc.getElementsByTagName('html')[0]);

// XHTML1.0HTML5DOCTYPE
if( _doc.compatMode === 'BackCompat' )
{
    alert( '\u9519\u8BEF: \u9875\u9762\u6587\u6863\u7C7B\u578B(DOCTYPE)\u672A\u58F0\u660E!' );
	return;
}

/*! IE6 CSS */
try{
	_doc.execCommand( 'BackgroundImageCache', false, true );
}catch(e){};

/*!  */
(function(style){
	if(!style)
	{
	    var head = _doc.getElementsByTagName('head')[0],
		    link = _doc.createElement('link');
			
		link.href = _path + 'skins/' + _skin + '.css';
	    link.rel = 'stylesheet';
		link.id = 'lhgdglink';
		head.insertBefore(link, head.firstChild);
	}
})(_doc.getElementById('lhgdglink'));

/*-----------------------------------------------------------------------------------------*/
if(!window.top.global_top_zIndex)
{
	window.top.global_top_zIndex = 1000
}
/*!
 * lhgdialog 
 */
var lhgdialog = function( config, ok, cancel )
{
	config = config || {};
	
	if( typeof config === 'string' )
		config = { content: config };
	
	var api, setting = lhgdialog.setting;
		
	// 
	for( var i in setting ){
		if( config[i] === undefined ) config[i] = setting[i];
	};
	
	config.id = config.id || _expando + _count;
	
	// idid
	api = lhgdialog.list[config.id];
	if(api) return api.zindex().focus();
	
	// 
	config.button = config.button || [];
	
	config.ok = ok || config.ok;
	config.cancel = cancel || config.cancel;
	
	config.ok && config.button.push({
	    name: config.okVal,
		callback: config.ok,
		focus: true
	});
	
	config.cancel && config.button.push({
	    name: config.cancelVal,
		callback: config.cancel
	});
	
	// zIndex
	lhgdialog.setting.zIndex = config.zIndex;
	
	_count++;
	
	return lhgdialog.list[config.id] = _box ?
	    _box._init(config) : new lhgdialog.fn._init( config );
};

lhgdialog.fn = lhgdialog.prototype =
{
    constructor: lhgdialog,
	
	_init: function( config )
	{
	    var that = this, DOM,
			txt = config.content,
			isIfr = _rurl.test(txt),
		    icon = config.icon;
			
		that.config = config;
		that.DOM = DOM = that.DOM || that._getDOM();
		
		// openerwindow
		that.opener = window;
		
		
		
		// 
		if( icon && !isIfr )
		{
		    config.min = false;
			config.max = false;
			
			DOM.icon[0].style.display = '';
			DOM.icon_bg[0].src = config.path + 'skins/icons/' + icon;
		}
		else
		    DOM.icon[0].style.display = 'none';
		
		DOM.wrap[0].style.display = 'block';
		DOM.wrap.addClass( config.skin ); // 
		DOM.rb[0].style.cursor = config.resize ? 'se-resize' : 'auto';
		DOM.title[0].style.cursor = config.drag ? 'move' : 'auto';
		DOM.max[0].style.display = config.max ? 'inline-block' : 'none';
		DOM.min[0].style.display = config.min ? 'inline-block' : 'none';
		DOM.close[0].style.display = config.cancel === false ? 'none' : 'inline-block'; //cancelfalse
		DOM.content[0].style.padding = config.padding;
		
		that.title( config.title )
		.content( txt, true, isIfr )
		.button( config.button )
		.size( config.width, config.height )
		.position( config.left, config.top )
		.time( config.time )
		[config.show?'show':'hide'](true).zindex();
		
		config.lock && that.lock();
		
		that._addEvent();
		that._ie6PngFix();
		
		_box = null;
		
		// config.init
		if( !isIfr && config.init )
		    config.init.call( that, window );
		if(config.maxState)
		that.max();
		return that;
	},
	
	/**
	 * 
	 * @param	{String, Boolean}	. false
	 * @return	{this}	
	 */
	title: function( text )
	{
		if( text === undefined ) return this;
		
		var DOM = this.DOM,
			border = DOM.border,
			title = DOM.title[0],
			className = 'ui_state_tips';
			
		if( text === false )
		{
			title.style.display = 'none';
			title.innerHTML = '';
			border.addClass(className);
		}
		else
		{
			title.style.display = '';
			title.innerHTML = text;
			border.removeClass(className);
		};
		
		return this;
	},
	
	/*!
	 * 
	 * @param	{String}	 (3url:)
	 * @param   {Boolean}   
	 * @param   {Boolean}   iframe
	 * @return	{this}		
	 */
	content: function( msg, add, frm )
	{
	    if( msg === undefined ) return this;
		
		var that = this, DOM = that.DOM,
		    wrap = DOM.wrap[0],
			width = wrap.offsetWidth,
			height = wrap.offsetHeight,
			left = parseInt(wrap.style.left),
			top = parseInt(wrap.style.top),
			cssWidth = wrap.style.width,
			$content = DOM.content,
			loading = lhgdialog.setting.content;
		
		// 3'url:'
		if( frm )
		{
			$content[0].innerHTML = loading;
			that._iframe( msg.split('url:')[1] );
		}
		else
			$content.html( msg );
		
		// 
		if( !add )
		{
			width = wrap.offsetWidth - width;
			height = wrap.offsetHeight - height;
			left = left - width / 2;
			top = top - height / 2;
			wrap.style.left = Math.max(left, 0) + 'px';
			wrap.style.top = Math.max(top, 0) + 'px';
			
			if( cssWidth && cssWidth !== 'auto' )
				wrap.style.width = wrap.offsetWidth + 'px';
			
			that._autoPositionType();
		}
		
		that._ie6SelectFix();
		
		return that;
	},
	
	/**
	 * 
	 * @example
		button({
			name: 'login',
			callback: function(){},
			disabled: false,
			focus: true
		}, .., ..)
	 */
	button: function()
	{
		var that = this, ags = arguments,
			buttons = that.DOM.buttons[0],
			focusButton = 'ui_state_highlight',
			listeners = that._listeners = that._listeners || {},
			list = $.isArray(ags[0]) ? ags[0] : [].slice.call(ags);
	
		$.each(list, function(i,obj){
		    var name = obj.name,
			    isNewButton = !listeners[name],
				button = !isNewButton ?
					listeners[name].elem :
					_doc.createElement('input');
			
			if( !listeners[name] ) listeners[name] = {};
			if( obj.callback ) listeners[name].callback = obj.callback;
			
			if( obj.focus )
			{
			    that._focus && that._focus.removeClass(focusButton);
				that._focus = $(button).addClass(focusButton);
				that.focus();
			}
			
			button[_expando + 'callback'] = name;
			button.disabled = !!obj.disabled;

			if( isNewButton )
			{
				button.type = 'button';
				button.value = name;
				listeners[name].elem = button;
				buttons.appendChild(button);
			}
		});
		
		buttons.style.display = list.length ? '' : 'none';
		that._ie6SelectFix();
		
		return that;
	},
	
	/**
	 *	
	 *	@param	{Number, String}	
	 *	@param	{Number, String}	
	 */
	size: function( width, height )
	{
		var that = this, DOM = that.DOM,
			wrap = DOM.wrap[0],
			wrapStyle = wrap.style,
			style = DOM.main[0].style;
		
		if( width )
		{
			wrapStyle.width = 'auto';
			
			if( typeof width === 'number' )
				style.width = width + 'px';
			else if( typeof width === 'string' )
				style.width = width;
			
			if( width !== 'auto' ) // 
			    wrapStyle.width = wrap.offsetWidth + 'px';
		}
		
		if( height )
		{
			if( typeof height === 'number' )
				style.height = height + 'px';
			else if( typeof height === 'string' )
				style.height = height;
		};
		
		that._ie6SelectFix();
		
		return that;
	},
	
	/**
	 * ()
	 * @param	{Number, String}
	 * @param	{Number, String}
	 */
	position: function( left, top )
	{
		var that = this,
			config = that.config,
			wrap = that.DOM.wrap[0],
			isFixed = _ie6 ? false : config.fixed,
			ie6Fixed = _ie6 && config.fixed,
			docLeft = _$doc.scrollLeft(),
			docTop = _$doc.scrollTop(),
			dl = isFixed ? 0 : docLeft,
			dt = isFixed ? 0 : docTop,
			ww = _$top.width(),
			wh = _$top.height(),
			ow = wrap.offsetWidth,
			oh = wrap.offsetHeight,
			style = wrap.style;
		
		if( left || left === 0 )
		{
			that._left = left.toString().indexOf('%') !== -1 ? left : null;
			left = that._toNumber(left, ww - ow);
			
			if(typeof left === 'number')
			{
				left = ie6Fixed ? (left += docLeft) : left + dl;
				style.left = Math.max(left,dl) + 'px';
			}
			else if(typeof left === 'string')
				style.left = left;
		}
		
		if( top || top === 0 )
		{
			that._top = top.toString().indexOf('%') !== -1 ? top : null;
			if(wh != undefined)
				top = that._toNumber(top, wh - oh);
			
			if(typeof top === 'number')
			{
				top = ie6Fixed ? (top += docTop) : top + dt;
				
				style.top = Math.max(top,dt) + 'px';
			}
			else if(typeof top === 'string')
				style.top = top;
		}
		
		if( left !== undefined && top !== undefined )
			that._autoPositionType();
		
		return that;
	},
	
	/*!  */
	show: function( args )
	{
		this.DOM.wrap[0].style.visibility = 'visible';
		if( !args && this._lock )
		    $('#ldg_lockmask',_doc)[0].style.display = '';
		return this;
	},
	
	/*!  */
	hide: function( args )
	{
		this.DOM.wrap[0].style.visibility = 'hidden';
		if( !args && this._lock )
		    $('#ldg_lockmask',_doc)[0].style.display = 'none';
		return this;
	},
	
	/*!  */
	close: function()
	{
		var that = this, DOM = that.DOM,
			wrap = DOM.wrap,
			list = lhgdialog.list,
			fn = that.config.close;
		
		that.time();
		
		// iframe
		if( that.iframe )
		{
			if( typeof  fn === 'function' && fn.call(that, that.iframe.contentWindow, window) === false )
			    return that;
			
			// iframeIE67input
			// IEiframeiframesrc
			$(that.iframe).unbind('load',that._fmLoad).attr('src','about:blank').remove();
			
			DOM.content.removeClass('ui_state_full');
			if( that._frmTimer ) clearTimeout(that._frmTimer);
		}
		else
		{
		    if( typeof fn === 'function' && fn.call(that, window) === false )
			    return that;
		}
		
		that.unlock();
		
		if( that._minState )
		{
		    DOM.main[0].style.display = '';
			DOM.buttons[0].style.display = '';
			DOM.dialog[0].style.width = '';
		}
		
		if( that._maxState )
		{
			_$html.removeClass('ui_lock_scroll');
		    DOM.res[0].style.display = 'none';
		}
		
		// 
		wrap[0].className = DOM.wrap[0].style.cssText = '';
		wrap[0].style.display = 'none';
		DOM.border.removeClass('ui_state_focus');
		DOM.title[0].innerHTML = '';
		DOM.content.html('');
		DOM.buttons[0].innerHTML = '';
		
		if( lhgdialog.focus === that ) lhgdialog.focus = null;
		
		delete list[that.config.id];
		that._removeEvent();
		that.hide(true)._setAbsolute();
		
		// this.DOM
		for( var i in that )
		{
			if(that.hasOwnProperty(i) && i !== 'DOM') delete that[i];
		};
		
		// HTMLElement
		_box ? wrap.remove() : _box = that;
		
		return that;
	},
	
	/*!
	 * 
	 * @param	{Number}	, 
	 * @param   {Function}  
	 */
	time: function( second, callback )
	{
		var that = this,
			cancel = that.config.cancelVal,
			timer = that._timer;
			
		timer && clearTimeout(timer);
		if( callback ) callback.call(that);
		
		if(second)
		{
			that._timer = setTimeout(function(){
				that._click(cancel);
			}, 1000 * second);
		}
		
		return that;
	},
	
	/**
	 * 
	 * @param	{Object, window}
	 * @param	{String, }
	 */
	reload: function( win, url )
	{
	    win = win || window;
		
		try{
		    win.location.href = url ? url : win.location.href;
		}
		catch(e){ // 
			url = this.iframe.src;
			$(this.iframe).attr('src', url);
		};
		
		return this;
	},
	
	/*!  */
	zindex: function()
	{
		var that = this, DOM = that.DOM,
		    load = that._load,
			top = lhgdialog.focus;
			
			if(window.top.global_top_zIndex)
		    {
			   index = window.top.global_top_zIndex ++;
			}
			else
		  {
			   index = window.top.global_top_zIndex = 1000;	
			}

			//index = lhgdialog.setting.zIndex++;
		
		// 
		DOM.wrap[0].style.zIndex = index;
		
		// 
		top && top.DOM.border.removeClass('ui_state_focus');
		lhgdialog.focus = that;
		DOM.border.addClass('ui_state_focus');
		
		// iframe
		// 
		if( load && load.style.zIndex )
		    load.style.display = 'none';
		if( top && top !== that && top.iframe )
		    top._load.style.display = '';
		
		return that;
	},
	
	/*!  */
	focus: function()
	{
	    try{
		    elemFocus = this._focus && this._focus[0] || this.DOM.close[0];
			elemFocus && elemFocus.focus();
		}catch(e){};
		
		return this;
	},
	
	/*!  */
	lock: function()
	{
		var that = this, frm,
		    index = window.top.global_top_zIndex - 1,
			config = that.config,
			mask = $('#ldg_lockmask',_doc)[0],
			style = mask ? mask.style : '',
			positionType = _ie6 ? 'absolute' : 'fixed';
		
		if( !mask )
		{
			frm = '<iframe src="about:blank" style="width:100%;height:100%;position:absolute;' +
			    'top:0;left:0;z-index:-1;filter:alpha(opacity=0)"></iframe>';
				
			mask = _doc.createElement('div');
			mask.id = 'ldg_lockmask';
			mask.style.cssText = 'position:' + positionType + ';left:0;top:0;width:100%;height:100%;overflow:hidden;background:' + config.background + ';';
			
			$(mask).css({opacity:config.opacity});
			
			style = mask.style;
			if( _ie6 ) mask.innerHTML = frm;
			
			_doc.body.appendChild( mask );
		}
		
		if( positionType === 'absolute' )
		{
		    style.width = _$top.width();
			style.height = _$top.height();
			style.top = _$doc.scrollTop();
			style.left = _$doc.scrollLeft();
			
			that._setFixed( mask );
		}

		style.zIndex = index;
		style.display = '';
		
		that.zindex();
		that.DOM.border.addClass('ui_state_lock');
		
		that._lock = true;
			
		return that;
	},
	
	/*!  */
	unlock: function()
	{
		var that = this,
		    config = that.config,
			mask = $('#ldg_lockmask',_doc)[0];
		
		/*if( mask && that._lock )
		{
		    // 
			if( config.parent && config.parent._lock )
			{
			    var index = config.parent.DOM.wrap[0].style.zIndex;
				mask.style.zIndex = parseInt(index,10) - 1;
			}
			else
			    mask.style.display = 'none';
			
			that.DOM.border.removeClass('ui_state_lock');
		}*/
		
		if( mask && that._lock )
		{
		    // 
			if( config.parent  )
			{
			    var index = config.parent.DOM.wrap[0].style.zIndex;
				if(index !=0)
			    mask.style.zIndex = parseInt(index,10) - 1;
			}
			else
			    mask.style.display = 'none';
			
			that.DOM.border.removeClass('ui_state_lock');
		}
		
		return that;
	},
	
	/*!  */
	max: function()
	{
		if( !this.config.max ) return this;
		
		var that = this, maxSize,
		    DOM = that.DOM,
			wrapStyle = DOM.wrap[0].style,
			mainStyle = DOM.main[0].style,
			rbStyle = DOM.rb[0].style,
			titleStyle = DOM.title[0].style,
			config = that.config,
		    top = _$doc.scrollTop(),
		    left = _$doc.scrollLeft();
		
		if( !that._maxState )
		{
		
			_$html.addClass('ui_lock_scroll');
			
			if( that._minState )
			    that.min();
			
			// 
			that._or = {
				t: wrapStyle.top,
				l: wrapStyle.left,
				w: mainStyle.width,
				h: mainStyle.height,
				d: config.drag,
				r: config.resize,
				rc: rbStyle.cursor,
				tc: titleStyle.cursor
			};
			
			wrapStyle.top = top + 'px';
			wrapStyle.left = left + 'px';
			
			maxSize = that._maxSize();
			that.size( maxSize.w, maxSize.h )._setAbsolute();
			
			config.drag = false;
			config.resize = false;
			rbStyle.cursor = 'auto';
			titleStyle.cursor = 'auto';
			
			DOM.max[0].style.display = 'none';
			DOM.res[0].style.display = 'inline-block';
			
			that._maxState = true;
		}
		else
		{
			_$html.removeClass('ui_lock_scroll');
			
			wrapStyle.top = that._or.t;
			wrapStyle.left = that._or.l;
			that.size( that._or.w, that._or.h )._autoPositionType();
			config.drag = that._or.d;
		    config.resize = that._or.r;
		    rbStyle.cursor = that._or.rc;
		    titleStyle.cursor = that._or.tc;
		
		    DOM.res[0].style.display = 'none';
			DOM.max[0].style.display = 'inline-block';
			
			delete that._or;
			
			that._maxState = false;
		}
		
		return that;
	},
	
	/*!  */
	_maxSize: function()
	{
	    var that = this, DOM = that.DOM,
		    wrap = DOM.wrap[0],
			main = DOM.main[0],
			maxWidth, maxHeight;
			
		maxWidth = _$top.width() - wrap.offsetWidth + main.offsetWidth;
		maxHeight = _$top.height() - wrap.offsetHeight + main.offsetHeight;
		
		return { w: maxWidth, h: maxHeight };
	},
	
	/*!  */
	min: function()
	{
	    if( !this.config.min ) return this;
		
		var that = this,
		    DOM = that.DOM,
			main = DOM.main[0].style,
			buttons = DOM.buttons[0].style,
			dialog = DOM.dialog[0].style,
			rb = DOM.rb[0].style.cursor,
			resize = that.config.resize;
			
		if( !that._minState )
		{
		    if( that._maxState )
				that.max();
			
			that._minRz = {rzs:resize,btn:buttons.display};
			main.display = 'none';
		    buttons.display = 'none';
		    dialog.width = main.width;
			rb.cursor = 'auto';
			resize = false;
		
		    that._minState = true;
		}
		else
		{
		    main.display = '';
			buttons.display = that._minRz.btn;
			dialog.width = '';
			resize = that._minRz;
			rb.cursor = that._minRz.rzs ? 'se-resize' : 'auto';
			
			delete that._minRz;
			
			that._minState = false;
		}
		
		that._ie6SelectFix();
		
		return that;
	},
	
	/*!
	 * idiframewindow
	 * @param {String} id
	 * @param {String} id
	 *        1false
	 * @return {Object|null}
	 */
	get: function( id, object )
	{
		if( lhgdialog.list[id] )
		{
			if( object === 1 )
			    return lhgdialog.list[id];
			else
			    return lhgdialog.list[id].iwin || null;
		}
		
		return null;
	},
	
	/*!
	 * iframe
	 */
	_iframe: function( url )
	{
	    var that = this, iframe, $iframe, iwin, $idoc, $ibody, iWidth, iHeight,
		    $content = that.DOM.content,
			config = that.config,
			loading = that._load = $('.ui_loading',$content[0])[0],
		    initCss = 'position:absolute;left:-9999em;border:none 0;background:transparent',
		    loadCss = 'width:100%;height:100%;border:none 0;';
		
		// . true
		if( config.cache === false )
		{
			var ts = (new Date).getTime(),
				ret = url.replace(/([?&])_=[^&]*/, '$1_=' + ts );
			url = ret + ((ret === url) ? (/\?/.test(url) ? '&' : '?') + '_=' + ts : '');
		}
			
		iframe = that.iframe = _doc.createElement('iframe');
		iframe.name = config.id;
		iframe.style.cssText = initCss;
		iframe.setAttribute('frameborder', 0, 0);
		
		$iframe = $(iframe);
		$content[0].appendChild( iframe );
		
		// iframesrcIE6BUG
		that._frmTimer = setTimeout(function(){
		    $iframe.attr('src', url);
		}, 1);
		
		// iframe
		var load = that._fmLoad = function()
		{
			$content.addClass('ui_state_full');
			
			// iframe
			// loadingfocus
			var DOM = that.DOM, ltSize,
			    lt = DOM.lt[0].offsetHeight,
				main = DOM.main[0].style;
				
			loading.style.cssText = 'display:none;position:absolute;background:#FFF;opacity:0;' + 
			    'filter:alpha(opacity=0);z-index:1;width:' + main.width + ';height:' + main.height + ';';
			// _dragEvent.onmove
			
			try{
			    iwin = that.iwin = iframe.contentWindow; // iwinwindow
				$idoc = $(iwin.document);
				$ibody = $(iwin.document.body);
			}catch(e){// 
			    iframe.style.cssText = loadCss;
				return;
			}
			// iframe
			iWidth = config.width === 'auto'
			? $idoc.width() + (_ie6 ? 0 : parseInt($ibody.css('marginLeft')))
			: config.width;
			
			iHeight = config.height === 'auto'
			? $idoc.height() : config.height;
			
			// iframe
			setTimeout(function(){
			    iframe.style.cssText = loadCss;
			},0);// setTimeout: IE6~7
		
			// 
			if( !that._maxState )
			{
			    that.size( iWidth, iHeight )
			    .position( config.left, config.top );
			}
			
			// loading'auto'
			loading.style.width = main.width;
			loading.style.height = main.height;
			
			config.init && config.init.call( that, iwin, _top );
		};
		
		// iframeapi
		that.iframe.api = that;
		$iframe.bind( 'load', load );
	},
	
	/*!  */
	_getDOM: function()
	{
		var wrap = _doc.createElement('div'),
		    body = _doc.body;
		
		wrap.style.cssText = 'position:absolute;visibility:hidden;';
		wrap.innerHTML = lhgdialog.templates;
		body.insertBefore(wrap, body.firstChild);
		
        var name, i = 0,
			DOM = { wrap: $(wrap) },
			els = wrap.getElementsByTagName('*'),
			len = els.length;
			
		for( ; i < len; i ++ )
		{
			name = els[i].className.split('ui_')[1];
			if(name) DOM[name] = $(els[i]);
		};
		
		return DOM;
	},
	
	/*!
	 * px% ()
	 * 
	 */
	_toNumber: function( thisValue, maxValue )
	{
		if( !thisValue && thisValue !== 0 || typeof thisValue === 'number')
			return thisValue;
		
		if( thisValue.indexOf('%') !== -1 )
			thisValue = parseInt(maxValue * thisValue.split('%')[0] / 100);
		
		return thisValue;
	},
	
	/*! IE6 CSSPNG */
	_ie6PngFix: _ie6 ? function(){
		var i = 0, elem, png, pngPath, runtimeStyle,
			path = lhgdialog.setting.path + '/skins/',
			list = this.DOM.wrap[0].getElementsByTagName('*');
		
		for( ; i < list.length; i ++ )
		{
			elem = list[i];
			png = elem.currentStyle['png'];
			if( png )
			{
				pngPath = path + png;
				runtimeStyle = elem.runtimeStyle;
				runtimeStyle.backgroundImage = 'none';
				runtimeStyle.filter = "progid:DXImageTransform.Microsoft." +
					"AlphaImageLoader(src='" + pngPath + "',sizingMethod='scale')";
			};
		}
	} : _noop,
	
	/*! IE6 */
	_ie6SelectFix: _ie6 ? function(){
		var $wrap = this.DOM.wrap,
			wrap = $wrap[0],
			expando = _expando + 'iframeMask',
			iframe = $wrap[expando],
			width = wrap.offsetWidth,
			height = wrap.offsetHeight;

		width = width + 'px';
		height = height + 'px';
		if(iframe)
		{
			iframe.style.width = width;
			iframe.style.height = height;
		}else{
			iframe = wrap.appendChild(_doc.createElement('iframe'));
			$wrap[expando] = iframe;
			iframe.src = 'about:blank';
			iframe.style.cssText = 'position:absolute;z-index:-1;left:0;top:0;'
			+ 'filter:alpha(opacity=0);width:' + width + ';height:' + height;
		}
	} : _noop,
	
	/*!  */
	_autoPositionType: function()
	{
		this[this.config.fixed ? '_setFixed' : '_setAbsolute']();
	},
	
	/*! 
	 * IE6 Fixed @see: http://www.planeart.cn/?p=877
	 */
	_setFixed: function( el )
	{
		var style = el ? el.style : this.DOM.wrap[0].style;
		
		if( _ie6 )
		{
			var sLeft = _$doc.scrollLeft(),
				sTop = _$doc.scrollTop(),
				left = parseInt(style.left) - sLeft,
				top = parseInt(style.top) - sTop,
				txt = 'this.ownerDocument.documentElement';
			
			this._setAbsolute();
			
			style.setExpression( 'left', txt + '.scrollLeft +' + left );
			style.setExpression( 'top', txt + '.scrollTop +' + top );
		}
		else
			style.position = 'fixed';
	},
	
	/*!  */
	_setAbsolute: function()
	{
		var style = this.DOM.wrap[0].style;
			
		if(_ie6)
		{
			style.removeExpression('left');
			style.removeExpression('top');
		}

		style.position = 'absolute';
	},
	
	/*!  */
	_click: function( name )
	{ 
		var that = this,
			fn = that._listeners[name] && that._listeners[name].callback;
		return typeof fn !== 'function' || fn.call(that, window) !== false ?
			that.close() : that;
	},
	
	/*!  */
	_reset: function( test )
	{
		var newSize,
			that = this,
			tw = _$top.width(),
			tt = _$top.height(),
			oldSize = that._winSize || tw * tt,
			oldWidth = that._lockDocW || tw,
			left = that._left,
			top = that._top;
		
		if(test)
		{
			//IE6
			if( that._lock && _ie6 )
			    $('#ldg_lockmask',_doc).css({ width:tw + 'px', height:tt + 'px' });
			
			newWidth = that._lockDocW = tw;
			//IE6~7 window.onresize bug
			newSize = that._winSize =  tw * tt;
			if( oldSize === newSize ) return;
		};
		
		if( that._maxState )
		{
		    var size = that._maxSize();
			that.size( size.w, size.h );
		}
		
		//IE6~8IEresizeBUG 
		if( test && Math.abs(oldWidth - newWidth) === 17 ) return;
		
		if( left || top )
			that.position( left, top );
	},
	
	/*!  */
	_addEvent: function()
	{
		var resizeTimer,
			that = this,
			config = that.config,
			DOM = that.DOM;
		
		// 
		that._winResize = function()
		{
			resizeTimer && clearTimeout(resizeTimer);
			resizeTimer = setTimeout(function()
			{
				that._reset(_ie);
			}, 140);
		};
		_$top.bind('resize', that._winResize);
		
		// 
		DOM.wrap.bind('click', function(event){
			var target = event.target, callbackID;
			
			if( target.disabled ) return false; // IE BUG
			
			if( target === DOM.close[0] )
			{
				that._click(config.cancelVal);
				return false;
			}
			else if( target === DOM.max[0] || target === DOM.res[0] || target === DOM.max_b[0]
			    || target === DOM.res_b[0] || target === DOM.res_t[0] )
			{
			    that.max();
				return false;
			}
			else if( target === DOM.min[0] || target === DOM.min_b[0] )
			{
				that.min();
				return false;
			}
			else
			{
				callbackID = target[_expando + 'callback'];
				callbackID && that._click(callbackID);
			}
		}).bind('mousedown',function(event){
		    that.zindex();
			
			var target = event.target;
			
			if( config.drag !== false && target === DOM.title[0]
			|| config.resize !== false && target === DOM.rb[0] )
			{
				_use(event);
				return false;
			}
		});
		
		// 
		if( config.max )
		    DOM.title.bind('dblclick',function(){ that.max(); return false; });
	},
	
	/*!   */
	_removeEvent: function()
	{
		var that = this,
			DOM = that.DOM;
		
		DOM.wrap.unbind();
		DOM.title.unbind();
		_$top.unbind('resize', that._winResize);
	}
};

lhgdialog.fn._init.prototype = lhgdialog.fn;

/*! jQ */
$.fn.dialog = function()
{
	var config = arguments;
	this.bind('click', function(){
		lhgdialog.apply(this, config);
		return false;
	});
	return this;
};

/*!  */
lhgdialog.focus = null;

/*!  */
lhgdialog.list = {};

/*!
 * 
 * 
 * unbind
 * 
 */
onKeyDown = function(event)
{
	var target = event.target,
		api = lhgdialog.focus,
		keyCode = event.keyCode;

	if( !api || !api.config.esc ) return;
		
	keyCode === 27 && api._click(api.config.cancelVal);
};

_$doc.bind('keydown',onKeyDown);

/*!
 * 
 * 
 */
_top != window && $(window).bind('unload',function()
{
    var list = lhgdialog.list;
	for( var i in list )
	{
	    if(list[i])
		    list[i].close();
	}
	_box && _box.DOM.wrap.remove();
	
	_$doc.unbind('keydown',onKeyDown);
	// 
	delete lhgdialog[_expando + '_data'];
	
	$('#ldg_lockmask',_doc)[0] && $('#ldg_lockmask',_doc).remove();
	$('#ldg_dragmask',_doc)[0] && $('#ldg_dragmask',_doc).remove();
});

/**
 * 
 * @see		http://www.planeart.cn/?p=1554
 * @param	{String}	
 * @param	{Any}		()
 */
lhgdialog[_expando + '_data'] = {};

lhgdialog.data = function( name, value )
{
    var cache = lhgdialog[_expando+'_data'];
	
	if( value !== undefined )
	    cache[name] = value;
	else
	    return cache[name];
	
	return cache;
};

/**
 * 
 * @param	{String}	
 */
lhgdialog.removeDate = function( name )
{
    var cache = lhgdialog[_expando+'_data'];
	if( cache && cache[name] ) delete cache[name];
};

/*! 
 * DOM
 */
$(function(){
	// 
	setTimeout(function()
	{
	    if(_count) return;
		lhgdialog({left:'-9999em',time:9,fixed:false,lock:false});
	},150);
	
	_ie6 && (function(){
		var bg = 'backgroundAttachment';
		if( _$html.css(bg) !== 'fixed' && $(_doc.body).css(bg) !== 'fixed' )
		{
			_$html.css({
				zoom: 1,// body
				backgroundImage: 'url(about:blank)',
				backgroundAttachment: 'fixed'
			});
		}
	})();
	
	// lhgdialog
	// iframe
	lhgdialog.setting.extendDrag &&
	(function(dragEvent){
	    var mask = _doc.createElement('div'),
		    style = mask.style,
			positionType = _ie6 ? 'absolute' : 'fixed';
		mask.id = 'ldg_dragmask';
		
		style.cssText = 'display:none;position:' + positionType + ';left:0;top:0;width:100%;height:100%;'
		+ 'cursor:move;filter:alpha(opacity=0);opacity:0;background:#FFF;pointer-events:none;';
		
		_doc.body.appendChild(mask);
		
		dragEvent._start = dragEvent.start;
		dragEvent._end = dragEvent.end;
		
		dragEvent.start = function()
		{
			var api = lhgdialog.focus,
				main = api.DOM.main[0],
				iframe = api.iframe;
			
			dragEvent._start.apply(this, arguments);
			style.display = 'block';
			style.zIndex = lhgdialog.setting.zIndex + 3;
			
			if(positionType === 'absolute')
			{
				style.width = '100%';
				style.height = _$top.height() + 'px';
				style.left = _$doc.scrollLeft() + 'px';
				style.top = _$doc.scrollTop() + 'px';
			};
			
			if( iframe && main.offsetWidth * main.offsetHeight > 307200 )
				main.style.visibility = 'hidden';
		};
		
		dragEvent.end = function()
		{
			var api = lhgdialog.focus;
			dragEvent._end.apply(this, arguments);
			style.display = 'none';
			if(api) api.DOM.main[0].style.visibility = 'visible';
		};
	})(lhgdialog.dragEvent);
});

/*!
 *------------------------------------------------
 * -
 *------------------------------------------------
 */
var _use, _isSetCapture = 'setCapture' in _root,
	_isLosecapture = 'onlosecapture' in _root;

lhgdialog.dragEvent =
{
    onstart: _noop,
	start: function(event)
	{
	    var that = lhgdialog.dragEvent;
		
		_$doc
		.bind( 'mousemove', that.move )
		.bind( 'mouseup', that.end );
		
		that._sClientX = event.clientX;
		that._sClientY = event.clientY;
		that.onstart( event.clientX, event.clientY );
		
		return false;
	},
	
	onmove: _noop,
	move: function(event)
	{
	    var that = lhgdialog.dragEvent;
		
		that.onmove(
		    event.clientX - that._sClientX,
			event.clientY - that._sClientY
		);
		
		return false;
	},
	
	onend: _noop,
	end: function(event)
	{
	    var that = lhgdialog.dragEvent;
		
		_$doc
		.unbind('mousemove', that.move)
		.unbind('mouseup', that.end);
		
		that.onend(  event.clientX, event.clientY );
		return false;
	}
};

_use = function(event)
{
	var limit, startWidth, startHeight, startLeft, startTop, isResize,
		api = lhgdialog.focus,
		config = api.config,
		DOM = api.DOM,
		wrap = DOM.wrap[0],
		title = DOM.title,
		main = DOM.main[0],
		_dragEvent = lhgdialog.dragEvent,
	
	// 
	clsSelect = 'getSelection' in _top ?
	function(){
		_top.getSelection().removeAllRanges();
	}:function(){
		try{_doc.selection.empty();}catch(e){};
	};
	
	// 
	_dragEvent.onstart = function( x, y )
	{
		if( isResize )
		{
			startWidth = main.offsetWidth;
			startHeight = main.offsetHeight;
		}
		else
		{
			startLeft = wrap.offsetLeft;
			startTop = wrap.offsetTop;
		};
		
		_$doc.bind( 'dblclick', _dragEvent.end );
		
		!_ie6 && _isLosecapture
		? title.bind('losecapture',_dragEvent.end )
		: _$top.bind('blur',_dragEvent.end);
		
		_isSetCapture && title[0].setCapture();
		
		DOM.border.addClass('ui_state_drag');
		api.focus();
	};
	
	// 
	_dragEvent.onmove = function( x, y )
	{
		if( isResize )
		{
			var wrapStyle = wrap.style,
				style = main.style,
				width = x + startWidth,
				height = y + startHeight;
			
			wrapStyle.width = 'auto';
			config.width = style.width = Math.max(0,width) + 'px';
			wrapStyle.width = wrap.offsetWidth + 'px';
			
			config.height = style.height = Math.max(0,height) + 'px';
			api._ie6SelectFix();
		    // loadingloading
			api._load && $(api._load).css({width:style.width, height:style.height});
		}
		else
		{
			var style = wrap.style,
				left = x + startLeft,
				top = y + startTop;

			config.left = Math.max( limit.minX, Math.min(limit.maxX,left) );
			config.top = Math.max( limit.minY, Math.min(limit.maxY,top) );
			style.left = config.left + 'px';
			style.top = config.top + 'px';
		}
			
		clsSelect();
	};
	
	// 
	_dragEvent.onend = function( x, y )
	{
		_$doc.unbind('dblclick',_dragEvent.end);
		
		!_ie6 && _isLosecapture
		? title.unbind('losecapture',_dragEvent.end)
		: _$top.unbind('blur',_dragEvent.end);
		
		_isSetCapture && title[0].releaseCapture();
		
		_ie6 && api._autoPositionType();
		
		DOM.border.removeClass('ui_state_drag');
	};
	
	isResize = event.target === DOM.rb[0] ? true : false;
	
	limit = (function()
	{
		var	fixed = wrap.style.position === 'fixed',
			ow = wrap.offsetWidth,
			// 
			oh = title[0].offsetHeight || 20,
			ww = _$top.width(),
			wh = _$top.height(),
			dl = fixed ? 0 : _$doc.scrollLeft(),
			dt = fixed ? 0 : _$doc.scrollTop();
		    // ()	
		    maxX = ww - ow + dl;
		    maxY = wh - oh + dt;
		
		return {
			minX: dl,
			minY: dt,
			maxX: maxX,
			maxY: maxY
		};
	})();
	
	_dragEvent.start(event);  
};

lhgdialog.templates =
'<table class="ui_border">' +
	'<tbody>' +
		'<tr>' +
			'<td class="ui_lt"></td>' +
			'<td class="ui_t"></td>' +
			'<td class="ui_rt"></td>' +
		'</tr>' +
		'<tr>' +
			'<td class="ui_l"></td>' +
			'<td class="ui_c">' +
				'<div class="ui_inner">' +
				'<table class="ui_dialog">' +
					'<tbody>' +
						'<tr>' +
							'<td colspan="2">' +
								'<div class="ui_title_bar">' +
									'<div class="ui_title" unselectable="on"></div>' +
									'<div class="ui_title_buttons">' +
										'<a class="ui_min" href="javascript:void(0);" title="\u6700\u5C0F\u5316"><b class="ui_min_b"></b></a>' +
										'<a class="ui_max" href="javascript:void(0);" title="\u6700\u5927\u5316"><b class="ui_max_b"></b></a>' +
										'<a class="ui_res" href="javascript:void(0);" title="\u8FD8\u539F"><b class="ui_res_b"></b><b class="ui_res_t"></b></a>' +
										'<a class="ui_close" href="javascript:void(0);" title="\u5173\u95ED(esc\u952E)">\xd7</a>' +
									'</div>' +
								'</div>' +
							'</td>' +
						'</tr>' +
						'<tr>' +
							'<td class="ui_icon">' +
								'<img  class="ui_icon_bg"/>' + 
							'</td>' +
							'<td class="ui_main">' +
								'<div class="ui_content"></div>' +
							'</td>' +
						'</tr>' +
						'<tr>' +
							'<td colspan="2">' +
								'<div class="ui_buttons"></div>' +
							'</td>' +
						'</tr>' +
					'</tbody>' +
				'</table>' +
				'</div>' +
			'</td>' +
			'<td class="ui_r"></td>' +
		'</tr>' +
		'<tr>' +
			'<td class="ui_lb"></td>' +
			'<td class="ui_b"></td>' +
			'<td class="ui_rb"></td>' +
		'</tr>' +
	'</tbody>' +
'</table>';

/*! lhgdialog  */
lhgdialog.setting =
{
    content: '<div class="ui_loading"><span>loading...</span></div>',
	title: '\u89C6\u7A97 ',     // ,''
	button: null,				// 
	ok: null,					// 
	cancel: null,				// 
	init: null,					// 
	close: null,				// 
	okVal: '\u786E\u5B9A',		// ,''
	cancelVal: '\u53D6\u6D88',	// ,''
	skin: '',					// 
	esc: true,					// Esc
	show: true,					// 
	width: 'auto',				// 
	height: 'auto',				// 
	icon: null,					// 
	path: _path,                // lhgdialog
	lock: true,				// 
	parent: null,               // 
	background: '#DCE2F1',		// 
	opacity: .6,				// 
	padding: '10px',		    // 
	fixed: false,				// 
	left: '50%',				// X
	top: '38.2%',				// Y
	max: true,                  // 
	min: true,                  // 
	zIndex: window.top.global_top_zIndex,				// ()
	resize: true,				// 
	drag: true, 				// 
	cache: true,                // 
	extendDrag: false,           // lhgdialog
	currentwindow:null,     //
	maxState:false          // 
	
};


		
window.lhgdialog = $.dialog = lhgdialog;

})( this.jQuery||this.lhgcore, this );

/*!
 *------------------------------------------------
 * 
 *------------------------------------------------
 */
;(function( $, lhgdialog, undefined ){

var _zIndex = function()
{
    return lhgdialog.setting.zIndex;
};

/**
 * 
 * @param	{String}	
 */
lhgdialog.alert = function( content, callback, parent,title )
{
	return lhgdialog({
		title: title,
		id: 'Alert',
		zIndex: _zIndex(),
		icon: 'alert.gif',
		fixed: true,
		lock: true,
		content: content,
		ok: true,
		resize: false,
		close: callback,
		parent: parent || null
	});
};

/**
 * 
 * @param	{String}	
 * @param	{Function}	
 * @param	{Function}	
 */
lhgdialog.confirm = function( content, yes, no, parent,title )
{
	return lhgdialog({
		title: title,
		id: 'confirm.gif',
		zIndex: _zIndex(),
		icon: 'confirm.gif',
		fixed: true,
		lock: true,
		content: content,
		resize: false,
		parent: parent || null,
		ok: function(here){
			return yes.call(this, here);
		},
		cancel: function(here){
			return no && no.call(this, here);
		}
	});
};

/**
 * 
 * @param	{String}	
 * @param	{Function}	. 
 * @param	{String}	
 */
lhgdialog.prompt = function( content, yes, value, parent )
{
	value = value || '';
	var input;
	
	return lhgdialog({
		title: '',
		id: 'Prompt',
		zIndex: _zIndex(),
		icon: 'prompt.gif',
		fixed: true,
		lock: true,
		parent: parent || null,
		content: [
			'<div style="margin-bottom:5px;font-size:12px">',
				content,
			'</div>',
			'<div>',
				'<input value="',
					value,
				'" style="width:18em;padding:6px 4px" />',
			'</div>'
			].join(''),
		init: function(){
			input = this.DOM.content[0].getElementsByTagName('input')[0];
			input.select();
			input.focus();
		},
		ok: function(here){
			return yes && yes.call(this, input.value, here);
		},
		cancel: true
	});
};

/**
 * 
 * @param	{String}	
 * @param   {Number}     (1.5)
 * @param	{String}	 ()
 * @param   {Function}  
 */
lhgdialog.tips = function( content, time, icon, callback )
{
	var reIcon = icon ? function(){
	    this.DOM.icon_bg[0].src = this.config.path + 'skins/icons/' + icon;
		this.DOM.icon[0].style.display = '';
		if( callback ) this.config.close = callback;
	} : function(){ this.DOM.icon[0].style.display = 'none'; };
	
	return lhgdialog({
		id: 'Tips',
		zIndex: _zIndex(),
		title: false,
		cancel: false,
		fixed: true,
		lock: false,
		resize: false,
		close: callback
	})
	.content(content)
	.time(time || 1.5, reIcon);
};

})( this.jQuery||this.lhgcore, this.lhgdialog );
