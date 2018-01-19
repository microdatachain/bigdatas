/**
 *  indexcustomtrue jspager.js
 * jsp form: <form name="com.frameworkset.goform"
 * method="post"></form>
 */

var  commitevent ;

 
/**
 * url: pages: maxPageItem: hasParam:
 * containerid, selector, url, pages, maxPageItem, hasParam, sortKey, desc, id,
 */
function goTo(gotopageid,gopageerror_msg,containerid, selector, url,
              pages,
              maxPageItem,
              hasParam, 
              sortKey,desc,id,
              formName,
              params,
              promotion)
{
	
	var regx = /\d+/g;
	var gotopage = document.getElementById(gotopageid);
	
    var goPage = "NaN";
    if(gotopage.length)
    {
	    for(var i = 0; i < gotopage.length; i ++ )
	    {
	    	// var temp = parseInt(gotopage[i].value);
	    	// if(!isNaN(temp))
	    	var temp = gotopage[i].value;
	    	if(regx.test(temp))
	        	goPage = temp;

	    }
	}
	else
	{
		// var temp = parseInt(gotopage.value);
		var temp = gotopage.value;
    	// if(!isNaN(temp))
	    	if(regx.test(temp))
	    	{
	        	goPage = temp;
		}

	}

    if(isNaN(goPage) || goPage.indexOf(".") != -1)
    {
		alert(gopageerror_msg);
		return;
    }
    // pages
    
    if(pages < goPage)
    {
    	goPage = pages;
    }
    
    var offset = (goPage - 1) * maxPageItem;
    var offset_key = "";
    var desc_key = "";
    var sort_key = "";
    if(id)
    {
        offset_key = id + ".offset";
        desc_key = id + ".desc"
        sort_key = id + ".sortKey";
    }
    else
    {
        offset_key = "pager.offset";
        desc_key = "desc";
        sort_key = "sortKey";
    }
    var forwardurl = url + (hasParam?"&":"?") + offset_key + "=" + offset;
    
    if(sortKey)
        forwardurl = forwardurl + "&" + sort_key + "=" + sortKey  + "&" + desc_key + "=" + desc;
    // params = (hasParam?"&":"") + offset_key + "=" + offset + "&"+ sort_key +
	// "=" + sortKey + "&" + desc_key + "=" + desc;
    params += (hasParam?"&":"") + offset_key + "=" + offset + "&"+ sort_key + "=" + sortKey  + "&" + desc_key + "=" + desc;
    
	var flag = false;
    if(commitevent){
        flag = eval(commitevent);
    }
	
    if(formName && flag)
        pageSubmit(formName,params,forwardurl,promotion,id);
    else
	{
	    var f = document.getElementById(formName);
	    
		f.action=forwardurl;
		f.submit();
		
	}

}

 /**
	 *  formName: params: forwardurl:
	 * promotion:
	 */
function pageSubmit(formName,params,forwardurl,promotion,id)
{
		 
    // 
    var paramsName = "PAGE_QUERY_STRING";
    if(id)
        paramsName = id + ".PAGE_QUERY_STRING";
	var flag = false;
    if(commitevent){
        flag = eval(commitevent);
    }
    if(promotion && flag)
    {
    	
        if(!confirm("\r\n''''."))
		{		
			
            location.replace(forwardurl);            
            // document.all.item(paramsName).value = params;
			// if(document.all.sendOrgId){
			// document.all.sendOrgId.value = "";
			// }
            // document.forms(formName).submit();
		}
        else
        {   
			if(commitevent){
				try
				{
					eval(commitevent);

				}
				catch(e)
				{
					alert("Error Command:\n\""+commitevent+"\".\n"+e.description);
				}
				
			}
			document.getElementById(paramsName).value = params;
			if(document.getElementById(formName).action && document.getElementById(formName).action != "")
			{
				document.getElementById(formName).submit();
			}
			else
			{
				document.getElementById(formName).action=forwardurl;
				document.getElementById(formName).submit();
			}
        }
    }
    else
    {		
    	document.getElementById(paramsName).value = params;
    	if(document.getElementById(formName).action && document.getElementById(formName).action != "")
		{
			document.getElementById(formName).submit();
		}
		else
		{
			document.getElementById(formName).action=forwardurl;
			document.getElementById(formName).submit();
		}
}
function keydowngo(event,goid)
{
	
	if(event.keyCode == 13)
	{
		
		document.getElementById(goid).onclick();
	}
	
	return false;
}

function checkAll(totalCheck,checkName){	
	var selectAll = document.getElementsByName(totalCheck);
	var o = document.getElementsByName(checkName);
	if(selectAll[0].checked==true){
		for (var i=0; i<o.length; i++){
	      if(!o[i].disabled){
	      		o[i].checked=true;
	      }
		}
	}else{
		  for (var i=0; i<o.length; i++){
	   	  	 o[i].checked=false;
	   	  }
	}
}

function checkOne(totalCheck,checkName){
	var selectAll = document.getElementsByName(totalCheck);
	var o = document.getElementsByName(checkName);
	var cbs = true;
	for (var i=0;i<o.length;i++){
		if(!o[i].disabled){
			if (o[i].checked==false){
				cbs=false;
			}
		}
	}
	if(cbs){
		selectAll[0].checked=true;
	}else{
		selectAll[0].checked=false;
	}
}		

function convertValue(object,needconvert)
{
	
	if(needconvert)
	{
    	var ret = object.replace(/:/g,"\\:");    	
      	ret = ret.replace(/\./g,"\\.");
      	ret = ret.replace(/\//g,"\\/");
      	ret = ret.replace(/\$/g,"\\$");
      	ret = ret.replace(/\[/g,"\\[");
      	ret = ret.replace(/\]/g,"\\]");
      	ret = ret.replace(/#/g,"\\#");
      	ret = ret.replace(/;/g,"\\;");
      	ret = ret.replace(/&/g,"\\&");
      	ret = ret.replace(/,/g,"\\,");
      	ret = ret.replace(/\+/g,"\\+");
      	ret = ret.replace(/\*/g,"\\*");
      	ret = ret.replace(/~/g,"\\~");
      	ret = ret.replace(/'/g,"\\'");
      	// ret = ret.replace(/"/g,"\\"");
      	ret = ret.replace(/!/g,"\\!");
      	ret = ret.replace(/\^/g,"\\^");
      	ret = ret.replace(/\(/g,"\\(");
      	ret = ret.replace(/\)/g,"\\)");
      	ret = ret.replace(/=/g,"\\=");
      	ret = ret.replace(/>/g,"\\>");
      	ret = ret.replace(/\|/g,"\\|");
      	ret = ret.replace(/ /g,"\\ ");
	     return ret;
	    
	 }
	 else
	 {
	 	return object;
	 }
}

/**
 * jquery
 * 
 * @param pageurl
 *            
 * @param containerid
 *            
 * @param selector
 *            
 * @return
 */
function loadPageContent(pageurl, containerid, selector) {
	containerid = convertValue(containerid, true);
	
	var idx = pageurl.indexOf("?");
	
	var queryString = null;
	var params = null;
	var tempurl = null;
	
	if(idx >= 0 )
	{
		
		queryString = pageurl.substring(idx+1);
		
		tempurl = pageurl.substring(0,idx)
			
		params = ___parserParams(queryString);
		
	}
	else
	{
		tempurl = pageurl;
	}

	
	
	if (selector && selector != "") {
		$("#" + containerid).load(tempurl + " #" + selector,params,function(){
			setTable_grayCss();
			if($.parser)	$.parser.parse("#" + containerid);
		});
		
	} else {
		$("#" + containerid).load(tempurl,params,function(){
			setTable_grayCss();
			if($.parser)	$.parser.parse("#" + containerid);
		});
	}
}

function ___parserParams(queryString)
{
	
	if(queryString == null )
	{
		return null;
	}
	
	// 
	var params = new Array();
	

	//  ?key=value&key2=value2
	var parameters = queryString.split("&");
	if(parameters.length)
	{
		var pos, paraName, paraValue;
		var datas = "{";
		for(var i=0; i<parameters.length; i++)
		{
			// 
			pos = parameters[i].indexOf('=');
			if(pos == -1) { continue; }
	
			// name  value
			if(i > 0)
				datas += ",";
			datas += "'"+parameters[i].substring(0, pos)+"'";
			datas += ":";
			datas += "'" + parameters[i].substring(pos + 1)+ "'";
			
		}
		datas += "}";
	}
	else
	{
		var pos = parameters.indexOf('=');
		if(pos == -1) { return null }

		// name  value
		datas += "{'" + parameters.substring(0, pos);
		datas += "':";
		datas += "'" + parameters.substring(pos + 1)+ "'}";
	}
	
	return _____parseObj(datas);

}
function _____parseObj( strData ){
	
	try
	{
        return (new Function( "return " + strData ))();
	}
	catch(e)
	{
		alert(e);
	}
	return null;

}

/**
 * 
 * 
 * @param cookieid
 *            cookie
 */
function __chagePageSize(event, cookieid, selectkey,pageurl,selector,containerid) {
	if (event) {

		var select_ = event.srcElement || event.target;

		var pagesize = select_.options[select_.selectedIndex].value;
		// var Days = 100; // cookie  30 
		// var exp = new Date(); //new Date("December 31, 9998");
		// exp.setTime(exp.getTime() + Days*24*60*60*1000);
		// document.cookie = name + "="+ pagesize +";expires="+
		// exp.toGMTString();
		cookiehandle(cookieid, pagesize, {
			path : '/',
			expires : 100
		});
		
		if(containerid && containerid != "")
		{
			loadPageContent(pageurl, containerid, selector);
		}
		else
		{
			location.replace(pageurl);
		}
	} else {
		var select_ = window.event.srcElement;

		var pagesize = select_.options[select_.selectedIndex].value;
		cookiehandle(cookieid, pagesize, {
			path : '/',
			expires : 100
		});
		if(containerid && containerid != "")
		{
			loadPageContent(pageurl, containerid, selector);
		}
		else
		{
			location.replace(pageurl);
		}
	}

}
function cookiehandle(name, value, options) {
if (typeof value != 'undefined') {
   options = options || {};
   if (value === null) {
    value = '';
    options = $.extend({}, options);
    options.expires = -1;
   }
   var expires = '';
   if (options.expires && (typeof options.expires == 'number' || options.expires.toUTCString)) {
    var date;
    if (typeof options.expires == 'number') {
     date = new Date();
     date.setTime(date.getTime() + (options.expires * 24 * 60 * 60 * 1000));
    } else {
     date = options.expires;
    }
    expires = '; expires=' + date.toUTCString();
   }
   var path = options.path ? '; path=' + (options.path) : '';
   var domain = options.domain ? '; domain=' + (options.domain) : '';
   var secure = options.secure ? '; secure' : '';
   document.cookie = [name, '=', encodeURIComponent(value), expires, path, domain, secure].join('');
} else {
   var cookieValue = null;
   if (document.cookie && document.cookie != '') {
    var cookies = document.cookie.split(';');
    for (var i = 0; i < cookies.length; i++) {
     var cookie = jQuery.trim(cookies[i]);
     if (cookie.substring(0, name.length + 1) == (name + '=')) {
      cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
      break;
     }
    }
   }
   return cookieValue;
}
}

/**
 * 
 * IDclasssetTable_grayCss(".table_gray") || setTable_grayCss("#tbid") 
 * @return
 */
function setTable_grayCss(idorClass) {
	if(idorClass==undefined || idorClass=="") idorClass = ".table_gray";

	$(idorClass + ' tbody tr:even').addClass('space_color');
	$(idorClass + ' tbody tr').hover(
	   function() {$(this).addClass('highlight');},
	   function() {$(this).removeClass('highlight');}
	);

	//  

	$(idorClass + ' tbody tr').click(
	   function() {
	    if ($(this).hasClass('selected')) {
	     $(this).removeClass('selected');
	     $(this).find('input[type="checkbox"]').removeAttr('checked');
	    } else {
	     $(this).addClass('selected');
	    $(this).find('input[type="checkbox"]').attr('checked','checked');
	    }
	   }
	);
	}
}