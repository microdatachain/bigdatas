package org.frameworkset.bigdata.imp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.frameworkset.bigdata.imp.monitor.ImpStaticManager;
import org.frameworkset.bigdata.util.DBHelper;
import org.frameworkset.event.EventType;
import org.frameworkset.event.NotifiableFactory;
import org.frameworkset.spi.BaseApplicationContext;
import org.frameworkset.spi.SOAApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frameworkset.util.SimpleStringUtil;

public class Imp {
	private static Logger log = LoggerFactory.getLogger(Imp.class);
	private static File appdir;
	private static Map<String,String> parserParams(String params_)
	{
		Map<String,String> params = new HashMap<String,String>();
		String[] temp = params_.split("\\+");
		for(String p:temp)
		{
			String[] p_ = p.split("=");
			params.put(p_[0], p_[1]);
		}
		return params;
	}
	private static ImpStaticManager impStaticManager = new ImpStaticManager();
	public static ImpStaticManager getImpStaticManager()
	
	{
		return impStaticManager;
	}
	public static void executeJob(String job) throws Exception
	{
		HDFSUploadData HDFSUploadData = new HDFSUploadData();
		HDFSUploadData.executeJob(job);
	}
	public static void startAdminNode(boolean adminasdatanode)
	{
		//
		//eventtypes
		impStaticManager.setAdminasdatanode(adminasdatanode);
		initDB();
		NotifiableFactory.getNotifiable().addListener(new HDFSUploadEventHandler(), HDFSUploadData.hdfsuploadevent);
		List<EventType> monitorEventTypes = new ArrayList<EventType>();
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_request_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_response_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_jobstop_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_stopdatasource_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_reassigntasks_request_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_addworkthread_commond);
		
		NotifiableFactory.getNotifiable().addListener(impStaticManager, monitorEventTypes);
		org.frameworkset.remote.EventUtils.init();
		log.info("");
		log.info("adminNode:true");




	}
	
	public static void startDataNode( )
	{
		//
		//eventtypes
		 
		NotifiableFactory.getNotifiable().addListener(new HDFSUploadEventHandler(), HDFSUploadData.hdfsuploadevent);
		List<EventType> monitorEventTypes = new ArrayList<EventType>();
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_request_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_response_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_jobstop_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_stopdatasource_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_reassigntasks_request_commond);
		monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_addworkthread_commond);
		NotifiableFactory.getNotifiable().addListener(impStaticManager, monitorEventTypes);
		org.frameworkset.remote.EventUtils.init();
		log.info("");
		log.info("adminNode:false");




	}
	
	private static void initDB()
	{
		File dbpath = appdir == null?new File("/configdb"):new File(appdir,"configdb");
		try {
			DBHelper.initConfgDB(dbpath.getCanonicalPath());
			log.info(""+dbpath.getCanonicalPath());
		} catch (IOException e) {
			log.error("",e);
		}
	}
	public static void main(String[] args) throws Exception
	{
		//
				//eventtypes
		String deleteNode = System.getProperty("deleteNode");
		if(deleteNode != null && !deleteNode.trim().equals(""))
		{
			log.info("deleteNode:"+deleteNode);
		}
		else
		{
			
			NotifiableFactory.getNotifiable().addListener(new HDFSUploadEventHandler(), HDFSUploadData.hdfsuploadevent);
			List<EventType> monitorEventTypes = new ArrayList<EventType>();
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_request_commond);
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_response_commond);
			
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_jobstop_commond);
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_stopdatasource_commond);
			
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_reassigntasks_request_commond);
			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_addworkthread_commond);
			NotifiableFactory.getNotifiable().addListener(impStaticManager, monitorEventTypes);
			org.frameworkset.remote.EventUtils.init();
			log.info("");
		}
		String adminNode = System.getProperty("adminNode");
		
		
//		String adminNode = "true";
//		String jobname_="test";
		
		log.info("adminNode:"+adminNode);
		StringBuilder buidler = new StringBuilder();
		for(int i =0 ;args != null && i < args.length; i++)
			 buidler.append(args[i]).append(" ");
		log.info("executor args:"+buidler);
		
		
		if(adminNode != null && adminNode.equals("true"))
		{
			if(args == null || args.length == 0)
			{
				log.info("resources/task.xmljobs=testpagine|datanode=true|");
				return;
			}
			
			initDB();
			String params_=args[0];
			Map<String,String> params =  parserParams(params_);
			String jobname_ = params.get("jobs");
			String[] jobnames = jobname_.split(",");
			if(jobnames == null  || jobnames.length == 0)
			{
				log.info("delete jobs hdfs files :");
				return ;
			}
			log.info("execute jobs:"+jobname_);
			boolean datanode = false;
			String datanode_ = params.get("datanode");
			if(datanode_ != null && datanode_.equals("true"))
			{
				datanode = true;
			}
			impStaticManager.setAdminasdatanode(datanode);
			if(datanode)
				log.info(",datanode=true");
			else
				log.info(",datanode=false");
			HDFSUploadData HDFSUploadData = new HDFSUploadData();
		 
			for(final String job:jobnames)
			{
//				if(jobnames.length > 1)
//				{
//					new Thread(new Runnable(){
//						public void run()
//						{
//							try {
//								HDFSUploadData.uploadData(job,isdatanode);
//							} catch (Exception e) {
//								log.error("["+job+"]", e);
//							}
//						}
//					},"["+job+"]").start();
//					
//				}
//				else
				{
					HDFSUploadData.executeJob(job);
				}
				
			}
		}
		else if(deleteNode != null && !deleteNode.trim().equals(""))
		{
			String params_=args[0];
			Map<String,String> params =  parserParams(params_);
		 
			String jobname_ = params.get("jobs");
			String[] jobnames = jobname_.split(",");
			if(jobnames == null  || jobnames.length == 0)
			{
				log.info("delete jobs hdfs files :");
				return ;
			}
			log.info("delete jobs hdfs files :"+jobname_);
			
			
			
			
			HDFSUploadData HDFSUploadData = new HDFSUploadData();
			for(String job:jobnames)
				HDFSUploadData.deleteData(job);
			
			log.info("delete jobs hdfs files :"+jobname_);
		}
	}
	
	/**
	 * 
	 * @param jobdef
	 * @return
	 * @throws Exception 
	 */
	public static String submitNewJob(String jobdef) throws Exception
	{
		StringBuilder builder = new StringBuilder();
		try {
			BaseApplicationContext ioccontext = new SOAApplicationContext(jobdef);
			List<String> jobs = getConfigTasks(ioccontext);
			
			if(jobs != null && jobs.size() > 0)
			{
				for(int i = 0; i < jobs.size(); i++)
				{
					String jobname = jobs.get(i);
					DBHelper.addOrUdate( jobname, jobdef) ;
					HDFSUploadData HDFSUploadData = new HDFSUploadData();
					try {
						
						HDFSUploadData.executeJob(ioccontext,jobname);
					} catch (Exception e) {
						log.error(jobname + " ",e);
						builder.append(SimpleStringUtil.exceptionToString(e));
					}
				}
			}
		} catch (Exception e) {
			log.error(" "+jobdef,e);
			builder.append(SimpleStringUtil.exceptionToString(e));
		}
		if(builder.length() > 0)
			return builder.toString();
		else
			return "success";
		
		
	}
	
	
	public static List<String> getConfigTasks(BaseApplicationContext context)
	{
		 if(context == null )
			 return null;
		List<String> tasks = new ArrayList<String>();
		
		tasks.addAll(context.getPropertyKeys());
		return tasks;
	}
	public static File getAppdir() {
		return appdir;
	}
	public static void setAppdir(File appdir) {
		Imp.appdir = appdir;
	}
	
	public static String clearJobStatic(String jobname,String hostName)
	{
		return Imp.getImpStaticManager().clearJobStatic(jobname, hostName);
	}
	
	 public static boolean dateRange(String pktype)
	 {
		 return pktype != null && pktype.equals("date"); 
	 }
	 
	 public static boolean timestampRange(String pktype)
	 {
		 return pktype != null && pktype.equals("timestamp"); 
	 }
	 
	 public static boolean numberRange(String pktype)
	 {
		 return pktype == null || pktype.equals("number"); 
	 }
	 
	 
	 public static Date addDays(Date startdate,int days,String pkType)
	 {
		 
		   java.util.Calendar c = java.util.Calendar.getInstance();
			c.setTime(startdate);
			c.add(Calendar.DAY_OF_MONTH, days);
			return  Imp.getDateTime(pkType, c.getTimeInMillis());
	 }
	 public static Date getDateTime(String pktype,long time)
	 {
		 if(dateRange(pktype))
		 {
			return new java.sql.Timestamp(time) ;
		 }
		 else
		 {
			 return new java.sql.Timestamp(time) ;
		 }
	 }
	 
	 public static boolean reachend(Date endoffset,Date end)
	 {
		 boolean reachend = endoffset.after(end) || endoffset.compareTo(end) == 0;
		 return reachend;
	 }

}
