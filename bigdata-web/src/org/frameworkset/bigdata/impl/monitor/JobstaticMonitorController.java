package org.frameworkset.bigdata.impl.monitor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.frameworkset.bigdata.imp.Imp;
import org.frameworkset.bigdata.imp.monitor.SpecialMonitorObject;
import org.frameworkset.bigdata.util.DBJobstatic;
import org.frameworkset.util.annotations.ResponseBody;
import org.frameworkset.web.servlet.ModelMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.handle.ResultSetNullRowHandler;
import com.frameworkset.orm.transaction.TransactionManager;
import com.frameworkset.util.SimpleStringUtil;
import com.frameworkset.util.StringUtil;

public class JobstaticMonitorController {
	private static Logger log = LoggerFactory
			.getLogger(JobstaticMonitorController.class);

	public String index(String job, ModelMap model) {
				
		List<String> allJobNames = Imp.getImpStaticManager().getJobNames();
		if(SimpleStringUtil.isEmpty(job) && allJobNames != null && allJobNames.size() > 0)
		{
			job = allJobNames.get(0);
		}
		SpecialMonitorObject specialMonitorObject = Imp.getImpStaticManager()
				.getSpecialMonitorObject(job);
		model.addAttribute("jobInfo", specialMonitorObject);
		model.addAttribute("allJobNames", allJobNames);
		return "path:index";
	}
	
	public String viewJobHistoryStatic(String jobname,String jobstaticid, ModelMap model)
	{
		 
		try {
			List<DBJobstatic> jobstatics = Imp.getImpStaticManager()
					.getMonitorObjects(jobname);
			String temp = null;
			if(jobstatics != null && jobstatics.size() > 0)
			{
				temp = jobstatics.get(0).getJobstaticid();
				model.addAttribute("jobstatics", jobstatics);
			}
			else
			{
				model.addAttribute("error",""+jobname+"");
			}
			if(!StringUtil.isEmpty(jobstaticid))
			{
				SpecialMonitorObject specialMonitorObject = Imp.getImpStaticManager()
						.getMonitorObject(jobname, jobstaticid);
				if(specialMonitorObject == null)
				{
					model.addAttribute("error",""+jobname+""+jobstaticid);
				}
				else
					model.addAttribute("jobInfo", specialMonitorObject);
			}
			else
			{
				if(temp != null)
				{
					SpecialMonitorObject specialMonitorObject = Imp.getImpStaticManager()
							.getMonitorObject(jobname, temp);
					if(specialMonitorObject == null)
					{
						model.addAttribute("error",""+jobname+""+temp);
					}
					else
						model.addAttribute("jobInfo", specialMonitorObject);
				}
				else
				{
					model.addAttribute("error",""+jobname+"");
				}
			}
			
		} catch (Exception e) {
			model.addAttribute("error",StringUtil.formatException(e));
			log.error("jobname="+jobname + ",jobstaticid="+jobstaticid, e);
		}
		return "path:viewJobHistoryStatic";
	
	}
	
	
	public @ResponseBody String saveJobstatic(String jobname)
	{
		SpecialMonitorObject specialMonitorObject = Imp.getImpStaticManager()
				.getSpecialMonitorObject(jobname);
		if(specialMonitorObject != null)
		{
			try {
				Imp.getImpStaticManager().persistentMonitorObject(specialMonitorObject);
				return "success";
			} catch (Exception e) {
				log.error("saveJobstaticjobname="+jobname , e);
				return StringUtil.formatException(e);
				
			}
		}
		return ""+jobname+".";
		
	}

	public @ResponseBody
	String synJobStatus() {
		Imp.getImpStaticManager().synJobStatus();
		return "success";
	}
	
	public @ResponseBody
	String stopJob(String jobname) {
		Imp.getImpStaticManager().stopJob(jobname);
		return "success";
	}

	public @ResponseBody
	String executeJob(String job) {
		if (StringUtil.isEmpty(job))
			return "";
		try {
			Imp.executeJob(job);
		} catch (Exception e) {
			log.error("" + job, e);
			return StringUtil.exceptionToString(e);
		}
		return "success";
	}

	/**
	 * 
	 * 
	 * @param jobdef
	 * @return
	 */
	public @ResponseBody
	String submitNewJob(String jobdef) {
		if (StringUtil.isEmpty(jobdef))
			return "";
		try {
			String msg = Imp.submitNewJob(jobdef);
			return msg;
		} catch (Exception e) {

			return StringUtil.exceptionToString(e);
		}

	}
	

	public static void main(String[] args) throws SQLException {

		TransactionManager tx = new TransactionManager();
		try {
			tx.begin(TransactionManager.RW_TRANSACTION);
			SQLExecutor.updateWithDBName("defaultp", "drop table if exists person");

			SQLExecutor.updateWithDBName("defaultp",
					"create table person (id integer, name string)");
			SQLExecutor.insert("insert into person values(1, 'leo')");
			SQLExecutor.insert("insert into person values(2, 'yui')");
			SQLExecutor.queryWithDBNameByNullRowHandler(
					new ResultSetNullRowHandler() {

						@Override
						public void handleRow(ResultSet rs) throws Exception {
							System.out.println("name = " + rs.getString("name"));
							System.out.println("id = " + rs.getInt("id"));

						}
					}, "defaultp", "select * from person");
			
			SQLExecutor.queryListInfoWithDBNameByNullRowHandler(
					new ResultSetNullRowHandler() {

						@Override
						public void handleRow(ResultSet rs) throws Exception {
							System.out.println("name = " + rs.getString("name"));
							System.out.println("id = " + rs.getInt("id"));

						}
					}, "defaultp", "select * from person", 0, 2);
			
			SQLExecutor.queryListInfoWithDBNameByNullRowHandler(
					new ResultSetNullRowHandler() {

						@Override
						public void handleRow(ResultSet rs) throws Exception {
							System.out.println("tet name = " + rs.getString("name"));
							

						}
					}, "test", "select 1 as name from dual", 0, 2);
			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.release();
		}
	}
	
	public @ResponseBody String clearJobStatic(String jobname,String hostName)
	{
		return Imp.clearJobStatic(jobname, hostName);
	}

}
