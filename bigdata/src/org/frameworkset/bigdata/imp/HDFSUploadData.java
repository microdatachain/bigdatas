package org.frameworkset.bigdata.imp;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.frameworkset.bigdata.imp.monitor.JobStatic;
import org.frameworkset.bigdata.imp.monitor.SpecialMonitorObject;
import org.frameworkset.bigdata.util.DBHelper;
import org.frameworkset.bigdata.util.DBJob;
import org.frameworkset.event.Event;
import org.frameworkset.event.EventHandle;
import org.frameworkset.event.EventImpl;
import org.frameworkset.event.EventTarget;
import org.frameworkset.event.SimpleEventType;
import org.frameworkset.remote.EventUtils;
import org.frameworkset.spi.BaseApplicationContext;
import org.frameworkset.spi.DefaultApplicationContext;
import org.frameworkset.spi.SOAApplicationContext;
import org.jgroups.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frameworkset.common.poolman.DBUtil;
import com.frameworkset.common.poolman.PreparedDBUtil;
import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.sql.PoolManResultSetMetaData;
import com.frameworkset.orm.annotation.TransactionType;
import com.frameworkset.orm.transaction.TransactionManager;
import com.frameworkset.util.SimpleStringUtil;

public class HDFSUploadData {
	public String HADOOP_PATH;
	
	private static Logger log = LoggerFactory.getLogger(HDFSUploadData.class);

	public static final SimpleEventType hdfsuploadevent = new SimpleEventType(
			"hdfsuploadevent");
	public static final SimpleEventType hdfs_upload_finish_event = new SimpleEventType(
			"hdfs_upload_finish_event");
	public static final SimpleEventType hdfs_upload_monitor = new SimpleEventType(
			"hdfs_upload_monitor");
	public static final SimpleEventType hdfs_upload_monitor_request_commond = new SimpleEventType(
			"hdfs_upload_monitor_request_commond");
	public static final SimpleEventType hdfs_upload_monitor_jobstop_commond = new SimpleEventType(
			"hdfs_upload_monitor_jobstop_commond");
	public static final SimpleEventType hdfs_upload_monitor_response_commond = new SimpleEventType(
			"hdfs_upload_monitor_response_commond");

	public static final SimpleEventType hdfs_upload_monitor_stopdatasource_commond = new SimpleEventType(
			"hdfs_upload_monitor_stopdatasource_commond");
	public static final SimpleEventType hdfs_upload_monitor_addworkthread_commond = new SimpleEventType(
			"hdfs_upload_monitor_addworkthread_commond");

	public static final SimpleEventType hdfs_upload_monitor_reassigntasks_request_commond = new SimpleEventType(
			"hdfs_upload_monitor_reassigntasks_request_commond");
	
	public static final SimpleEventType hdfs_upload_monitor_reassigntasks_response_commond = new SimpleEventType(
			"hdfs_upload_monitor_reassigntasks_response_commond");

	/**
	 * 
	 */
	private String reassigntaskNode;
	
	private String reassigntaskJobname;
	// /**
	// * 
	// */
	// private int needassigntasks;
	// /**
	// * 
	// */
	// private String inhandle;
	/**
	 * 
	 */
	private String driver;
	private String dburl;
	private String dbpassword;
	private String dbuser;
	private String validatesql;
	private String readOnly;
	private boolean usepool;
	// public static final String FILE_PATH="/10.0.15.71.1433174400004";
	private FileSystem fileSystem = null;
	private String hdfsserver;
	private String hdfsdatadir;
	/**
	 * deletefileshdfs
	 */
	private String deletefiles;

	/**
	 * 
	 */
	private String stopdbnames;
	
	private String addworkthreads;
	private String adjustJobname;
	private String localpath;
	private String tablename;
	private String schema;
	private String pkName;
	private String pkType;
	private String columns;
	private String filebasename;
	/**
	 *  
	 *  datablocks
	 *  
	 *  
	 */
	private int datablocks;
	private String dbname;
	private int workservers;
	private boolean rundirect;
	int geneworkthreads = 20;
	int uploadeworkthreads = 20;
	int genqueques = 5;
	int uploadqueues = 5;
	int genquequetimewait = 10;
	int uploadqueuetimewait = 10;
	long startid;
	long endid;
	String datatype = "json";
	boolean genlocalfile;
	boolean clearhdfsfiles;
	String querystatement;
	/**
	 * sql
	 */
	String querypartitionstmt;
	String limitstatement;
	String countstatement;
	String pageinestatement;
	long tablerows;
	boolean usepagine = false;
	boolean adminnodeasdatanode;
	String jobname;
	int fetchsize;
	/**
	 * 
	 */
	int[] blocks = null;
	/**
	 * 
	 */
	private Map<String, List<Integer>> blocksplits = null;

	/**
	 * 
	 */
	int[] excludeblocks = null;
	String excludeblocks_str;
	String blocks_str;
	/**
	 * 
	 */
	Map<String, List<Integer>> excludeblocksplits = null;
	/**
	 * 
	 * subblocks
	 * subblocks
	 */
	int subblocks = -1;
	private Map<String, TaskConfig> tasks;
	
	/**
	 * -joinbytablename
	 */
	private String subtablename;
	/**
	 * 
	 */
	private boolean usepartition;
	private String partitions;
	private boolean usesubpartition = true;
	private String excludepartitions;
	private String leftJoinby;
	private String rightJoinby;
	/**
	 * sql
	 */
	String subquerystatement;
	
	/**
	 * 
	 */
	boolean onejob;
	String target ;
	int rowsperfile;
	private int errorrowslimit = -1;
	/**
	 * 
	 */
	int startfileNo = -1;
	
	/**
	 * 
	 * 
	 * @return
	 */
	TaskConfig buildTaskConfigWithID(String jobstaticid) {
		TaskConfig config = new TaskConfig();
		config.driver = this.driver;
		config.dburl = this.dburl;
		config.dbpassword = this.dbpassword;
		config.dbuser = this.dbuser;
		config.validatesql = this.validatesql;
		config.setReadOnly(readOnly);
		config.usepool = this.usepool;
		config.filebasename = filebasename;
		config.hdfsdatadirpath = this.hdfsdatadir;
		config.hdfsserver = this.hdfsserver;
		config.localdirpath = this.localpath;
		config.pkname = this.pkName;
		config.pktype = this.pkType;
		config.tablename = this.tablename;
		config.geneworkthreads = this.geneworkthreads;
		config.uploadeworkthreads = this.uploadeworkthreads;
		config.genqueques = this.genqueques;
		config.uploadqueues = this.uploadqueues;
		config.genquequetimewait = this.genquequetimewait;
		config.uploadqueuetimewait = this.uploadqueuetimewait;
		config.datatype = this.datatype;
		config.datablocks = this.datablocks;
		config.columns = this.columns;
		config.dbname = this.dbname;
		config.genlocalfile = genlocalfile;
		config.datatype = this.datatype;
		config.schema = this.schema;
		config.usepagine = this.usepagine;
		config.adminnodeasdatanode = this.adminnodeasdatanode;
		config.jobname = jobname;
		config.subblocks = this.subblocks;
		config.setDeletefiles(deletefiles);
		config.setStopdbnames(stopdbnames);
		config.setAddworkthreads(addworkthreads);
		config.setAdjustJobname(adjustJobname);
		config.setReassigntaskNode(reassigntaskNode);
		config.setReassigntaskJobname(reassigntaskJobname);
		config.excludeblocks = this.excludeblocks_str;
		config.blocks = this.blocks_str;
		config.setUsepartition(this.usepartition);
		config.setPartitions(partitions);
		config.setQuerypartitionstmt(querypartitionstmt);
		config.setExcludepartitions(excludepartitions);
		config.setUsesubpartition(usesubpartition);
		config.setSubtablename(subtablename);;
		config.setLeftJoinby(leftJoinby);
		config.setRightJoinby(rightJoinby);
		config.setSubquerystatement(subquerystatement);
		 config.setTarget(target);
		 config.setErrorrowslimit(errorrowslimit);
		 config.setRowsperfile(rowsperfile);
		 config.setOnejob(onejob);
		 config.setStartfileNo(startfileNo);
		 config.startid = this.startid;
		 config.endid = this.endid;
		 config.fetchsize = this.fetchsize;
		 
		 config.setJobstaticid(jobstaticid);
		if(this.onejob)
		{
			if (this.querystatement == null || this.querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if (this.schema != null && !this.schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				sqlbuilder.append(config.tablename);
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement(this.querystatement);
			}
		}
		else if(this.usepartition)
		{
			if (this.querystatement == null || this.querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if (this.schema != null && !this.schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
//				sqlbuilder.append(config.tablename).append(" #{partition}");
				sqlbuilder.append(config.tablename).append(" #{partition}");
				if(datablocks > 0)
				{
//					if(Imp.numberRange(pkType))
//					{
//						sqlbuilder.append(config.tablename).append(" #{partition}").append(" where ")
//						.append(config.pkname).append("<=? and ")
//						.append(config.pkname).append(">=?");
//					}
//					else
//					{
//						sqlbuilder.append(config.tablename).append(" #{partition}");
//					}
					config.setPartitiondataraged(true);
				}
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement(this.querystatement);
			}
			
			
		}
		else if (!this.usepagine) {
			config.limitstatement = limitstatement;
			if (this.querystatement == null || this.querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if (this.schema != null && !this.schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				
//				if(Imp.numberRange(pkType))
//				{
//					sqlbuilder.append(config.tablename).append(" where ")
//					.append(config.pkname).append("<=? and ")
//					.append(config.pkname).append(">=?");
//				}
//				else
				{
					sqlbuilder.append(config.tablename);
				}
				
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement(this.querystatement);
			}
			
			 
		} else {
			config.tablerows = tablerows;
			config.countstatement = countstatement;
			if (pageinestatement == null || pageinestatement.equals("")) {
				// select * from (SELECT t.*,
				// ROW_NUMBER() OVER ( ORDER BY TID) rowno_ from testbigdata t)
				// bb where bb.rowno_ <100 and bb.rowno_ >10
				// StringBuilder sqlbuilder = new StringBuilder();
				// sqlbuilder.append("select * from (SELECT ");
				// if(config.columns != null && ! config.columns.equals(""))
				// {
				// sqlbuilder.append( config.columns);
				// }
				// else
				// sqlbuilder.append("t.* ");
				// sqlbuilder.append(",ROW_NUMBER() OVER ( ORDER BY TID) rowno_  from   ");
				// if(this.schema != null && !this.schema.equals(""))
				// sqlbuilder.append(config.schema).append(".");
				// sqlbuilder.append( config.tablename);
				// sqlbuilder.append(
				// "t) bb where bb.rowno_ <? and bb.rowno_ >?");
				// config.pageinestatement = sqlbuilder.toString();
				config.pageinestatement = DBUtil.getDBAdapter(this.dbname)
						.getStringPagineSql(schema, tablename, pkName, columns);

				// DBUtil.getDBAdapter(this.dbname).getStringPagineSql(sqlbuilder.toString());
			} else {
				config.pageinestatement = pageinestatement;
			}
			
			 

		}
		if (this.subquerystatement == null || this.subquerystatement.equals("")) {
			if(!SimpleStringUtil.isEmpty(subtablename))
			{
				StringBuilder sqlbuilder = new StringBuilder();
				
				sqlbuilder.append("select * ");
				sqlbuilder.append(" from  ");
				if (this.schema != null && !this.schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				sqlbuilder.append(this.subtablename).append(" where ").append(this.leftJoinby).append("=?");
				config.subquerystatement= sqlbuilder.toString();
			}
		} else {
			config.subquerystatement = this.subquerystatement;
		}

		return config;
	}

	private static LinkBlocks buildLinkBlocks(String blocks_) {
		int blocks[] = null;
		LinkBlocks linkBlocks = new LinkBlocks();
		Map<String, List<Integer>> blocksplits = new HashMap<String, List<Integer>>();
		if (blocks_ != null && !blocks_.equals("")) {
			String[] blocks_str = blocks_.trim().split(",");
			Set<Integer> blockset = new java.util.TreeSet<Integer>();

			for (int i = 0; i < blocks_str.length; i++) {
				String block = blocks_str[i];
				if (block.indexOf(".") > 0)// 
				{
					String[] tt = block.split("\\.");
					// blocks[i] = Integer.parseInt(tt[0]);
					blockset.add(Integer.parseInt(tt[0]));
					List<Integer> subtasks = blocksplits.get(tt[0]);
					if (subtasks == null) {
						subtasks = new ArrayList<Integer>();
						blocksplits.put(tt[0], subtasks);
					}
					subtasks.add(Integer.parseInt(tt[1]));

				} else {
					// blocks[i] = Integer.parseInt(block);
					blockset.add(Integer.parseInt(block));
				}
			}
			blocks = new int[blockset.size()];
			int i = 0;
			for (Integer b : blockset) {
				blocks[i] = b.intValue();
				i++;
			}
		}
		linkBlocks.blocks = blocks;
		linkBlocks.blocksplits = blocksplits;
		return linkBlocks;
	}

	/**
	 * 
	 * 
	 * @param jobname
	 * @return
	 */
	public static TaskConfig buildTaskConfig(String jobname) {
		TaskConfig config = new TaskConfig();
		BaseApplicationContext context = DefaultApplicationContext
				.getApplicationContext("tasks.xml");
		if (context.getProBean(jobname) == null)// db
		{
			try {
				DBJob job = DBHelper.getDBJob(jobname);
				if (job != null) {
					config.setJobdef(job.getJobdef());
					context = new SOAApplicationContext(job.getJobdef());
				} else
					log.info("jobname[" + jobname
							+ "],tasksxml.");
			} catch (Exception e) {
				log.error("jobname[" + jobname
						+ "],tasksxml.", e);
			}
		}
		String localpath = context
				.getStringExtendAttribute(jobname, "localdir");
		String hdfsdatadir = context.getStringExtendAttribute(jobname,
				"hdfsdatadir");
		String dbname = context.getStringExtendAttribute(jobname, "dbname");
		String hdfsserver = context.getStringExtendAttribute(jobname,
				"hdfsserver");
		String tablename = context.getStringExtendAttribute(jobname,
				"tablename");
		String schema = context.getStringExtendAttribute(jobname, "schema");
		String pkName = context.getStringExtendAttribute(jobname, "pkname");
		String pkType = context.getStringExtendAttribute(jobname, "pktype");
		String columns = context.getStringExtendAttribute(jobname, "columns");
		int datablocks = context.getIntExtendAttribute(jobname, "datablocks");

		int geneworkthreads = context.getIntExtendAttribute(jobname,
				"geneworkthreads", 20);
		int uploadeworkthreads = context.getIntExtendAttribute(jobname,
				"uploadeworkthreads", 20);
		int genqueques = context
				.getIntExtendAttribute(jobname, "genqueques", 5);
		int uploadqueues = context.getIntExtendAttribute(jobname,
				"uploadqueues", 5);
		int genquequetimewait = context.getIntExtendAttribute(jobname,
				"genquequetimewait", 10);
		int uploadqueuetimewait = context.getIntExtendAttribute(jobname,
				"uploadqueuetimewait", 10);
		String datatype = context.getStringExtendAttribute(jobname, "datatype",
				"json");
		boolean genlocalfile = context.getBooleanExtendAttribute(jobname,
				"genlocalfile", false);
		String filebasename = context.getStringExtendAttribute(jobname,
				"filebasename", tablename);
		boolean clearhdfsfiles = context.getBooleanExtendAttribute(jobname,
				"clearhdfsfiles", false);
		String limitstatement = context.getStringExtendAttribute(jobname,
				"limitstatement");
		String querystatement = context.getStringExtendAttribute(jobname,
				"querystatement");
		int fetchsize = context.getIntExtendAttribute(jobname,
				"fetchsize",0);
		config.setFetchsize(fetchsize);
		long tablerows = context
				.getLongExtendAttribute(jobname, "tablerows", 0);
		boolean usepagine = context.getBooleanExtendAttribute(jobname,
				"usepagine", false);

		String countstatement = context.getStringExtendAttribute(jobname,
				"countstatement");
		String pageinestatement = context.getStringExtendAttribute(jobname,
				"pageinestatement");

		long startid = context
				.getLongExtendAttribute(jobname, "startid", -9999);
		long endid = context.getLongExtendAttribute(jobname, "endid", -9999);

		/**
		 * 
		 */
		String excludeblocks_ = context.getStringExtendAttribute(jobname,
				"excludeblocks");

		String blocks_ = context.getStringExtendAttribute(jobname, "blocks");
		// LinkBlocks linkBlocks = buildLinkBlocks(blocks_);
		// int[] blocks = linkBlocks.blocks;
		// Map<String,List<Integer>> blocksplits = linkBlocks.blocksplits;
		int subblocks = context.getIntExtendAttribute(jobname, "subblocks", -1);

		String driver = context.getStringExtendAttribute(jobname, "driver");
		String dburl = context.getStringExtendAttribute(jobname, "dburl");
		String dbpassword = context.getStringExtendAttribute(jobname,
				"dbpassword", "");
		String dbuser = context.getStringExtendAttribute(jobname, "dbuser", "");
		String validatesql = context.getStringExtendAttribute(jobname,
				"validatesql", "");

		String deletefiles = context.getStringExtendAttribute(jobname,
				"deletefiles");
		String stopdbnames = context.getStringExtendAttribute(jobname,
				"stopdbnames");
		
		config.setStopdbnames(stopdbnames);
		
		
		String addworkthreads = context.getStringExtendAttribute(jobname,
				"addworkthreads");
		 
		config.setAddworkthreads(addworkthreads);

		String adjustJobname = context.getStringExtendAttribute(jobname,
				"adjustJobname");
		 
		config.setAdjustJobname(adjustJobname);
		
		String reassigntaskNode = context.getStringExtendAttribute(jobname,
				"reassigntaskNode");
		String reassigntaskJobname = context.getStringExtendAttribute(jobname,
				"reassigntaskJobname");
		boolean usepartition = context.getBooleanExtendAttribute(jobname,
				"usepartition",false);
		String partitions = context.getStringExtendAttribute(jobname,
				"partitions");
		
		config.setPartitions(partitions);
		String querypartitionstmt = context.getStringExtendAttribute(jobname,
				"querypartitionstmt");
		config.setQuerypartitionstmt(querypartitionstmt);
		String excludepartitions = context.getStringExtendAttribute(jobname,
				"excludepartitions");
		boolean usesubpartition = context.getBooleanExtendAttribute(jobname,
				"usesubpartition",true);
		config.setUsesubpartition(usesubpartition);
		config.setExcludepartitions(excludepartitions);
		config.setUsepartition(usepartition);
		String subtablename = context.getStringExtendAttribute(jobname,
				"subtablename");
		config.setSubtablename(subtablename);
		String leftJoinby = context.getStringExtendAttribute(jobname,
				"leftJoinby");
		config.setLeftJoinby(leftJoinby);
		String rightJoinby = context.getStringExtendAttribute(jobname,
				"rightJoinby");
		
		String subquerystatement = context.getStringExtendAttribute(jobname,
				"subquerystatement"); 
		config.setSubquerystatement(subquerystatement);
		config.setRightJoinby(rightJoinby);
		config.setReassigntaskNode(reassigntaskNode);
		config.setReassigntaskJobname(reassigntaskJobname);
		
		 String target = context.getStringExtendAttribute(jobname,
					"target");
		 int errorrowslimit = context.getIntExtendAttribute(jobname,
					"errorrowslimit",-1);
		 config.setErrorrowslimit(errorrowslimit);
		 boolean onejob = context.getBooleanExtendAttribute(jobname,
					"single",false); 	 
		 int rowsperfile = context.getIntExtendAttribute(jobname, "rowsperfile", 0);
		 
		 config.setTarget(target);
		 config.setRowsperfile(rowsperfile);
		 config.setOnejob(onejob);
		 int startfileNo = context.getIntExtendAttribute(jobname, "startfileNo", -1);
		 config.setStartfileNo(startfileNo);
		boolean usepool = context.getBooleanExtendAttribute(jobname, "usepool",
				false);

		String readOnly = context.getStringExtendAttribute(jobname, "readOnly");

		config.setReadOnly(readOnly);
		config.driver = driver;
		config.dburl = dburl;
		config.dbpassword = dbpassword;
		config.dbuser = dbuser;
		config.validatesql = validatesql;

		config.usepool = usepool;
		config.filebasename = filebasename;
		config.hdfsdatadirpath = hdfsdatadir;
		config.hdfsserver = hdfsserver;
		config.localdirpath = localpath;
		config.pkname = pkName;
		config.pktype = pkType;
		config.tablename = tablename;
		config.geneworkthreads = geneworkthreads;
		config.uploadeworkthreads = uploadeworkthreads;
		config.genqueques = genqueques;
		config.uploadqueues = uploadqueues;
		config.genquequetimewait = genquequetimewait;
		config.uploadqueuetimewait = uploadqueuetimewait;
		config.datatype = datatype;
		config.datablocks = datablocks;
		config.columns = columns;
		config.dbname = dbname;
		config.genlocalfile = genlocalfile;
		config.datatype = datatype;
		config.schema = schema;

		config.usepagine = usepagine;
		config.clearhdfsfiles = clearhdfsfiles;
		config.jobname = jobname;
		config.startid = startid;
		config.endid = endid;
		config.blocks = blocks_;
		config.excludeblocks = excludeblocks_;
		// config.blocksplits = blocksplits;
		config.subblocks = subblocks;
		if(onejob)
		{
			if (querystatement == null || querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if ( schema != null && ! schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				sqlbuilder.append(config.tablename);
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement( querystatement);
			}
		}
		else if( usepartition)
		{
			if ( querystatement == null ||  querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if ( schema != null && ! schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				sqlbuilder.append(config.tablename).append(" #{partition}");
				if(datablocks > 0)
				{
//					sqlbuilder.append(config.tablename).append(" #{partition}").append(" where ")
//					.append(config.pkname).append("<=? and ")
//					.append(config.pkname).append(">=?");
					config.setPartitiondataraged(true);
				}
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement( querystatement);
			}
			
			
		}
		else if (!usepagine) {
			config.limitstatement = limitstatement;
			if (querystatement == null || querystatement.equals("")) {
				StringBuilder sqlbuilder = new StringBuilder();
				sqlbuilder.append("select ");
				if (config.columns != null && !config.columns.equals("")) {
					sqlbuilder.append(config.columns);
				} else
					sqlbuilder.append("* ");
				sqlbuilder.append(" from  ");
				if (schema != null && !schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
//				sqlbuilder.append(config.tablename).append(" where ")
//						.append(config.pkname).append("<=? and ")
//						.append(config.pkname).append(">=?");
				config.setQuerystatement(sqlbuilder.toString());
			} else {
				config.setQuerystatement( querystatement);
			}
		} else {
			config.tablerows = tablerows;
			config.countstatement = countstatement;
			if (pageinestatement == null || pageinestatement.equals("")) {

				// DBHelper.initDB(config);
				// config.pageinestatement = DBUtil.getDBAdapter(dbname)
				// .getStringPagineSql(schema, tablename, pkName, columns);

			} else {
				config.pageinestatement = pageinestatement;
			}

		}
		
		if (subquerystatement == null || subquerystatement.equals("") ) {
			if(!SimpleStringUtil.isEmpty(subtablename))
			{
				StringBuilder sqlbuilder = new StringBuilder();
				
				sqlbuilder.append("select * ");
				sqlbuilder.append(" from  ");
				if (schema != null && !schema.equals(""))
					sqlbuilder.append(config.schema).append(".");
				sqlbuilder.append(subtablename).append(" where ").append(leftJoinby).append("=?");
				config.subquerystatement= sqlbuilder.toString();
			}
			
		} else {
			config.subquerystatement = subquerystatement;
		}


		return config;
	}

	static class SplitTasks {
		TaskInfo[] segments;
		long segement = 0l;
		
		long startid =-9999;
		long endid = -9999;
		int nextpartpositionoffset;
		
	}
	
	private void spiltTask_(TaskInfo[] segments, long startid, long endid,
			int datablocks, long segement, long div, boolean usepagine,
			String filebasename, String parentTaskNo)
	{
		spiltTask_(segments, startid, endid,
				datablocks, segement, div, usepagine,
				filebasename, parentTaskNo,-1);
	}

	/**
	 * segments , startid, subblocks,segement, div, usepagine
	 * 
	 * @param segments
	 * @param startid
	 * @param datablocks
	 * @param segement
	 * @param div
	 * @param usepagine
	 */
	private void spiltTask_(TaskInfo[] segments, long startid, long endid,
			int datablocks, long segement, long div, boolean usepagine,
			String filebasename, String parentTaskNo,int partpositionoffset) {
		if (!usepagine) {
			for (int i = 0; i < datablocks; i++) {
				TaskInfo task = new TaskInfo();
				task.startid = startid;
				task.endid = endid;
				if (i < div) {
					task.startoffset = startid + i * segement + i;
					task.endoffset = task.startoffset + segement - 1 + 1;
					task.pagesize = task.endoffset - task.startoffset + 1;

				} else {
					task.startoffset = startid + i * segement + div;
					if (i == segments.length - 1)
						task.endoffset = endid;
					else
						task.endoffset = task.startoffset + segement - 1;
					task.pagesize = task.endoffset - task.startoffset + 1;
				}
				task.filename = filebasename + "_" + i;
				int reali = partpositionoffset != -1?partpositionoffset+i:i;
				task.taskNo = parentTaskNo == null ? "" + reali: parentTaskNo
						+ "." + i;
				segments[i] = task;
			}
		} else {
			for (int i = 0; i < datablocks; i++) {
				TaskInfo task = new TaskInfo();
				task.startid = startid;
				task.endid = endid;
				if (i < div) {
					task.startoffset = startid + i * segement + i;
					// task.endoffset = task.startoffset + segement-1+1;
					task.pagesize = segement + 1;

				} else {
					task.startoffset = startid + i * segement + div;
					// if(i == segments.length - 1)
					// task.endoffset = endid;
					// else
					// task.endoffset = task.startoffset + segement-1;
					task.pagesize = segement;
				}
				task.filename = filebasename + "_" + i;
				task.taskNo = parentTaskNo == null ? "" + i : parentTaskNo
						+ "." + i;
				segments[i] = task;
			}
		}
	}
	
	/**
	 * segments , startid, subblocks,segement, div, usepagine
	 * 
	 * @param segments
	 * @param startid
	 * @param datablocks
	 * @param segement
	 * @param div
	 * @param usepagine
	 */
	private void spiltDateTask_(List<TaskInfo> segments, java.util.Date start, java.util.Date end,
			int datablocks, 
			String filebasename, String parentTaskNo,int partpositionoffset,boolean lasted) {
		java.util.Date startoffset = start, endoffset = null;
		int i = 0;
		while(true)
		{
//			endoffset = Imp.addDays(startoffset, datablocks-1, pkType);
			endoffset = Imp.addDays(startoffset, datablocks, pkType);
			TaskInfo task = new TaskInfo();
			task.startid = start.getTime();
			task.endid = end.getTime();
			task.startoffset = startoffset.getTime();
			boolean reachend = Imp.reachend(endoffset,end) ;
			if(reachend)
			{
				endoffset = end;
				
			}
//			startoffset = Imp.addDays(endoffset, 1, pkType);
			startoffset = endoffset;
			task.endoffset = endoffset.getTime();
			task.pagesize = datablocks;
			task.filename = filebasename + "_" + i;
			int reali = partpositionoffset != -1?partpositionoffset+i:i;
			task.taskNo = parentTaskNo == null ? "" + reali: parentTaskNo
					+ "." + i;
			if(parentTaskNo != null)
			{
				task.setSubblock(true);
				task.setLasted(lasted);
				if(reachend)
					task.setSublasted(true);
			}
			else
			{
				if(reachend)
					task.setLasted(true);
			}
			
			segments.add( task);
			
			if(reachend)
				break;
			i ++;
		}
//		for (int i = 0; i < datablocks; i++) {
//			TaskInfo task = new TaskInfo();
//			task.startid = start.getTime();
//			task.endid = end.getTime();
//			if (i < div) {
//				task.startoffset = startid + i * segement + i;
//				task.endoffset = task.startoffset + segement - 1 + 1;
//				task.pagesize = task.endoffset - task.startoffset + 1;
//
//			} else {
//				task.startoffset = startid + i * segement + div;
//				if (i == segments.length - 1)
//					task.endoffset = endid;
//				else
//					task.endoffset = task.startoffset + segement - 1;
//				task.pagesize = task.endoffset - task.startoffset + 1;
//			}
//			task.filename = filebasename + "_" + i;
//			int reali = partpositionoffset != -1?partpositionoffset+i:i;
//			task.taskNo = parentTaskNo == null ? "" + reali: parentTaskNo
//					+ "." + i;
//			segments[i] = task;
//		}
//		
	}

	static class LinkTasks {
		int block;
		TaskInfo taskInfo;
	}

	static class LinkBlocks {
		int[] blocks;
		Map<String, List<Integer>> blocksplits;
	}

	/**
	 * 
	 * @param segments
	 * @param segement
	 * @return
	 */
	SplitTasks filterTasks(TaskInfo[] segments ,long segement)
	{
		List<LinkTasks> temp = null;
		List<TaskInfo> retTasks = null;
		if (this.blocks != null && this.blocks.length > 0)// 
		{
			temp = new ArrayList<LinkTasks>(blocks.length);
			int blockslen = this.blocks.length;
			for (int i = 0; i < blockslen; i++) {
				if (blocks[i] < segments.length) {
					LinkTasks linkTasks = new LinkTasks();
					linkTasks.block = blocks[i];
					linkTasks.taskInfo = segments[blocks[i]];
					temp.add(linkTasks);

				}
			}
			if (temp.size() > 0) {
				if (this.subblocks > 0)// hdfs
				{
					// 
					List<TaskInfo> subchunks = new ArrayList<TaskInfo>();
					for (int i = 0; i < temp.size(); i++) {
						LinkTasks linkTasks = temp.get(i);
						TaskInfo t = linkTasks.taskInfo;
						SplitTasks subsplitTasks = buildJobSubChunks(t);
						if (subsplitTasks == null) {
							subchunks.add(t);
						} else {
							segement = subsplitTasks.segement;
							List<Integer> filters = this.blocksplits
									.get(linkTasks.block + "");// 
							if (filters != null && filters.size() > 0) {
								List<TaskInfo> subsgs = new ArrayList<TaskInfo>(
										filters.size());
								TaskInfo[] sgs = subsplitTasks.segments;
								for (int j = 0; j < filters.size(); j++) {
									int pos = filters.get(j).intValue();
									if (pos < sgs.length) {
										subsgs.add(sgs[pos]);
									}
								}
								subchunks.addAll(subsgs);
							} else {
								subchunks.addAll(Arrays
										.asList(subsplitTasks.segments));
							}

						}
					}
					retTasks = subchunks;

				} else {
					retTasks = new ArrayList<TaskInfo>();
					for (int i = 0; i < temp.size(); i++) {
						LinkTasks linkTasks = temp.get(i);
						retTasks.add(linkTasks.taskInfo);
					}
				}

				segments = new TaskInfo[retTasks.size()];

				retTasks.toArray(segments);

			}
		} else if (this.excludeblocks != null && this.excludeblocks.length > 0)// 
		{
			temp = new ArrayList<LinkTasks>();
			int blockslen = this.excludeblocks.length;
			for (int i = 0; i < blockslen; i++) {// 
				if (excludeblocks[i] < segments.length) {
					LinkTasks linkTasks = new LinkTasks();
					linkTasks.block = excludeblocks[i];
					linkTasks.taskInfo = segments[linkTasks.block];
					temp.add(linkTasks);

				}
			}
			if (temp.size() > 0) {
				if (this.subblocks > 0)// hdfs
				{
					// 
					List<TaskInfo> subchunks = new ArrayList<TaskInfo>();
					for (int j = 0; j < segments.length; j++) {
						SplitTasks subsplitTasks = buildJobSubChunks(segments[j]);
						if (subsplitTasks != null)
							segement = subsplitTasks.segement;
						boolean isexclude = false;
						for (int i = 0; i < temp.size(); i++) {
							LinkTasks linkTasks = temp.get(i);
							if (linkTasks.block == j)// 
							{
								isexclude = true;
								break;
							}
						}
						if (!isexclude) {
							if (subsplitTasks == null) {
								subchunks.add(segments[j]);
							} else {
								subchunks.addAll(Arrays
										.asList(subsplitTasks.segments));
							}
						} else {
							if (subsplitTasks == null)// 
							{

							} else // 
							{
								List<Integer> excludes = this.excludeblocksplits
										.get(j + "");
								if (excludes == null)// 
								{

								} else // 
								{

									for (int k = 0; k < subsplitTasks.segments.length; k++) {
										boolean issubexclude = false;
										for (Integer epos : excludes) {
											if (k == epos.intValue()) {
												issubexclude = true;
												break;
											}

										}
										if (!issubexclude)
											subchunks
													.add(subsplitTasks.segments[k]);
									}

								}

							}
						}

					}
					retTasks = subchunks;

				} else {
					retTasks = new ArrayList<TaskInfo>();
					for (int j = 0; j < segments.length; j++) {
						boolean isexclude = false;
						for (int i = 0; i < temp.size(); i++) {
							LinkTasks linkTasks = temp.get(i);
							if (linkTasks.block == j) {
								isexclude = true;
							}
						}
						if (!isexclude)
							retTasks.add(segments[j]);
					}
				}

				segments = new TaskInfo[retTasks.size()];

				retTasks.toArray(segments);

			}
		}
		SplitTasks splitTasks = new SplitTasks();
		splitTasks.segement = segement;
		splitTasks.segments = segments;
		return splitTasks;
		
	}
	private List<PartitionInfo> handlePartitions(List<String> partitions) throws SQLException
	{
		List<PartitionInfo> result = null;
		if(this.usesubpartition )
		{
			if(partitions != null && partitions.size() > 0 )
			{
				result = new ArrayList<PartitionInfo>();
				StringBuilder queryPartitions = new StringBuilder();
				if(!SimpleStringUtil.isEmpty(schema))
				{
					queryPartitions.append("SELECT SUBPARTITION_NAME FROM ALL_TAB_SUBPARTITIONS WHERE TABLE_NAME=? and PARTITION_NAME=?   order by PARTITION_NAME");
				}
				else
					queryPartitions.append("SELECT SUBPARTITION_NAME FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=? and PARTITION_NAME=?   order by PARTITION_NAME");
				String t = queryPartitions.toString();
				String tab = this.tablename.toUpperCase();
				List<String> subparts = null;
				for(String partition:partitions)
				{
					if(!partition.startsWith("sub:"))
					{
						subparts = SQLExecutor.queryListWithDBName(String.class, this.dbname,t,tab,partition);
						if(subparts == null || subparts.size() == 0)
						{
							PartitionInfo partitionInfo = new PartitionInfo();
							partitionInfo.setIssubpartition(false);
							partitionInfo.setPartition(partition);
							result.add(partitionInfo);
						}
						else
						{
							for(String subpart:subparts)
							{
								PartitionInfo partitionInfo = new PartitionInfo();
								partitionInfo.setIssubpartition(true);
								partitionInfo.setPartition(partition);
								partitionInfo.setSubpartition(subpart);
								result.add(partitionInfo);
							}
						}
					}
					else
					{
						partition = partition.substring("sub:".length());
						PartitionInfo partitionInfo = new PartitionInfo();
						partitionInfo.setIssubpartition(true);
						partitionInfo.setPartition(null);
						partitionInfo.setSubpartition(partition);
						result.add(partitionInfo);
					}
						
					
				}
			}
		}
		else
		{
			if(partitions != null && partitions.size() > 0 )
			{
				result = new ArrayList<PartitionInfo>(partitions.size());
				for(String partition:partitions)
				{					 
					if(!partition.startsWith("sub:"))
					{
						PartitionInfo partitionInfo = new PartitionInfo();
						partitionInfo.setIssubpartition(false);
						partitionInfo.setPartition(partition);
						result.add(partitionInfo);
					}
					else
					{
						partition = partition.substring("sub:".length());
						PartitionInfo partitionInfo = new PartitionInfo();
						partitionInfo.setIssubpartition(true);
						partitionInfo.setSubpartition(partition);
						result.add(partitionInfo);
						
					}
				}
			}
		}
		return result;
	}
	private List<PartitionInfo> queryPartitions() throws SQLException
	{
		List<String> partitions = null;
		
		StringBuilder queryPartitions = new StringBuilder();
		if(this.querypartitionstmt == null || querypartitionstmt.equals(""))
		{
			if(!SimpleStringUtil.isEmpty(schema))
			{
				queryPartitions.append("SELECT PARTITION_NAME FROM ALL_TAB_PARTITIONS WHERE TABLE_NAME=upper('").append(this.tablename).append("') and table_owner='").append(this.schema).append("'  order by PARTITION_NAME");
			}
			else
				queryPartitions.append("SELECT PARTITION_NAME FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=upper('").append(this.tablename).append("') order by PARTITION_NAME");
			
			partitions = SQLExecutor.queryListWithDBName(String.class, this.dbname,queryPartitions.toString());
		}
		else
		{
			partitions = SQLExecutor.queryListWithDBName(String.class, this.dbname,querypartitionstmt);
		}
		return handlePartitions( partitions);
	}
	
	private SplitTasks buildpartitionsegement(PartitionInfo partitionInfo,int partposition) throws Exception
	{
		long startid =-9999;
		long endid = -9999;
		TaskInfo[] segments = null;
		long segement = 0;
		String plimitsql = null;
		StringBuilder limitsql = new StringBuilder();
		if (this.limitstatement == null
				|| this.limitstatement.equals("")) {

			String tableName = null;
			if (this.schema == null) {
				tableName = this.tablename;
			} else
				tableName = this.schema + "." + this.tablename;
			limitsql.append("select min(").append(this.pkName)
					.append(") as startid,max(").append(this.pkName)
					.append(") as endid from ").append(tableName).append(partitionInfo.evalsqlpart());
			plimitsql = limitsql.toString();
		}
		else
		{
			plimitsql = partitionInfo.evalsqlpart(limitstatement);
		}

		log.info("Admin Data Node evaluate data scope for job["
				+ this.jobname + "] use pk[" + this.pkName
				+ "] partitions:" + plimitsql + " on datasource "
				+ dbname);
		PreparedDBUtil db = new PreparedDBUtil();
		db.preparedSelect(this.dbname, plimitsql);
		db.executePrepared();
		if (db.size() > 0) {

			PoolManResultSetMetaData meta = (PoolManResultSetMetaData)db.getMeta();
			int starttype = meta.getColumnType(1);
			
		    if(starttype == java.sql.Types.TIMESTAMP  )
		    {
		    	Timestamp startid_ = db.getTimestamp(0, "startid");
		    	Timestamp endid_ = db.getTimestamp(0, "endid");
		    	if(startid_ == null)
		    	{
		    		log.info("PK[" + this.pkName + "] partition job["
							+ this.jobname + "]  base infomation:start data id="
							+ startid_ + ",endid=" + endid_ + ",datablocks="
							+ datablocks + ",.");
					return null;
		    	}
		    	this.pkType="timestamp";
		    	startid = startid_.getTime();
		    	endid = endid_.getTime();
		    }
		    else if(starttype == java.sql.Types.DATE)
		    {
		    	 Date startid_ = db.getTimestamp(0, "startid");
		    	Date endid_ = db.getTimestamp(0, "endid");
		    	if(startid_ == null)
		    	{
		    		log.info("PK[" + this.pkName + "] partition job["
							+ this.jobname + "]  base infomation:start data id="
							+ startid_ + ",endid=" + endid_ + ",datablocks="
							+ datablocks + ",.");
					return null;
		    	}
		    	this.pkType="date";
		    	startid = startid_.getTime();
		    	endid = endid_.getTime();
		    }
		    else
		    {
		    	this.pkType= "number";
		    	startid = db.getLong(0, "startid");
		    	endid = db.getLong(0, "endid");
		    }
			
			
			
		}
		else
		{
			log.info("PK[" + this.pkName + "] partition job["
					+ this.jobname + "]  base infomation:start data id="
					+ startid + ",endid=" + endid + ",datablocks="
					+ datablocks + ",.");
			return null;
		}
		
		String filebasename = partitionInfo.isIssubpartition()?partitionInfo.getSubpartition():partitionInfo.getPartition();
			SplitTasks splitTasks = new SplitTasks();
			splitTasks.startid= startid;
			splitTasks.endid = endid;
			
			if(!Imp.numberRange(pkType))//
			{
				java.util.Date startdate = Imp.getDateTime(pkType, startid);
				java.util.Date enddate = Imp.getDateTime(pkType, endid);
				log.info("PK[" + this.pkName + "] partition job["
						+ this.jobname + "]  base infomation:start date="
						+ startdate + ",end date=" + enddate + ",datablocks="
						+ datablocks);
//				java.util.Calendar c = java.util.Calendar.getInstance();
//				c.setTime(startdate);
//				c.add(Calendar.DAY_OF_MONTH, datablocks);
				java.util.Date tempdate = Imp.addDays(startdate, datablocks-1, pkType);
	
				if (!Imp.reachend(tempdate,enddate)) {
					segement = this.datablocks;
					List<TaskInfo> tempsegments = new ArrayList<TaskInfo>();
					spiltDateTask_(tempsegments, startdate, enddate, this.datablocks, filebasename, null,partposition,false);
					segments = new TaskInfo[tempsegments.size()];
					tempsegments.toArray(segments);
					splitTasks.nextpartpositionoffset = partposition+segments.length;
				} 
				else // 
				{
					TaskInfo task = new TaskInfo();
					task.startoffset = startid;
					task.endoffset = endid;
					task.pagesize = datablocks;
					task.filename = filebasename + "_0";
					task.taskNo = partposition+"";
					task.setLasted(true);
					segement = datablocks;
					segments = new TaskInfo[1];
					segments[0] = task;
					splitTasks.nextpartpositionoffset = partposition+1;
				}
			}
			else//
			{
				log.info("PK[" + this.pkName + "] partition job["
						+ this.jobname + "]  base infomation:start data id="
						+ startid + ",endid=" + endid + ",datablocks="
						+ datablocks);
				long datas = endid - startid + 1;
				
				// segments[this.workservers-1] = segments[this.workservers-1] +
				// div;
	
				// 
				
	
				if (datas > datablocks) {
					segments = new TaskInfo[this.datablocks];
					segement = datas / this.datablocks;
	
					long div = datas % this.datablocks;
					
					spiltTask_(segments, startid, endid, this.datablocks,
							segement, div, false, filebasename, null,partposition);
					splitTasks.nextpartpositionoffset = partposition+this.datablocks;
				} 
				else // 
				{
					TaskInfo task = new TaskInfo();
					task.startoffset = startid;
					task.endoffset = endid;
					task.pagesize = datas;
					task.filename = filebasename + "_0";
					task.taskNo = partposition+"";
					segement = datas;
					segments = new TaskInfo[1];
					segments[0] = task;
					splitTasks.nextpartpositionoffset = partposition+1;
				}
			}
			splitTasks.segments = segments;
			splitTasks.segement = segement;
			return splitTasks;
		
	}
	private SplitTasks buildatarangeTasks() throws Exception
	{
		SplitTasks splitTasks = new SplitTasks();
		long segement = 0;
		TaskInfo[] segments = null;
		if (this.startid == -9999 || this.endid == -9999) {
			StringBuilder limitsql = new StringBuilder();
			if (this.limitstatement == null
					|| this.limitstatement.equals("")) {

				String tableName = null;
				if (this.schema == null) {
					tableName = this.tablename;
				} else
					tableName = this.schema + "." + this.tablename;
				limitsql.append("select min(").append(this.pkName)
						.append(") as startid,max(").append(this.pkName)
						.append(") as endid from ").append(tableName);
				this.limitstatement = limitsql.toString();
			}

			log.info("Admin Data Node evaluate data scope for job["
					+ this.jobname + "] use pk[" + this.pkName
					+ "] partitions:" + limitstatement + " on datasource "
					+ dbname);
			PreparedDBUtil db = new PreparedDBUtil();
			db.preparedSelect(this.dbname, limitstatement);
			db.executePrepared();
			if (db.size() > 0) {

				PoolManResultSetMetaData meta = (PoolManResultSetMetaData)db.getMeta();
				int starttype = meta.getColumnType(1);
				
			    if(starttype == java.sql.Types.TIMESTAMP  )
			    {
			    	Timestamp startid_ = db.getTimestamp(0, "startid");
			    	Timestamp endid_ = db.getTimestamp(0, "endid");
			    	if(startid_ == null)
			    	{
			    		log.info("PK[" + this.pkName + "] partition job["
								+ this.jobname + "]  base infomation:start data id="
								+ startid_ + ",endid=" + endid_ + ",datablocks="
								+ datablocks + ",.");
						return null;
			    	}
			    	this.pkType="timestamp";
			    	startid = startid_.getTime();
			    	endid = endid_.getTime();
			    }
			    else if(starttype == java.sql.Types.DATE)
			    {
			    	 Date startid_ = db.getTimestamp(0, "startid");
			    	Date endid_ = db.getTimestamp(0, "endid");
			    	if(startid_ == null)
			    	{
			    		log.info("PK[" + this.pkName + "] partition job["
								+ this.jobname + "]  base infomation:start data id="
								+ startid_ + ",endid=" + endid_ + ",datablocks="
								+ datablocks + ",.");
						return null;
			    	}
			    	this.pkType="date";
			    	startid = startid_.getTime();
			    	endid = endid_.getTime();
			    }
			    else
			    {
			    	startid = db.getLong(0, "startid");
			    	endid = db.getLong(0, "endid");
			    }
				
				
			}
		}
		if (this.startid != -9999 && this.endid != -9999) {
			log.info("PK[" + this.pkName + "] partition job["
					+ this.jobname + "]  base infomation:start data id="
					+ startid + ",endid=" + endid + ",datablocks="
					+ datablocks);
			if(!Imp.numberRange(pkType))//
			{
			
				java.util.Date startdate = Imp.getDateTime(pkType, startid);
				java.util.Date enddate = Imp.getDateTime(pkType, endid);
				
//				java.util.Calendar c = java.util.Calendar.getInstance();
//				c.setTime(startdate);
//				c.add(Calendar.DAY_OF_MONTH, datablocks);
				java.util.Date tempdate = Imp.addDays(startdate, datablocks-1, pkType);
	
				if (!Imp.reachend(tempdate,enddate)) {
					segement = this.datablocks;
					List<TaskInfo> tempsegments = new ArrayList<TaskInfo>();
					spiltDateTask_(tempsegments, startdate, enddate, this.datablocks,
							 filebasename, null,-1,false);
					segments = new TaskInfo[tempsegments.size()];
					tempsegments.toArray(segments);
					 
				} 
				else // 
				{
					TaskInfo task = new TaskInfo();
					task.startoffset = startid;
					task.endoffset = endid;
					task.pagesize = datablocks;
					task.filename = filebasename + "_0";
					task.setLasted(true);
					task.taskNo = "0";
					segement = datablocks;
					segments = new TaskInfo[1];
					segments[0] = task;
				}
			}
			else
			{
				long datas = endid - startid + 1;	
				// segments[this.workservers-1] = segments[this.workservers-1] +
				// div;	
				// 	
				if (datas > datablocks) {
					segement = datas / this.datablocks;
	
					long div = datas % this.datablocks;
					segments = new TaskInfo[this.datablocks];
					spiltTask_(segments, startid, endid, this.datablocks,
							segement, div, usepagine, filebasename, null);
				} 
				else // 
				{
					TaskInfo task = new TaskInfo();
					task.startoffset = startid;
					task.endoffset = endid;
					task.pagesize = datas;
					task.filename = filebasename + "_0";
					task.taskNo = "0";
					segement = datas;
					segments = new TaskInfo[1];
					segments[0] = task;
				}
			}
			splitTasks.segement = segement;
			splitTasks.segments = segments;
			return splitTasks;
		} else {
			log.info("PK[" + this.pkName + "] partition job["
					+ this.jobname + "]  base infomation:start data id="
					+ startid + ",endid=" + endid + ",datablocks="
					+ datablocks + ",.");
			return null;
		}
	}
	
	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	SplitTasks spiltTask() throws Exception {
		SplitTasks splitTasks = new SplitTasks();
		TaskInfo[] segments = null;
		long segement = 0l;
		if(this.usepartition)//
		{
			List<PartitionInfo> partitions = null;
			TransactionManager tm = new TransactionManager();
			try
			{
				tm.begin(TransactionType.RW_TRANSACTION);
				if(SimpleStringUtil.isEmpty(this.partitions))//
				{
	
					partitions = queryPartitions();
					if(partitions != null && partitions.size() > 0)
					{
						if(!SimpleStringUtil.isEmpty(excludepartitions))//excludeblocksexcludeblocks
						{
							String[] epts = this.excludepartitions.split(",");						
							List<PartitionInfo> temppts = new ArrayList<PartitionInfo>();
							for(int i = 0; i < partitions.size(); i ++)
							{
								PartitionInfo part = partitions.get(i);
								boolean exclude = false;
								for(String ept:epts )
								{
									if(ept.startsWith("sub:"))
										ept = ept.substring("sub:".length());
									String pt = part.isIssubpartition()?part.getSubpartition():part.getPartition();
									if(pt.equalsIgnoreCase(ept))
									{
										exclude = true;
										break;
									}
								}
								if(!exclude)
								{
									temppts.add(part);
								}
							}
							partitions = temppts;
						}
						
						
						
	
					}
				}
				else //
				{
					
					String[] pts = this.partitions.split(",");
					partitions = handlePartitions(Arrays.asList(pts));
	
				}
				tm.commit();
			}
			finally
			{
				tm.releasenolog();
			}
			
			if(partitions != null && partitions.size() > 0)
			{
				if(this.datablocks <= 0)
				{
					segments = new TaskInfo[partitions.size()];
					for(int i = 0; i < partitions.size(); i ++)
					{
						PartitionInfo partition = partitions.get(i);
						TaskInfo taskInfo = new TaskInfo();
						taskInfo.partitionName = partition.getPartition(); 
						taskInfo.setIssubpartition(partition.isIssubpartition());
						taskInfo.setSubpartition(partition.getSubpartition());
						taskInfo.taskNo = i + "";
						taskInfo.filename = partition.isIssubpartition()?partition.getSubpartition():partition.getPartition();
						segments[i] = taskInfo;
					}
				}
				else
				{
					List<TaskInfo> allpartitionsegments = new ArrayList<TaskInfo>();
					int nextpartpositionoffset = 0;
					for(int i = 0; i < partitions.size(); i ++)
					{
						PartitionInfo partition = partitions.get(i);
						SplitTasks partTasks = buildpartitionsegement(partition,nextpartpositionoffset);
						
						if(partTasks != null && partTasks.segments != null)
						{
							nextpartpositionoffset = partTasks.nextpartpositionoffset;
							for(int j = 0; j < partTasks.segments.length; j ++)
							{
								TaskInfo taskInfo = partTasks.segments[j];
								taskInfo.partitionName = partition.getPartition(); 
								taskInfo.setIssubpartition(partition.isIssubpartition());
								taskInfo.setSubpartition(partition.getSubpartition());
								taskInfo.startid = partTasks.startid;
								taskInfo.endid = partTasks.endid;
								allpartitionsegments.add(taskInfo);
							}
							
						}
					}
					if(allpartitionsegments.size() > 0)
					{
						segments = new TaskInfo[allpartitionsegments.size()];
	
						allpartitionsegments.toArray(segments);
					}
					else 
					{
							log.info("PK[" + this.pkName + "] partition job["
									+ this.jobname + "]  base infomation:datablocks="
									+ datablocks + ",.");
							return null;
						}
				}
			}
			
			
		}
		else if (!this.usepagine) // 
		{
			SplitTasks partTasks = this.buildatarangeTasks();
			if(partTasks == null || partTasks.segments == null)
			{
				return null;
			}
			segments = partTasks.segments;
			segement = partTasks.segement;
//			segments = new TaskInfo[this.datablocks];
//			if (this.startid == -9999 || this.endid == -9999) {
//				StringBuilder limitsql = new StringBuilder();
//				if (this.limitstatement == null
//						|| this.limitstatement.equals("")) {
//
//					String tableName = null;
//					if (this.schema == null) {
//						tableName = this.tablename;
//					} else
//						tableName = this.schema + "." + this.tablename;
//					limitsql.append("select min(").append(this.pkName)
//							.append(") as startid,max(").append(this.pkName)
//							.append(") as endid from ").append(tableName);
//					this.limitstatement = limitsql.toString();
//				}
//
//				log.info("Admin Data Node evaluate data scope for job["
//						+ this.jobname + "] use pk[" + this.pkName
//						+ "] partitions:" + limitstatement + " on datasource "
//						+ dbname);
//				PreparedDBUtil db = new PreparedDBUtil();
//				db.preparedSelect(this.dbname, limitstatement);
//				db.executePrepared();
//				if (db.size() > 0) {
//
//					PoolManResultSetMetaData meta = (PoolManResultSetMetaData)db.getMeta();
//					int starttype = meta.getColumnType(1);
//					
//				    if(starttype == java.sql.Types.TIMESTAMP  )
//				    {
//				    	Timestamp startid_ = db.getTimestamp(0, "startid");
//				    	Timestamp endid_ = db.getTimestamp(0, "endid");
//				    	if(startid_ == null)
//				    	{
//				    		log.info("PK[" + this.pkName + "] partition job["
//									+ this.jobname + "]  base infomation:start data id="
//									+ startid_ + ",endid=" + endid_ + ",datablocks="
//									+ datablocks + ",.");
//							return null;
//				    	}
//				    	this.pkType="timestamp";
//				    	startid = startid_.getTime();
//				    	endid = endid_.getTime();
//				    }
//				    else if(starttype == java.sql.Types.DATE)
//				    {
//				    	 Date startid_ = db.getDate(0, "startid");
//				    	Date endid_ = db.getDate(0, "endid");
//				    	if(startid_ == null)
//				    	{
//				    		log.info("PK[" + this.pkName + "] partition job["
//									+ this.jobname + "]  base infomation:start data id="
//									+ startid_ + ",endid=" + endid_ + ",datablocks="
//									+ datablocks + ",.");
//							return null;
//				    	}
//				    	this.pkType="date";
//				    	startid = startid_.getTime();
//				    	endid = endid_.getTime();
//				    }
//				    else
//				    {
//				    	startid = db.getLong(0, "startid");
//				    	endid = db.getLong(0, "endid");
//				    }
//					
//					
//				}
//			}
//			if (this.startid != -9999 && this.endid != -9999) {
//				log.info("PK[" + this.pkName + "] partition job["
//						+ this.jobname + "]  base infomation:start data id="
//						+ startid + ",endid=" + endid + ",datablocks="
//						+ datablocks);
//				
//				long datas = endid - startid + 1;
//
//				// segments[this.workservers-1] = segments[this.workservers-1] +
//				// div;
//
//				// 
//
//				if (datas > datablocks) {
//					segement = datas / this.datablocks;
//
//					long div = datas % this.datablocks;
//
//					spiltTask_(segments, startid, endid, this.datablocks,
//							segement, div, usepagine, filebasename, null);
//				} else // 
//				{
//					TaskInfo task = new TaskInfo();
//					task.startoffset = startid;
//					task.endoffset = endid;
//					task.pagesize = datas;
//					task.filename = filebasename + "_0";
//					task.taskNo = "0";
//					segement = datas;
//					segments = new TaskInfo[1];
//					segments[0] = task;
//				}
//			} else {
//				log.info("PK[" + this.pkName + "] partition job["
//						+ this.jobname + "]  base infomation:start data id="
//						+ startid + ",endid=" + endid + ",datablocks="
//						+ datablocks + ",.");
//				return null;
//			}
		} 
		else // 
		{
			segments = new TaskInfo[this.datablocks];
			if (this.tablerows <= 0) {
				StringBuilder countsql = new StringBuilder();
				if (this.countstatement == null
						|| this.countstatement.equals("")) {

					String tableName = null;
					if (this.schema == null) {
						tableName = this.tablename;
					} else
						tableName = this.schema + "." + this.tablename;
					countsql.append("select count(1) as tablerows from ")
							.append(tableName);
					this.countstatement = countsql.toString();
				}

				PreparedDBUtil db = new PreparedDBUtil();
				db.preparedSelect(this.dbname, countstatement);
				db.executePrepared();
				tablerows = db.getLong(0, 0);
			}

			long datas = tablerows;
			if (datas == 0) {
				log.info("Pagine job[" + this.jobname + "] end:tablerows="
						+ datas + ",datablocks=" + datablocks + ",");
				return null;
			}

			log.info("Pagine job[" + this.jobname
					+ "] base infomation:tablerows=" + datas + ",datablocks="
					+ datablocks);
			// segments[this.workservers-1] = segments[this.workservers-1] +
			// div;

			// 

			if (datas > datablocks) {
				segement = datas / this.datablocks;

				long div = datas % this.datablocks;
				spiltTask_(segments, 0, 0, this.datablocks, segement, div,
						usepagine, filebasename, null);
			} else {
				TaskInfo task = new TaskInfo();
				task.startoffset = 0;

				task.pagesize = datas;
				task.filename = filebasename + "_0";
				task.taskNo = "0";
				segement = datas;
				segments = new TaskInfo[1];
				segments[0] = task;
			}
		}

		splitTasks = filterTasks(segments ,segement);
		return splitTasks;
	}

	private List<String> deleteParentBlockHDFS;

	/**
	 * 
	 */
	SplitTasks buildJobSubChunks(TaskInfo taskInfo) {

		SplitTasks splitTasks = new SplitTasks();
		TaskInfo[] segments = null;
		long segement = 0l;
		long startid = taskInfo.startoffset;
		long endid = 0;
		if (deleteParentBlockHDFS == null)
			deleteParentBlockHDFS = new ArrayList<String>();
		if (!this.usepagine) {

			if(Imp.numberRange(pkType))
			{
				long datas = taskInfo.pagesize;
				endid = startid + datas - 1;
				if (datas > subblocks) {
					segments = new TaskInfo[this.subblocks];
					segement = datas / subblocks;
	
					long div = datas % this.subblocks;
	
					deleteParentBlockHDFS.add(taskInfo.filename);
					spiltTask_(segments, startid, endid, subblocks, segement, div,
							usepagine, taskInfo.filename, taskInfo.taskNo);
					if(this.usepartition)
					{
						for(int i = 0; segments != null && i < segments.length; i++)
						{
							TaskInfo t = segments[i];
							t.partitionName = taskInfo.getPartitionName(); 
							t.setIssubpartition(taskInfo.isIssubpartition());
							t.setSubpartition(taskInfo.getSubpartition());
							t.startid = taskInfo.startid;
							t.endid = taskInfo.endid;
						}
					}
					splitTasks.segement = segement;
					splitTasks.segments = segments;
					
					return splitTasks;
				} else
					return null;
			}
			else
			{
				endid = taskInfo.endoffset;
				java.util.Date startdate = Imp.getDateTime(pkType, startid);
				java.util.Date enddate = Imp.getDateTime(pkType, endid);
				
//				java.util.Calendar c = java.util.Calendar.getInstance();
//				c.setTime(startdate);
//				c.add(Calendar.DAY_OF_MONTH, datablocks);
				java.util.Date tempdate = Imp.addDays(startdate, subblocks-1, pkType);
	
				if (!Imp.reachend(tempdate,enddate)) {
					deleteParentBlockHDFS.add(taskInfo.filename);
					segement = this.subblocks;
					List<TaskInfo> tempsegments = new ArrayList<TaskInfo>();
					spiltDateTask_(tempsegments, startdate, enddate, this.subblocks,
							 taskInfo.filename, taskInfo.taskNo,-1,taskInfo.isLasted());
					segments = new TaskInfo[tempsegments.size()];
					tempsegments.toArray(segments);
					if(this.usepartition)
					{
						for(int i = 0; segments != null && i < segments.length; i++)
						{
							TaskInfo t = segments[i];
							t.partitionName = taskInfo.getPartitionName(); 
							t.setIssubpartition(taskInfo.isIssubpartition());
							t.setSubpartition(taskInfo.getSubpartition());
							t.startid = taskInfo.startid;
							t.endid = taskInfo.endid;
						}
					}
					splitTasks.segement = segement;
					splitTasks.segments = segments;
					
					return splitTasks;
				} 
				else // 
				{
					return null;
				}
			}
		} else {
			long datas = taskInfo.pagesize;
			endid = startid + datas - 1;
			if (datas > subblocks) {
				segments = new TaskInfo[this.subblocks];
				segement = datas / this.subblocks;

				long div = datas % this.subblocks;
				deleteParentBlockHDFS.add(taskInfo.filename);
				spiltTask_(segments, startid, endid, subblocks, segement, div,
						usepagine, taskInfo.filename, taskInfo.taskNo);
				splitTasks.segement = segement;
				splitTasks.segments = segments;
				return splitTasks;
			} else
				return null;
		}

	}

	public void deleteHDFSFile(String filename) throws Exception {
		Path hdfsdatafile = new Path(this.hdfsdatadir, filename);
		this.fileSystem.delete(hdfsdatafile, true);
	}

	void buildJobChunks() throws Exception {
		Address locaAddress = EventUtils.getLocalAddress();
		List<Address> allAddress = EventUtils.getRPCAddresses();
		if (locaAddress == null) {
			workservers = 1;
			rundirect = true;
		} else {
			workservers = allAddress.size();
			if (!this.adminnodeasdatanode) {
				workservers = workservers - 1;// admin
				if (workservers == 0) {
					workservers = 1;
					rundirect = true;
				} else {
					allAddress = EventUtils.removeSelf(allAddress);
				}
			}

			log.info("Data Node for job[" + this.jobname + "] workservers="
					+ workservers);
		}
		SplitTasks splitTasks = spiltTask();// 
		if (splitTasks == null || splitTasks.segments == null)
		{
			TaskConfig config = buildTaskConfig(jobname);
			JobStatic jobStatic = new JobStatic();
			String jobstaticid = java.util.UUID.randomUUID().toString();
			jobStatic.setJobstaticid(jobstaticid);
			jobStatic.setConfig(config.toString());
			jobStatic.setStatus(1);
			jobStatic.setStartTime(System.currentTimeMillis());
			jobStatic.setEndTime(System.currentTimeMillis());
			jobStatic.setJobname(jobname);
			jobStatic.setErrormsg("");
			 
			Imp.getImpStaticManager().addJobStatic(jobStatic);
			return;
		}
		TaskInfo[] segments = splitTasks.segments;
		for (int i = 0; i < segments.length; i++) {
			TaskInfo task = segments[i];
			task.setPktype(this.pkType);
			
		}
		long segement = splitTasks.segement;
		if (this.deleteParentBlockHDFS != null
				&& deleteParentBlockHDFS.size() > 0)// hdfshdfs
		{
			for (String filename : deleteParentBlockHDFS) {
				deleteHDFSFile(filename);
			}
		}
		String jobstaticid = java.util.UUID.randomUUID().toString();
		// 
		if (!this.rundirect) {
			tasks = new HashMap<String, TaskConfig>();
			int alltasks = segments.length;
			int servertasks = alltasks / workservers;
			int taskdiv = alltasks % workservers;
			 
			
			for (int i = 0; i < workservers && i < alltasks; i++) {
				TaskConfig config = buildTaskConfigWithID(jobstaticid);

				config.setPagesize(segement);
				if (i < taskdiv) {

					TaskInfo task[] = new TaskInfo[servertasks + 1];
					System.arraycopy(segments, i * servertasks + i, task, 0,
							servertasks + 1);
					config.setTasks(Arrays.asList(task));

				} else {

					TaskInfo task[] = new TaskInfo[servertasks];
					System.arraycopy(segments, i * servertasks + taskdiv, task,
							0, servertasks);
					config.setTasks(Arrays.asList(task));

				}

				tasks.put(allAddress.get(i).toString(), config);

			}
			Event<Map<String, TaskConfig>> event = new EventImpl<Map<String, TaskConfig>>(
					tasks, hdfsuploadevent);
			/**
			 * 
			 */

			EventHandle.getInstance().change(event, false);
		} else {
			TaskConfig config =buildTaskConfigWithID(jobstaticid);

			config.setPagesize(segement);
			config.setTasks(Arrays.asList(segments));
			tasks = new HashMap<String, TaskConfig>();
			tasks.put("rundirect", config);
			Event<Map<String, TaskConfig>> event = new EventImpl<Map<String, TaskConfig>>(
					tasks, hdfsuploadevent, Event.LOCAL);
			EventHandle.getInstance().change(event, false);
		}

	}

	private void initJob(BaseApplicationContext context, String jobname) {

		this.localpath = context.getStringExtendAttribute(jobname, "localdir");
		this.deletefiles = context.getStringExtendAttribute(jobname,
				"deletefiles");
		stopdbnames = context.getStringExtendAttribute(jobname, "stopdbnames");
		addworkthreads = context.getStringExtendAttribute(jobname,
				"addworkthreads");
		 
		  adjustJobname = context.getStringExtendAttribute(jobname,
				"adjustJobname");
		 
		 
		reassigntaskNode = context.getStringExtendAttribute(jobname,
				"reassigntaskNode");
		
		reassigntaskJobname = context.getStringExtendAttribute(jobname,
				"reassigntaskJobname");

		this.hdfsdatadir = context.getStringExtendAttribute(jobname,
				"hdfsdatadir");
		this.dbname = context.getStringExtendAttribute(jobname, "dbname");
		this.hdfsserver = context.getStringExtendAttribute(jobname,
				"hdfsserver");
		this.tablename = context.getStringExtendAttribute(jobname, "tablename");
		this.schema = context.getStringExtendAttribute(jobname, "schema");
		this.pkName = context.getStringExtendAttribute(jobname, "pkname");
		this.pkType = context.getStringExtendAttribute(jobname, "pktype");
		this.columns = context.getStringExtendAttribute(jobname, "columns");
		this.datablocks = context.getIntExtendAttribute(jobname, "datablocks");

		geneworkthreads = context.getIntExtendAttribute(jobname,
				"geneworkthreads", 20);
		uploadeworkthreads = context.getIntExtendAttribute(jobname,
				"uploadeworkthreads", 20);
		genqueques = context.getIntExtendAttribute(jobname, "genqueques", 5);
		uploadqueues = context
				.getIntExtendAttribute(jobname, "uploadqueues", 5);
		genquequetimewait = context.getIntExtendAttribute(jobname,
				"genquequetimewait", 10);
		uploadqueuetimewait = context.getIntExtendAttribute(jobname,
				"uploadqueuetimewait", 10);
		this.datatype = context.getStringExtendAttribute(jobname, "datatype",
				"json");
		this.genlocalfile = context.getBooleanExtendAttribute(jobname,
				"genlocalfile", false);
		this.filebasename = context.getStringExtendAttribute(jobname,
				"filebasename", tablename);
		this.clearhdfsfiles = context.getBooleanExtendAttribute(jobname,
				"clearhdfsfiles", false);

		this.tablerows = context
				.getLongExtendAttribute(jobname, "tablerows", 0);
		this.usepagine = context.getBooleanExtendAttribute(jobname,
				"usepagine", false);
		this.limitstatement = context.getStringExtendAttribute(jobname,
				"limitstatement");
		this.querystatement = context.getStringExtendAttribute(jobname,
				"querystatement");
		fetchsize = context.getIntExtendAttribute(jobname,
				"fetchsize",0);
		
		this.countstatement = context.getStringExtendAttribute(jobname,
				"countstatement");
		this.pageinestatement = context.getStringExtendAttribute(jobname,
				"pageinestatement");
		this.adminnodeasdatanode = Imp.getImpStaticManager()
				.isAdminasdatanode();
		this.jobname = jobname;
		this.startid = context
				.getLongExtendAttribute(jobname, "startid", -9999);
		this.endid = context.getLongExtendAttribute(jobname, "endid", -9999);

		blocks_str = context.getStringExtendAttribute(jobname, "blocks");

		LinkBlocks linkBlocks = buildLinkBlocks(blocks_str);
		blocks = linkBlocks.blocks;
		blocksplits = linkBlocks.blocksplits;
		excludeblocks_str = context.getStringExtendAttribute(jobname,
				"excludeblocks");
		linkBlocks = buildLinkBlocks(excludeblocks_str);
		excludeblocks = linkBlocks.blocks;
		excludeblocksplits = linkBlocks.blocksplits;
		subblocks = context.getIntExtendAttribute(jobname, "subblocks", -1);

		driver = context.getStringExtendAttribute(jobname, "driver");
		dburl = context.getStringExtendAttribute(jobname, "dburl");
		dbpassword = context
				.getStringExtendAttribute(jobname, "dbpassword", "");
		dbuser = context.getStringExtendAttribute(jobname, "dbuser", "");
		readOnly = context.getStringExtendAttribute(jobname, "readOnly");

		validatesql = context.getStringExtendAttribute(jobname, "validatesql",
				"");
		usepool = context.getBooleanExtendAttribute(jobname, "usepool", false);
		usepartition = context.getBooleanExtendAttribute(jobname,
				"usepartition",false);
		partitions = context.getStringExtendAttribute(jobname,
				"partitions");
		querypartitionstmt= context.getStringExtendAttribute(jobname,
				"querypartitionstmt");
		excludepartitions = context.getStringExtendAttribute(jobname,
				"excludepartitions");
		usesubpartition = context.getBooleanExtendAttribute(jobname,
				"usesubpartition",true);
		
		subtablename = context.getStringExtendAttribute(jobname,
				"subtablename");
		  leftJoinby = context.getStringExtendAttribute(jobname,
				"leftJoinby");
		  rightJoinby = context.getStringExtendAttribute(jobname,
				"rightJoinby");
			 subquerystatement = context.getStringExtendAttribute(jobname,
					"subquerystatement");
			 this.target = context.getStringExtendAttribute(jobname,
						"target");
			 errorrowslimit = context.getIntExtendAttribute(jobname,
						"errorrowslimit",-1);
			 this.onejob = context.getBooleanExtendAttribute(jobname,
						"single",false); 	 
			 this.rowsperfile = context.getIntExtendAttribute(jobname, "rowsperfile", 0);
			 startfileNo = context.getIntExtendAttribute(jobname, "startfileNo", -1);
			 
		this.fileSystem = HDFSServer.getFileSystem(hdfsserver);
	}

	public void executeJob(String jobname) throws Exception {
		BaseApplicationContext context = DefaultApplicationContext
				.getApplicationContext("tasks.xml");
		if (context.containsBean(jobname)) {
			executeJob(context, jobname);
		} else {
			DBJob dbjob = DBHelper.getDBJob(jobname);
			if (dbjob == null) {
				throw new Exception("" + jobname + "!");
			}
			context = new SOAApplicationContext(dbjob.getJobdef());
			executeJob(context, jobname);
		}

	}

	public void deleteData(String jobname) throws Exception {
		BaseApplicationContext context = DefaultApplicationContext
				.getApplicationContext("tasks.xml");
		initJob(context, jobname);
		Path path = new Path(hdfsdatadir);
		fileSystem.delete(path, true);

		log.info("hdfs [jobname=" + jobname + "],[hdfsdatadir="
				+ hdfsdatadir + "] on hdfsserver[" + hdfsserver + "],"
				+ "[localdir=" + localpath + "],[dbname=" + dbname
				+ "],[tablename=" + tablename + "]," + "[schema=" + schema
				+ "],[pkName=" + pkName + "],[columns=" + columns
				+ "],[datablocks=" + datablocks + "].");
	}

	public String getQuerystatement() {
		return querystatement;
	}

	public void setQuerystatement(String querystatement) {
		this.querystatement = querystatement;
	}

	public String getLimitstatement() {
		return limitstatement;
	}

	public void setLimitstatement(String limitstatement) {
		this.limitstatement = limitstatement;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	private void doDeleteFiles() throws Exception {
		JobStatic jobStatic = new JobStatic();
		String jobstaticid = java.util.UUID.randomUUID().toString();
		jobStatic.setJobstaticid(jobstaticid);
		jobStatic.setStartTime(System.currentTimeMillis());
		String[] deletefiles_ = deletefiles.split("\\,");
		for (String file : deletefiles_) {
			Path path = new Path(file);
			fileSystem.delete(path, true);
		}
		String info = "hdfs [jobname=" + jobname + "],[deletefiles="
				+ deletefiles + "] on hdfsserver[" + hdfsserver + "]";

		jobStatic.setConfig(info);
		jobStatic.setStatus(1);
		jobStatic.setEndTime(System.currentTimeMillis());
		jobStatic.setJobname(jobname);

		Imp.getImpStaticManager().addJobStatic(jobStatic);
		log.info(info + ".");
	}

	private void doStopdbnames() throws Exception {
		String jobstaticid = java.util.UUID.randomUUID().toString();
		StopDS stopds = new StopDS();
		stopds.setJobname(this.jobname);
		stopds.setStopdbnames(stopdbnames);
		stopds.setJobstaticid(jobstaticid);
		Event<StopDS> event = new EventImpl<StopDS>(stopds,
				hdfs_upload_monitor_stopdatasource_commond);
		/**
		 * 
		 */

		EventHandle.getInstance().change(event, false);
		log.info("[" + this.stopdbnames + "].");
	}
	
	private Map<String,Integer> parserAddworkthreadInfos(AddWorkthreads addWorkthreads)
	{
		String[] stres = addWorkthreads.getAddworkthreads().split("\\,");
		Map<String,Integer>  datas = new HashMap<String,Integer>(stres.length);
		for(String str:stres)
		{
			if(str.trim().length() == 0)
				continue;
			String servers[] = str.split("\\:");
			if(servers.length < 2 || servers[0].trim().length() == 0 || servers[1].trim().length() == 0)
				continue;
			
			datas.put(servers[0].trim(), Integer.parseInt(servers[1].trim()));
		}
		return datas;
		
	}
	private void doAddWorkthreads() throws Exception {
		String jobstaticid = java.util.UUID.randomUUID().toString();
		AddWorkthreads addWorkthreads = new AddWorkthreads();
		addWorkthreads.setJobname(this.jobname);
		addWorkthreads.setAddworkthreads(addworkthreads);
		addWorkthreads.setAdjustJobname(adjustJobname);
		addWorkthreads.setServerWorkthreadnums(this.parserAddworkthreadInfos(addWorkthreads));
		addWorkthreads.setJobstaticid(jobstaticid);
		Event<AddWorkthreads> event = new EventImpl<AddWorkthreads>(addWorkthreads,
				hdfs_upload_monitor_addworkthread_commond);
		/**
		 * 
		 */

		EventHandle.getInstance().change(event, false);
		log.info("[" + this.addworkthreads + "]["+adjustJobname+"].");
	}

	
	/**
	 * 
	 * 
	 * @throws Exception
	 */
	private void doReassignTasks() throws Exception {

		SpecialMonitorObject monitorJob = Imp.getImpStaticManager()
				.getSpecialMonitorObject(reassigntaskJobname);
		Map<String, JobStatic> hostJobs = monitorJob.getJobstaticsIdxByHost();
		String jobstaticid = java.util.UUID.randomUUID().toString();
		JobStatic jobstatic = hostJobs.get(this.reassigntaskNode);
		if (jobstatic.getUnruntasks() == 0  ) {
			JobStatic result = new JobStatic();
			
			result.setJobstaticid(jobstaticid);
			result.setStartTime(System.currentTimeMillis());
			result.setStatus(1);
			result.setConfig(new StringBuilder().append("reassigntaskNode=").append(reassigntaskNode).append("reassigntaskJobname=").append(this.reassigntaskJobname).toString());
			String msg = new StringBuilder().append("[").append( this.reassigntaskNode ).append( "]["
					).append( reassigntaskJobname ).append( "].").toString();
			result.setErrormsg(msg);
			result.setJobname(jobname);
			result.setEndTime(System.currentTimeMillis());
			Imp.getImpStaticManager().addJobStatic(result);
			log.info(msg);
			return;
		} else {
			Address address = EventUtils.getAddress(reassigntaskNode);
			if(address== null)
			{
				JobStatic result = new JobStatic();
				 
				result.setJobstaticid(jobstaticid);
				result.setStartTime(System.currentTimeMillis());
				result.setStatus(1);
				result.setConfig("reassigntaskNode=" + reassigntaskNode);
				String msg = "[" + this.reassigntaskNode + "]["
						+ reassigntaskJobname + "]["+reassigntaskNode+"].";
				result.setErrormsg(msg);
				result.setJobname(jobname);
				result.setEndTime(System.currentTimeMillis());
				Imp.getImpStaticManager().addJobStatic(result);
				log.info(msg);
				return;
			}
			ReassignTask reassignTask = new ReassignTask();
			reassignTask.setJobname(jobname);
			reassignTask.setReassigntaskNode(this.reassigntaskNode);
			reassignTask.setReassigntaskJobname(reassigntaskJobname);
			reassignTask.setAdminasdatanode(Imp.getImpStaticManager().isAdminasdatanode());
			reassignTask.setAdminnode(Imp.getImpStaticManager().getLocalNode());
			reassignTask.setJobstaticid(jobstaticid);
			Map<String, Integer> taskinfos = new HashMap<String, Integer>();// 
			for (Map.Entry<String, JobStatic> entry : hostJobs.entrySet()) {
				String host = entry.getKey();
				
				if(!host.equals(reassigntaskNode))//
				{
					if(host.equals(reassignTask.getAdminnode()))
					{
						if (reassignTask.isAdminasdatanode()) //
						{
							JobStatic other = entry.getValue();
							int unhandletasks = other.getWaittasks()
									+ other.getRuntasks() + other.getUnruntasks();
							taskinfos.put(host, unhandletasks);
						}
					}
					else
					{
						JobStatic other = entry.getValue();
						int unhandletasks = other.getWaittasks()
								+ other.getRuntasks() + other.getUnruntasks();
						taskinfos.put(host, unhandletasks);
					}
					
				}
			}
			reassignTask.setHostTaskInfos(taskinfos);
			
			EventTarget target = new EventTarget(address);
			Event<ReassignTask> event = new EventImpl<ReassignTask>(
					reassignTask, hdfs_upload_monitor_reassigntasks_request_commond,target);
			/**
			 * 
			 */

			EventHandle.getInstance().change(event, false);
			log.info("[" + this.reassigntaskNode + "][" + reassigntaskJobname
					+ "].");
		}
	}

	private void doUploadData() throws Exception {
		if (genlocalfile) {
			File file = new File(localpath);
			if (!file.exists())
				file.mkdirs();
		}

		Path path = new Path(hdfsdatadir);
		if (!fileSystem.exists(path))
			fileSystem.mkdirs(path);
		else {
			if (clearhdfsfiles)// hdfs
			{
				if ((excludeblocks == null || excludeblocks.length == 0)
						&& (blocks == null || blocks.length == 0)) {
					fileSystem.delete(path, true);
					fileSystem.mkdirs(path);
				}
			}
		}
		DBHelper.initDB(this);
		if(!this.onejob)
		{
			buildJobChunks();
		}
		else
		{
			doOnejob();
		}
		log.info("[jobname=" + jobname + "],[hdfsdatadir="
				+ hdfsdatadir + "] on hdfsserver[" + hdfsserver + "],"
				+ "[localdir=" + localpath + "],[dbname=" + dbname
				+ "],[tablename=" + tablename + "]," + "[schema=" + schema
				+ "],[pkName=" + pkName + "],[columns=" + columns
				+ "],[datablocks=" + datablocks + "].");
	}
	
	private void doOnejob()
	{
		String jobstaticid = java.util.UUID.randomUUID().toString();
		TaskConfig config = buildTaskConfigWithID(jobstaticid);
		tasks = new HashMap<String, TaskConfig>();
		if(SimpleStringUtil.isEmpty(target))
		{
			if(Imp.getImpStaticManager().isAdminasdatanode())
			{
				tasks.put("rundirect", config);
				Event<Map<String, TaskConfig>> event = new EventImpl<Map<String, TaskConfig>>(
						tasks, hdfsuploadevent, Event.LOCAL);
				EventHandle.getInstance().change(event, false);
			}
			else
			{
				List<Address> ads = EventUtils.getRPCAddresses();
				Address local = EventUtils.getLocalAddress();
				Address tgt = null;
				for(int i =0; i < ads.size(); i ++)
				{
					Address tmp = ads.get(i);
					if(!tmp.toString().equals(local.toString()))
					{
						tgt = tmp;
						break;
					}
				}
				if(tgt != null)
				{
					tasks.put(tgt.toString(), config);
					Address address = tgt;
					EventTarget target = new EventTarget(address);
					Event<Map<String, TaskConfig>> event = new EventImpl<Map<String, TaskConfig>>(
							tasks, hdfsuploadevent,target);
					EventHandle.getInstance().change(event, false);
				}
				else
				{
					JobStatic jobStatic = new JobStatic();
					
					jobStatic.setJobstaticid(jobstaticid);
					jobStatic.setStartTime(System.currentTimeMillis());
					 
					

					jobStatic.setConfig(config.toString());
					jobStatic.setStatus(2);
					jobStatic.setEndTime(System.currentTimeMillis());
					jobStatic.setJobname(jobname);
					jobStatic.setErrormsg("");
					Imp.getImpStaticManager().addJobStatic(jobStatic);
					log.info(jobname+".");
				}
			}
		}
		else
		{
			tasks.put(target, config);
			Address address = EventUtils.getAddress(this.target);
			EventTarget target = new EventTarget(address);
			Event<Map<String, TaskConfig>> event = new EventImpl<Map<String, TaskConfig>>(
					tasks, hdfsuploadevent,target);
			EventHandle.getInstance().change(event, false);
		}
		
	}

	private void runjob(BaseApplicationContext ioccontext, String jobname) {
		long start = System.currentTimeMillis();
		try {
			initJob(ioccontext, jobname);
			if (this.deletefiles != null && !this.deletefiles.trim().equals(""))// 
			{
				doDeleteFiles();
			} 
			 else if (this.addworkthreads != null
						&& !this.addworkthreads.trim().equals("")) {
					this.doAddWorkthreads();;
				} 
			else if (this.stopdbnames != null
					&& !this.stopdbnames.trim().equals("")) {
				this.doStopdbnames();
			} else if (this.reassigntaskNode != null
					&& !this.reassigntaskNode.trim().equals("")) {
				this.doReassignTasks();
			} 
			else // 
			{
				doUploadData();
			}
		} catch (IllegalArgumentException e) {
			TaskConfig config = buildTaskConfig(jobname);
			JobStatic jobStatic = new JobStatic();	
			String jobstaticid = java.util.UUID.randomUUID().toString();
			jobStatic.setJobstaticid(jobstaticid);
			jobStatic.setConfig(config.toString());
			jobStatic.setStatus(2);
			jobStatic.setStartTime(start);
			jobStatic.setEndTime(System.currentTimeMillis());
			jobStatic.setJobname(jobname);
			jobStatic.setErrormsg(SimpleStringUtil.exceptionToString(e));
			Imp.getImpStaticManager().addJobStatic(jobStatic);
			log.error("", e);
		} catch (IOException e) {
			TaskConfig config = buildTaskConfig(jobname);
			JobStatic jobStatic = new JobStatic();
			String jobstaticid = java.util.UUID.randomUUID().toString();
			jobStatic.setJobstaticid(jobstaticid);
			jobStatic.setConfig(config.toString());
			jobStatic.setStatus(2);
			jobStatic.setStartTime(start);
			jobStatic.setEndTime(System.currentTimeMillis());
			jobStatic.setJobname(jobname);
			jobStatic.setErrormsg(SimpleStringUtil.exceptionToString(e));
			Imp.getImpStaticManager().addJobStatic(jobStatic);
			log.error("", e);
		} catch (Exception e) {
			TaskConfig config = buildTaskConfig(jobname);
			JobStatic jobStatic = new JobStatic();
			String jobstaticid = java.util.UUID.randomUUID().toString();
			jobStatic.setJobstaticid(jobstaticid);
			jobStatic.setConfig(config.toString());
			jobStatic.setStatus(2);
			jobStatic.setStartTime(start);
			jobStatic.setEndTime(System.currentTimeMillis());
			jobStatic.setJobname(jobname);
			jobStatic.setErrormsg(SimpleStringUtil.exceptionToString(e));
			Imp.getImpStaticManager().addJobStatic(jobStatic);
			log.error("", e);
		}
	}

	public void executeJob(final BaseApplicationContext ioccontext,
			final String jobname) throws Exception {
		new Thread(new Runnable() {
			public void run() {
				runjob(ioccontext, jobname);
			}
		}).start();

	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getDburl() {
		return dburl;
	}

	public void setDburl(String dburl) {
		this.dburl = dburl;
	}

	public String getDbpassword() {
		return dbpassword;
	}

	public void setDbpassword(String dbpassword) {
		this.dbpassword = dbpassword;
	}

	public String getDbuser() {
		return dbuser;
	}

	public void setDbuser(String dbuser) {
		this.dbuser = dbuser;
	}

	public String getValidatesql() {
		return validatesql;
	}

	public void setValidatesql(String validatesql) {
		this.validatesql = validatesql;
	}

	public boolean isUsepool() {
		return usepool;
	}

	public void setUsepool(boolean usepool) {
		this.usepool = usepool;
	}

	public String getHADOOP_PATH() {
		return HADOOP_PATH;
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public String getHdfsserver() {
		return hdfsserver;
	}

	public String getHdfsdatadir() {
		return hdfsdatadir;
	}

	public String getLocalpath() {
		return localpath;
	}

	public String getTablename() {
		return tablename;
	}

	public String getSchema() {
		return schema;
	}

	public String getPkName() {
		return pkName;
	}

	public String getColumns() {
		return columns;
	}

	public String getFilebasename() {
		return filebasename;
	}

	public int getDatablocks() {
		return datablocks;
	}

	public String getDbname() {
		return dbname;
	}

	public int getWorkservers() {
		return workservers;
	}

	public boolean isRundirect() {
		return rundirect;
	}

	public int getGeneworkthreads() {
		return geneworkthreads;
	}

	public int getUploadeworkthreads() {
		return uploadeworkthreads;
	}

	public int getGenqueques() {
		return genqueques;
	}

	public int getUploadqueues() {
		return uploadqueues;
	}

	public int getGenquequetimewait() {
		return genquequetimewait;
	}

	public int getUploadqueuetimewait() {
		return uploadqueuetimewait;
	}

	public long getStartid() {
		return startid;
	}

	public long getEndid() {
		return endid;
	}

	public String getDatatype() {
		return datatype;
	}

	public boolean isGenlocalfile() {
		return genlocalfile;
	}

	public boolean isClearhdfsfiles() {
		return clearhdfsfiles;
	}

	public String getCountstatement() {
		return countstatement;
	}

	public String getPageinestatement() {
		return pageinestatement;
	}

	public long getTablerows() {
		return tablerows;
	}

	public boolean isUsepagine() {
		return usepagine;
	}

	public boolean isAdminnodeasdatanode() {
		return adminnodeasdatanode;
	}

	public int[] getBlocks() {
		return blocks;
	}

	public int getSubblocks() {
		return subblocks;
	}

	public Map<String, TaskConfig> getTasks() {
		return tasks;
	}

	public List<String> getDeleteParentBlockHDFS() {
		return deleteParentBlockHDFS;
	}

	public Map<String, List<Integer>> getBlocksplits() {
		return blocksplits;
	}

	public String isReadOnly() {
		// TODO Auto-generated method stub
		return readOnly;
	}

	public String getDeletefiles() {
		return deletefiles;
	}

	public void setDeletefiles(String deletefiles) {
		this.deletefiles = deletefiles;
	}

	public String getStopdbnames() {
		return stopdbnames;
	}

	public void setStopdbnames(String stopdbnames) {
		this.stopdbnames = stopdbnames;
	}

	public String getReassigntaskNode() {
		return reassigntaskNode;
	}

	public void setReassigntaskNode(String reassigntaskNode) {
		this.reassigntaskNode = reassigntaskNode;
	}

	public String getReassigntaskJobname() {
		return reassigntaskJobname;
	}

	public void setReassigntaskJobname(String reassigntaskJobname) {
		this.reassigntaskJobname = reassigntaskJobname;
	}

	public String getSubtablename() {
		return subtablename;
	}

	public void setSubtablename(String subtablename) {
		this.subtablename = subtablename;
	}

	public boolean isUsepartition() {
		return usepartition;
	}

	public void setUsepartition(boolean usepartition) {
		this.usepartition = usepartition;
	}

	

	public String getSubquerystatement() {
		return subquerystatement;
	}

	public void setSubquerystatement(String subquerystatement) {
		this.subquerystatement = subquerystatement;
	}

	public String getLeftJoinby() {
		return leftJoinby;
	}

	public void setLeftJoinby(String leftJoinby) {
		this.leftJoinby = leftJoinby;
	}

	public String getRightJoinby() {
		return rightJoinby;
	}

	public void setRightJoinby(String rightJoinby) {
		this.rightJoinby = rightJoinby;
	}

	public boolean isOnejob() {
		return onejob;
	}

	public void setOnejob(boolean onejob) {
		this.onejob = onejob;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public int getRowsperfile() {
		return rowsperfile;
	}

	public void setRowsperfile(int rowsperfile) {
		this.rowsperfile = rowsperfile;
	}

	public String getPartitions() {
		return partitions;
	}

	public void setPartitions(String partitions) {
		this.partitions = partitions;
	}

	public String getExcludepartitions() {
		return excludepartitions;
	}

	public void setExcludepartitions(String excludepartitions) {
		this.excludepartitions = excludepartitions;
	}

	public boolean isUsesubpartition() {
		return usesubpartition;
	}

	public void setUsesubpartition(boolean usesubpartition) {
		this.usesubpartition = usesubpartition;
	}

	public String getPkType() {
		return pkType;
	}

	public void setPkType(String pkType) {
		this.pkType = pkType;
	}

	public String getAddworkthreads() {
		return addworkthreads;
	}

	public void setAddworkthreads(String addworkthreads) {
		this.addworkthreads = addworkthreads;
	}

	public int getFetchsize() {
		return fetchsize;
	}

	public void setFetchsize(int fetchsize) {
		this.fetchsize = fetchsize;
	}

}
