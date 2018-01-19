package org.frameworkset.bigdata.imp;

import java.util.List;
import java.util.Map;

public class TaskConfig implements java.io.Serializable{
	private String jobstaticid ;
	/**
	 * 
	 */
	boolean onejob;
	String target ;
	int rowsperfile;
	int startfileNo;
	
	private String jobdef; 
	/**
	 * -joinbytablename
	 */
	private String subtablename;
	/**
	 * 
	 */
	private boolean usepartition;
	
	private boolean partitiondataraged;
	private String partitions;
	private String querypartitionstmt;
	private String excludepartitions;
	private boolean usesubpartition = true;
	private String leftJoinby;
	private String rightJoinby;
	/**
	 * 
	 */
	String driver;
	String dburl;
	String dbpassword;
	String dbuser;
	String validatesql;
	/**
	 * 
	 */
	boolean usepool;
	private String readOnly;
	private String deletefiles;
	Map<String,List<Integer>> blocksplits; 
	/**
	 * dbname
	 * dbname
	 */
	String dbname;
	String tablename;
	String columns;
	String pkname;
	String pktype;
	int datablocks;
	String hdfsserver;
	String hdfsdatadirpath;
	String localdirpath;
	long pagesize;
	String filebasename;
	int geneworkthreads=20;
    int uploadeworkthreads=20;
	int genqueques=5;
	int uploadqueues=5;
	int genquequetimewait=10;
	int uploadqueuetimewait=10;
	boolean genlocalfile;
	String datatype;
	private String querystatement;
	int fetchsize;
	/**
	 * 
	 */
	private int errorrowslimit =-1;
	/**
	 * sql
	 */
	String subquerystatement;
	String limitstatement;
	String countstatement;
 	String pageinestatement;
 	long tablerows;
 	boolean usepagine = false;
	boolean adminnodeasdatanode;
	List<TaskInfo> tasks;
	public String schema;
	String jobname;
	public boolean clearhdfsfiles;
	public long startid;
	public long endid;
	public String blocks;
	String excludeblocks;
	/**
	 * 
	 */
	private String stopdbnames;
	private String addworkthreads;
	
	private String adjustJobname;

	/**
	 * blocks
	 */
	/**
	 * 
	 */
	private String reassigntaskNode;
	
	private String reassigntaskJobname;
	int subblocks;
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		
		builder.append("jobname=").append(jobname).append(",")
		.append("dbname=").append(dbname).append(",")
				.append("schema=").append(this.schema).append(",")
		.append("tablename=").append(tablename).append(",")
		.append("columns=[").append(columns).append("],")
		
		.append("subtablename=").append(this.subtablename).append(",")
		.append("leftjoinby=").append(this.leftJoinby).append(",")
		.append("rightJoinby=").append(this.rightJoinby).append(",")
		.append("errorrowslimit=").append(this.errorrowslimit).append(",")
		.append("fetchsize=").append(this.fetchsize).append(",")
		.append("usepartition=").append(this.usepartition).append(",");
		if(this.usepartition)
		{
			builder.append("excludepartitions=").append(this.excludepartitions).append(",")
			.append("partitions=").append(this.partitions).append(",");
			if(this.querypartitionstmt != null)
			{
				builder.append("querypartitionstmt=").append(this.querypartitionstmt).append(",");
			}
		}
		
		if(driver != null && driver.trim().length() > 0)
		{
			builder.append("driver=").append(driver).append(",")
					.append("dburl=").append(this.dburl).append(",")
			.append("dbpassword=").append(dbpassword).append(",")
			.append("dbuser=[").append(dbuser).append("],")
			.append("usepool=[").append(usepool).append("],")
			.append("readOnly=[").append(readOnly).append("],")
			.append("validatesql=").append(validatesql).append(",");
			
		}
		
	
		if(this.onejob)
		{
			builder.append("onejob=").append(onejob).append(",")
					.append("target=").append(this.target).append(",")
			.append("rowsperfile=").append(rowsperfile).append(",");
			
		}
		
		builder.append("datablocks=").append(datablocks).append(",")
		.append("hdfsserver=").append(hdfsserver).append(",")
		.append("hdfsdatadirpath=").append(hdfsdatadirpath).append(",")
		.append("localdirpath=").append(localdirpath).append(",")
		.append("pagesize=").append(pagesize).append(",")
		.append("filebasename=").append(filebasename).append(",")
		.append("geneworkthreads=").append(geneworkthreads).append(",")
		.append("uploadeworkthreads=").append(uploadeworkthreads).append(",")
		.append("genqueques=").append(genqueques).append(",")
		.append("uploadqueues=").append(uploadqueues).append(",")
		.append("genquequetimewait=").append(genquequetimewait).append(",")
		.append("uploadqueuetimewait=").append(uploadqueuetimewait).append(",")
		.append("genlocalfile=").append(genlocalfile).append(",")
		.append("datatype=").append(datatype).append(",")
		.append("usepagine=").append(usepagine).append(",")
		.append("countstatement=").append(countstatement).append(",")
		.append("pageinestatement=").append(pageinestatement).append(",")
			.append("tablerows=").append(tablerows).append(",")
		.append("querystatement=").append(querystatement).append(",")
		.append("subquerystatement=").append(subquerystatement).append(",")
		.append("adminnodeasdatanode=").append(adminnodeasdatanode).append(",")
		.append("limitstatement=").append(limitstatement).append(",")
		.append("startid=").append(startid).append(",")
		.append("endid=").append(endid).append(",")
		.append("blocks=").append(blocks).append(",")
		.append("excludeblocks=").append(excludeblocks).append(",")
		.append("clearhdfsfiles=").append(clearhdfsfiles);
//		if(blocks != null && blocks.trim().length() > 0)
			builder.append(",").append("subblocks=").append(subblocks).append(",");
		if(deletefiles != null)
			builder.append("deletefiles=").append(deletefiles).append(",");
		
		if(this.stopdbnames != null)
			builder.append("stopdbnames=").append(stopdbnames).append(",");
		
		if(this.reassigntaskNode != null)
			builder.append("reassigntaskNode=").append(reassigntaskNode).append(",").append("reassigntaskJobname=").append(this.reassigntaskJobname);
		return builder.toString();
		
		
	}
	public String getDbname() {
		return dbname;
	}
	public void setDbname(String dbname) {
		this.dbname = dbname;
	}
	public String getTablename() {
		return tablename;
	}
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getPkname() {
		return pkname;
	}
	public void setPkname(String pkname) {
		this.pkname = pkname;
	}
	public int getDatablocks() {
		return datablocks;
	}
	public void setDatablocks(int datablocks) {
		this.datablocks = datablocks;
	}
	public String getHdfsserver() {
		return hdfsserver;
	}
	public void setHdfsserver(String hdfsserver) {
		this.hdfsserver = hdfsserver;
	}
	public String getHdfsdatadirpath() {
		return hdfsdatadirpath;
	}
	public void setHdfsdatadirpath(String hdfsdatadirpath) {
		this.hdfsdatadirpath = hdfsdatadirpath;
	}
	public String getLocaldirpath() {
		return localdirpath;
	}
	public void setLocaldirpath(String localdirpath) {
		this.localdirpath = localdirpath;
	}
	public String getFilebasename() {
		return filebasename;
	}
	public void setFilebasename(String filebasename) {
		this.filebasename = filebasename;
	}
	public int getGeneworkthreads() {
		return geneworkthreads;
	}
	public void setGeneworkthreads(int geneworkthreads) {
		this.geneworkthreads = geneworkthreads;
	}
	public int getUploadeworkthreads() {
		return uploadeworkthreads;
	}
	public void setUploadeworkthreads(int uploadeworkthreads) {
		this.uploadeworkthreads = uploadeworkthreads;
	}
	public int getGenqueques() {
		return genqueques;
	}
	public void setGenqueques(int genqueques) {
		this.genqueques = genqueques;
	}
	public int getUploadqueues() {
		return uploadqueues;
	}
	public void setUploadqueues(int uploadqueues) {
		this.uploadqueues = uploadqueues;
	}
	public int getGenquequetimewait() {
		return genquequetimewait;
	}
	public void setGenquequetimewait(int genquequetimewait) {
		this.genquequetimewait = genquequetimewait;
	}
	public int getUploadqueuetimewait() {
		return uploadqueuetimewait;
	}
	public void setUploadqueuetimewait(int uploadqueuetimewait) {
		this.uploadqueuetimewait = uploadqueuetimewait;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public List<TaskInfo> getTasks() {
		return tasks;
	}
	public void setTasks(List<TaskInfo> tasks) {
		this.tasks = tasks;
	}
	public long getPagesize() {
		return pagesize;
	}
	public void setPagesize(long pagesize) {
		this.pagesize = pagesize;
	}
	public boolean isGenlocalfile() {
		return genlocalfile;
	}
	public void setGenlocalfile(boolean genlocalfile) {
		this.genlocalfile = genlocalfile;
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
	public String getCountstatement() {
		return countstatement;
	}
	public void setCountstatement(String countstatement) {
		this.countstatement = countstatement;
	}
	public long getTablerows() {
		return tablerows;
	}
	public void setTablerows(long tablerows) {
		this.tablerows = tablerows;
	}
	public boolean isUsepagine() {
		return usepagine;
	}
	public void setUsepagine(boolean usepagine) {
		this.usepagine = usepagine;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getPageinestatement() {
		return pageinestatement;
	}
	public void setPageinestatement(String pageinestatement) {
		this.pageinestatement = pageinestatement;
	}
	public boolean isAdminnodeasdatanode() {
		return adminnodeasdatanode;
	}
	public void setAdminnodeasdatanode(boolean adminnodeasdatanode) {
		this.adminnodeasdatanode = adminnodeasdatanode;
	}
	public String getJobname() {
		return jobname;
	}
	public void setJobname(String jobname) {
		this.jobname = jobname;
	}
	public int getSubblocks() {
		return subblocks;
	}
	public void setSubblocks(int subblocks) {
		this.subblocks = subblocks;
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
	public boolean isClearhdfsfiles() {
		return clearhdfsfiles;
	}
	public void setClearhdfsfiles(boolean clearhdfsfiles) {
		this.clearhdfsfiles = clearhdfsfiles;
	}
	public long getStartid() {
		return startid;
	}
	public void setStartid(long startid) {
		this.startid = startid;
	}
	public long getEndid() {
		return endid;
	}
	public void setEndid(long endid) {
		this.endid = endid;
	}
	public String getBlocks() {
		return blocks;
	}
	public void setBlocks(String blocks) {
		this.blocks = blocks;
	}
	public Map<String, List<Integer>> getBlocksplits() {
		return blocksplits;
	}
	public void setBlocksplits(Map<String, List<Integer>> blocksplits) {
		this.blocksplits = blocksplits;
	}
	public boolean isUsepool() {
		return usepool;
	}
	public void setUsepool(boolean usepool) {
		this.usepool = usepool;
	}
	public String getReadOnly() {
		return readOnly;
	}
	public void setReadOnly(String readOnly) {
		this.readOnly = readOnly;
	}
	public String getJobdef() {
		return jobdef;
	}
	public void setJobdef(String jobdef) {
		this.jobdef = jobdef;
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
	public int getStartfileNo() {
		return startfileNo;
	}
	public void setStartfileNo(int startfileNo) {
		this.startfileNo = startfileNo;
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
	public String getJobstaticid() {
		return jobstaticid;
	}
	public void setJobstaticid(String jobstaticid) {
		this.jobstaticid = jobstaticid;
	}
	public int getErrorrowslimit() {
		return errorrowslimit;
	}
	public void setErrorrowslimit(int errorrowslimit) {
		this.errorrowslimit = errorrowslimit;
	}
	public boolean isUsesubpartition() {
		return usesubpartition;
	}
	public void setUsesubpartition(boolean usesubpartition) {
		this.usesubpartition = usesubpartition;
	}
	public boolean isPartitiondataraged() {
		return partitiondataraged;
	}
	public void setPartitiondataraged(boolean partitiondataraged) {
		this.partitiondataraged = partitiondataraged;
	}
	public String getPktype() {
		return pktype;
	}
	public void setPktype(String pktype) {
		this.pktype = pktype;
	}
	public String getAddworkthreads() {
		return addworkthreads;
	}
	public void setAddworkthreads(String addworkthreads) {
		this.addworkthreads = addworkthreads;
	}
	public String getAdjustJobname() {
		return adjustJobname;
	}
	public void setAdjustJobname(String adjustJobname) {
		this.adjustJobname = adjustJobname;
	}
	public String getQuerypartitionstmt() {
		return querypartitionstmt;
	}
	public void setQuerypartitionstmt(String querypartitionstmt) {
		this.querypartitionstmt = querypartitionstmt;
	}
	public int getFetchsize() {
		return fetchsize;
	}
	public void setFetchsize(int fetchsize) {
		this.fetchsize = fetchsize;
	}
	
//	 public boolean dateRange()
//	 {
//		 return getPktype() != null && getPktype().equals("date"); 
//	 }
//	 
//	 public boolean timestampRange()
//	 {
//		 return getPktype() != null && getPktype().equals("timestamp"); 
//	 }
}

