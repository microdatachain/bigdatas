package org.frameworkset.bigdata.imp.monitor;

import java.util.Map;


public class HostJobs  implements java.io.Serializable,java.lang.Cloneable{
	private Map<String,JobStatic> jobs;

	private long datanodeTimestamp ;
	public Map<String, JobStatic> getJobs() {
		return jobs;
	}
	public void setJobs(Map<String, JobStatic> jobs) {
		this.jobs = jobs;
	}
	public long getDatanodeTimestamp() {
		return datanodeTimestamp;
	}
	public void setDatanodeTimestamp(long datanodeTimestamp) {
		this.datanodeTimestamp = datanodeTimestamp;
	}
	@Override
	public Object clone() throws CloneNotSupportedException {
		HostJobs ret = new HostJobs();
		ret.datanodeTimestamp = this.datanodeTimestamp;
		ret.jobs = ImpStaticManager.cloneStaticData(jobs);
		return ret;
	}
}
