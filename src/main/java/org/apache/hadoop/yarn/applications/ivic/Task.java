package org.apache.hadoop.yarn.applications.ivic;

public class Task {
	private String id = null;
	private String uuid = null;
	private String jobId = null;
	private String title = null;
	private String descrition = null;
	private String status = null;
	private String dependingTaskId = null;
	private String taskInfo = null;
	private String targetObjectId = null;
	private String targetObjectType = null;
	private String operation = null;
	private String soapMethod = null;
	private String soapArgs = null;
	private String shellArgs = null;
	private String createdTime;
	private int memory = 10;
	private int vcpu = 1;
	
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getUuid() {
        return uuid;
    }
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
    public String getJobId() {
        return jobId;
    }
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getDescrition() {
        return descrition;
    }
    public void setDescrition(String descrition) {
        this.descrition = descrition;
    }
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getDependingTaskId() {
        return dependingTaskId;
    }
    public void setDependingTaskId(String dependingTaskId) {
        this.dependingTaskId = dependingTaskId;
    }
    public String getTaskInfo() {
        return taskInfo;
    }
    public void setTaskInfo(String taskInfo) {
        this.taskInfo = taskInfo;
    }
    public String getTargetObjectId() {
        return targetObjectId;
    }
    public void setTargetObjectId(String targetObjectId) {
        this.targetObjectId = targetObjectId;
    }
    public String getTargetObjectType() {
        return targetObjectType;
    }
    public void setTargetObjectType(String targetObjectType) {
        this.targetObjectType = targetObjectType;
    }
    public String getOperation() {
        return operation;
    }
    public void setOperation(String operation) {
        this.operation = operation;
    }
    public String getSoapMethod() {
        return soapMethod;
    }
    public void setSoapMethod(String soapMethod) {
        this.soapMethod = soapMethod;
    }
    public String getSoapArgs() {
        return soapArgs;
    }
    public void setSoapArgs(String soapArgs) {
        this.soapArgs = soapArgs;
    }
    public String getShellArgs() {
        return shellArgs;
    }
    public void setShellArgs(String shellArgs) {
        this.shellArgs = shellArgs;
    }
    public String getCreatedTime() {
        return createdTime;
    }
    public void setCreatedTime(String createdTime) {
        this.createdTime = createdTime;
    }
    public int getMemory() {
        return memory;
    }
    public void setMemory(int memory) {
        this.memory = memory;
    }
    public int getVcpu() {
        return vcpu;
    }
    public void setVcpu(int vcpu) {
        this.vcpu = vcpu;
    }
	
}
