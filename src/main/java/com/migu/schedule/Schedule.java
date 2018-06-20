package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
*类名和方法不能修改
 */
public class Schedule {

	private Map<Integer,ServerInfo> servers;
	
	private Queue<QueueEntity> taskQueue;
	
	class QueueEntity{
		int taskId;
		int consumption;
		public QueueEntity(int taskId, int consumption){
			this.taskId=taskId;
			this.consumption=consumption;
		}
		public int getTaskId() {
			return taskId;
		}
		public void setTaskId(int taskId) {
			this.taskId = taskId;
		}
		public int getConsumption() {
			return consumption;
		}
		public void setConsumption(int consumption) {
			this.consumption = consumption;
		}
	}
	
	class ServerInfo{
		int serverConsumption;
		Map<Integer,QueueEntity> taskEntry;
		ServerInfo(){
			taskEntry=new HashMap<Integer,QueueEntity>();
			serverConsumption=0;
		}
		void add(int taskId,int consumption){
			QueueEntity q=new QueueEntity(taskId,consumption);
			taskEntry.put(taskId, q);
		}
		public int getServerConsumption() {
			return serverConsumption;
		}
		public void setServerConsumption(int serverConsumption) {
			this.serverConsumption = serverConsumption;
		}
		public Map<Integer, QueueEntity> getTaskEntry() {
			return taskEntry;
		}
		public void setTaskEntry(Map<Integer, QueueEntity> taskEntry) {
			this.taskEntry = taskEntry;
		}
	}
	
    public int init() {
        // TODO 方法未实现
    	servers=new HashMap<Integer, ServerInfo>();
    	taskQueue=new LinkedList<QueueEntity>();
    	return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        // TODO 方法未实现
    	if(nodeId<=0){
    		return ReturnCodeKeys.E004;
    	}else if(servers.containsKey(nodeId)){
    		return ReturnCodeKeys.E005;
    	}
    	servers.put(nodeId, new ServerInfo());
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        // TODO 方法未实现
    	if(nodeId<=0){
    		return ReturnCodeKeys.E004;
    	}else if(!servers.containsKey(nodeId)){
    		return ReturnCodeKeys.E007;
    	}
    	if(servers.get(nodeId)!=null){
    		for(Map.Entry<Integer, QueueEntity> q:servers.get(nodeId).getTaskEntry().entrySet()){
    			taskQueue.offer(q.getValue());
    		}
    	}
    	servers.remove(nodeId);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        // TODO 方法未实现
    	if(taskId<=0){
    		return ReturnCodeKeys.E009;
    	}else if(queueContainsTask(taskId)){
    		return ReturnCodeKeys.E010;
    	}
    	QueueEntity q=new QueueEntity(taskId, consumption);
    	taskQueue.offer(q);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        // TODO 方法未实现
    	if(taskId<=0){
    		return ReturnCodeKeys.E009;
    	}else if(!queueContainsTask(taskId)){
    		return ReturnCodeKeys.E012;
    	}
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        // TODO 方法未实现
    	if(taskQueue.size()>0){
    		
    	}
        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        // TODO 方法未实现
    	if(tasks==null){
    		return ReturnCodeKeys.E016;
    	}
        return ReturnCodeKeys.E015;
    }
    
    private int containsTask(int taskId){
    	for(Map.Entry<Integer, ServerInfo> entry:servers.entrySet()){
    		if(entry.getValue().getTaskEntry().containsKey(taskId)){
    			return entry.getKey();
    		}
    	}
    	return -1;
    }
    
    private boolean queueContainsTask(int taskId){
    	for(QueueEntity q:taskQueue){
    		if(q.getTaskId()==taskId){
    			return true;
    		}
    	}
    	return false;
    }
}
