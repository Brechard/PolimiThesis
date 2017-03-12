package com.polimi.thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Job {
	
	private int jobId;
	private String callSite;
	private List<String> condition;
	private List<String> loop;
	private Map<Integer, Stage> stages;
		
	public Job(int id, String callSite) {
		jobId = id;
		this.callSite = callSite;
		stages = new HashMap<Integer, Stage>();
	}	
	public int getId() {
		return jobId;
	}
	public void setId(int id) {
		jobId = id;
	}
	public List<String> getCondition() {
		return condition;
	}
	public void setCondition(List<String> condition) {			
		this.condition = condition;
	}
	public List<String> getLoop() {
		return loop;
	}
	public void setLoop(List<String> loop) {
		this.loop = loop;
	}
	public Map<Integer, Stage> getStages() {
		return stages;
	}
	public void setStages(Map<Integer, Stage> stages) {
		this.stages = stages;
	}
	public String getCallSite(){
		return callSite;
	}
	public void addCondition(String cond){
		if(condition == null) 
			condition = new ArrayList<String>();
		condition.add(cond);
	}
	public void addLoop(String loop){
		if(this.loop == null) 
			this.loop = new ArrayList<String>();
		this.loop.add(loop);
	}		
}
