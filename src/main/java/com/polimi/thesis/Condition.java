package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

public class Condition {
	private int id;
	private String type;
	private int start;
	private String condition;
	private int end;
	private List<String> firstMethod;
	private List<Integer> firstMethodPos;
	private List<String> lastMethod;
	private List<Integer> lastMethodPos;
	private List<Integer> idParentCondition;
	public Condition(int id, String type, int start, String condition, int end, String firstMethod, int firstMethodPos, String lastMethod,
			int lastMethodPos, List<Integer> idParentCondition) {
		this.id = id;
		this.setType(type);
		this.start = start;
		this.condition = condition;
		this.end = end;
		this.firstMethod = new ArrayList<String>();
		this.firstMethod.add(firstMethod);
		this.firstMethodPos = new ArrayList<Integer>();
		this.firstMethodPos.add(firstMethodPos);
		this.lastMethod = new ArrayList<String>();
		this.lastMethod.add(lastMethod);
		this.lastMethodPos = new ArrayList<Integer>();
		this.lastMethodPos.add(lastMethodPos);
		this.setIdParentCondition(idParentCondition);
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public List<String> getFirstMethod() {
		return firstMethod;
	}
	public void setFirstMethod(List<String> firstMethod) {
		this.firstMethod = firstMethod;
	}
	public void addFirstMethod(String firstMethod) {
		if(!this.firstMethod.contains(firstMethod))
			this.firstMethod.add(firstMethod);
	}
	public List<Integer> getFirstMethodPos() {
		return firstMethodPos;
	}
	public void setFirstMethodPos(List<Integer> firstMethodPos) {
		this.firstMethodPos = firstMethodPos;
	}
	public void addFirstMethodPos(int firstMethodPos) {
		if(!this.firstMethodPos.contains(firstMethodPos))
			this.firstMethodPos.add(firstMethodPos);
	}
	public List<String> getLastMethod() {
		return lastMethod;
	}
	public void setLastMethod(List<String> lastMethod) {
		this.lastMethod = lastMethod;
	}
	public void addLastMethod(String lastMethod) {
		if(!this.lastMethod.contains(lastMethod))
			this.lastMethod.add(lastMethod);
	}
	public List<Integer> getLastMethodPos() {
		return lastMethodPos;
	}
	public void setLastMethodPos(List<Integer> lastMethodPos) {
		this.lastMethodPos = lastMethodPos;
	}
	public void addLastMethodPos(int lastMethodPos) {
		if(!this.lastMethodPos.contains(lastMethodPos))
			this.lastMethodPos.add(lastMethodPos);
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<Integer> getIdParentCondition() {
		return idParentCondition;
	}
	public void setIdParentCondition(List<Integer> idParentCondition) {
		this.idParentCondition = idParentCondition;
	}
	public Boolean isNested(){
		return idParentCondition.size() > 0;
	}
}
