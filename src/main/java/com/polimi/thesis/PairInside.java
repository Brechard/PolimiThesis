package com.polimi.thesis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PairInside{
    private Boolean inside;
    private Boolean isFirst;
    private Boolean isLast;
    private List<String> condition;
    private List<Integer> idIfs;
    private List<Integer> idIfsFirst;
    private List<Integer> idIfsLast;
    private String type;
    public PairInside(Boolean inside, List<String> condition) {
        this.setInside(inside);
        this.setCondition(condition);
        idIfs = new ArrayList<Integer>();
        idIfsFirst = new ArrayList<Integer>();
        idIfsLast = new ArrayList<Integer>();
        isFirst = false;
        isLast = false;
    }

	public Boolean isInside() {
		return inside;
	}

	public void setInside(Boolean inside) {
		this.inside = inside;
	}

	public List<String> getCondition() {
		return condition;
	}

	public void setCondition(List<String> condition) {
		this.condition = condition;
	}

	public Boolean getIsFirst() {
		return isFirst;
	}

	public void setIsFirst(Boolean isFirst) {
		this.isFirst = isFirst;
	}

	public Boolean getIsLast() {
		return isLast;
	}

	public void setIsLast(Boolean isLast) {
		this.isLast = isLast;
	}
	public List<Integer> getIdIfs(){
        if(condition.size() > 0 && condition.get(0).contains(" - ")){
        	for(String s: condition){
        		if(!idIfs.contains(Integer.valueOf(s.split(" - ", 2)[0])))
        		idIfs.add(Integer.valueOf(s.split(" - ", 2)[0]));
        	}
        }
		return idIfs;
	}
	public void setIdIfs(List<Integer> idIfs) {
		this.idIfs = idIfs;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<Integer> getIdIfsFirst(){
		Collections.sort(idIfsFirst);
		return idIfsFirst;
	}
	public List<Integer> getIdIfsLast(){
		Collections.sort(idIfsLast);
		return idIfsLast;
	}
	public void setIdIfsFirst(List<Integer> idIfs) {
		this.idIfsFirst = idIfs;
	}
	public void setIdIfsLast(List<Integer> idIfs) {
		this.idIfsLast = idIfs;
	}
	
}
