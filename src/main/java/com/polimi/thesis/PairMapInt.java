package com.polimi.thesis;

import java.util.HashMap;
import java.util.Map;

public class PairMapInt {
	
	private Map<Integer, Stage> stagesList = new HashMap<Integer, Stage>();
	private int nStages = 0;
	
	public PairMapInt(Map<Integer, Stage> stagesList, int nStages) {
		super();
		this.stagesList = stagesList;
		this.nStages = nStages;
	}

	public Map<Integer, Stage> getStagesList() {
		return stagesList;
	}

	public void setStagesList(Map<Integer, Stage> stagesList) {
		this.stagesList = stagesList;
	}

	public int getnStages() {
		return nStages;
	}

	public void setnStages(int nStages) {
		this.nStages = nStages;
	}	
}
