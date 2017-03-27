package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

public class PairList {

	private List<Condition> ifs = new ArrayList<Condition>();
	private List<String> loops = new ArrayList<String>();

	public PairList(List<Condition> ifs, List<String> loops) {
        this.ifs = ifs;
        this.loops = loops;
    }

	public List<Condition> getIfs() {
		return ifs;
	}

	public void setIfs(List<Condition> ifs) {
		this.ifs = ifs;
	}

	public List<String> getLoops() {
		return loops;
	}

	public void setLoops(List<String> loops) {
		this.loops = loops;
	}

	
}
