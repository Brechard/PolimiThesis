package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

public class PairList {

	private List<String> ifs = new ArrayList<String>();
	private List<String> loops = new ArrayList<String>();

	public PairList(List<String> ifs, List<String> loops) {
        this.ifs = ifs;
        this.loops = loops;
    }

	public List<String> getIfs() {
		return ifs;
	}

	public void setIfs(List<String> ifs) {
		this.ifs = ifs;
	}

	public List<String> getLoops() {
		return loops;
	}

	public void setLoops(List<String> loops) {
		this.loops = loops;
	}

	
}
