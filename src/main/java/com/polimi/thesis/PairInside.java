package com.polimi.thesis;

import java.util.List;

public class PairInside{
    private Boolean inside;
    private List<String> condition;

    public PairInside(Boolean inside, List<String> condition) {
        this.setInside(inside);
        this.setCondition(condition);
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
}
