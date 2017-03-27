package com.polimi.thesis;

public class Pair{
    private final String block;
    private final int firstPos;
    private final int posFirstMethod;

    public Pair(String block, int firstPos, int posFirstMethod) {
        this.block = block;
        this.firstPos = firstPos;
        this.posFirstMethod = posFirstMethod;
    }

    public int getFirstPos() {
        return firstPos;
    }

    public String getBlock() {
        return block;
    }
    
    public int getPosFirstMethod(){
    	return posFirstMethod;
    }
}
