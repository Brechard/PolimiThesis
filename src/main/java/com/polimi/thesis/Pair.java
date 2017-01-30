package com.polimi.thesis;

public class Pair{
    private final String block;
    private final int firstPos;

    public Pair(String block, int firstPos) {
        this.block = block;
        this.firstPos = firstPos;
    }

    public int getFirstPos() {
        return firstPos;
    }

    public String getBlock() {
        return block;
    }
}
