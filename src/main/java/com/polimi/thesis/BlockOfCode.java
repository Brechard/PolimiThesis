package com.polimi.thesis;

/*
 * This class is needed because the information saved as positions are relative to the file with all the information an not to the block itself
 */
public class BlockOfCode{
    private final String block;
    private final int firstPos;
    private final int posFirstMethod;
   
    public BlockOfCode(String block, int firstPos, int posFirstMethod) {
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
