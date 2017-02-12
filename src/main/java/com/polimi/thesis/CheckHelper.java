package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.polimi.thesis.Variables.MethodsType;

public class CheckHelper {

	public static boolean checkCombine(String method){
		for (int i = 0; i < Variables.combineMethods.length; i++) {
			if(method.equals(Variables.combineMethods[i]))
				return true;		
		}
		return false;
	}
	
	public static int checkExistence(Stage stage, Map<Integer, Stage> stagesList){
		List<RDD> rdds = stage.getRDDs();
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage1 = entry.getValue();
			if (stage1.getId() == stage.getId())
				continue;
			List<RDD> rdds1 = stage1.getRDDs();
			int equal = 0;
			for (int i = 0; i < rdds1.size(); i++) {
				String callsite = rdds1.get(i).getCallSite();
				if (rdds.size() > i && callsite.equals(rdds.get(i).getCallSite())) 
					continue;
				else equal++;
			}
			if (equal == 0 && rdds.size() == rdds1.size()) {
				return stage1.getId();
			}
		}
		
		
		return -1;
	}
	
	
	public static PairInside checkInside(int pos, String op, List<String> ifs, List<String> loops){
		List<String> list = loops;
		if(op.equals("if")) list = ifs;
		PairInside pair = new PairInside(false, new ArrayList<String>());
		for(String s: list){
			String[] splitted = s.split("-");
			int start = Integer.valueOf(splitted[0]);
			String conditionS = splitted[1];
			int end = Integer.valueOf(splitted[2]);
			if((start <= pos) && (pos <= end)){
				pair.setInside(true);
				pair.getCondition().add(conditionS);
			}
		}		
		return pair;
	}
	
	// Check if the method receive is a shuflle method, an action, a transformation or is some method that is not from spark
	public static MethodsType checkMethod(String method){
		
		if(method.matches(".*By.*")){ // Check if the method passed will shuffle, considering that any transformation of the kind *By or *ByKey can result in shuffle
			return MethodsType.shuffle;		
		}
		else {
			for (int i = 0; i < Variables.shuffles.length; i++) {
				if(method.equals(Variables.shuffles[i]))
					return MethodsType.shuffle;		
			}
		}
		
		for (int i = 0; i < Variables.actions.length; i++) {
			if(method.equals(Variables.actions[i]))
				return MethodsType.action;
		}
		for (int i = 0; i < Variables.transformations.length; i++) {
			if(method.equals(Variables.transformations[i]))
				return MethodsType.transformation;
		}
		return MethodsType.others;
	}
	
	public static Boolean checkDependsOnPartitioner(String method){
		for (int i = 0; i < Variables.dependsOnPartitioner.length; i++) {
			if(method.equals(Variables.dependsOnPartitioner[i]))
				return true;
		}
		return false;
	}
		
	public static boolean checkRDD(String word, Set<String> listRDDs){
//		System.out.println("checkRDD: " +word);
		for(String s: listRDDs){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	public static boolean checkSC(String word, Set<String> listSC){
//		System.out.println("checkSC: " +word);
		for(String s: listSC){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	public static Boolean checkIqual(int pos, String file){
		int i = pos;
		while(i < file.length() && file.charAt(i) == ' ') 
			i++;
//		System.out.println("Char: " +file.charAt(i)+",i: " +i+", pos: " +pos);
		if(file.charAt(i) == '=')
			return true;
		else return false;
	}
}
