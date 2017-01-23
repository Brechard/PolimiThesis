package com.github.javaparser.javaparser_core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.tree.DefaultMutableTreeNode;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.javaparser_core.Thesis.Stage;
import com.github.javaparser.javaparser_core.Variables.MethodsType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Thesis {
	
	private static int stages;
	private static int jobs;
	private static int rdds;
	private static String file;
	
	private static Set<String> listRDDs = new HashSet<String>();
	private static Set<String> listSC = new HashSet<String>();
	
	private static TreeMap<Integer, String> map = new TreeMap<Integer, String>(Collections.reverseOrder());
	private static List<String> actionList = new ArrayList<String>();
	
	private static DefaultMutableTreeNode root;
	private static List<Map<Integer, Stage>> jobsList = new ArrayList<Map<Integer, Stage>>();
	private static Map<Integer, Stage> stagesList = new HashMap<Integer, Stage>();
	private static int nStages;

	public static void main(String[] args) throws Exception {
		jobs = 0; stages = 0; rdds = 0;

		// creates an input stream for the file to be parsed
        FileInputStream in = null;
		try {
			in = new FileInputStream(Variables.path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

        // parse the file
        CompilationUnit cu = null;
		try {
			cu = JavaParser.parse(in);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		file = cu.toStringWithoutComments();
		file = file.split("public static void main")[1];

		root = new DefaultMutableTreeNode("Jobs");
		
		findRDDs();
		findSC();		
		
		// Create a Pattern object
		Pattern r = Pattern.compile("\\w*");
		// Now create matcher object.
		Matcher m = r.matcher(file);
		
		String cache = "";
		while (m.find()) {
			if ((m.start() - 1) >= 0 && file.charAt(m.start() - 1) == '.') {
				// Now we know that m.group() is a method, we have to check if it is a spark method
				MethodsType type = checkMethod(m.group());
				if (type == MethodsType.action) {
					String s = m.group();
					jobs++;
					if (file.charAt(m.start() - 2) != ')' && (checkRDD(cache) || checkSC(cache))) 
						s = cache+ "." +m.group();
//					map.put(m.start() - 1, s); We are not using TreeMap because it is too slow
					actionList.add(String.valueOf(m.start() +"-"+s));
				} 
			}
			if(!m.group().replaceAll(" ", "").equals(""))
				cache = m.group();
		}

		for(int i = 0; i < actionList.size(); i++) { // We iterate over the map with the spark methods
			addChild(String.valueOf(i)+ "-" +actionList.get(i).split("-")[1], root);
			Map<Integer, Stage> map = new HashMap<Integer, Stage>();
			jobsList.add(map);
		}
		System.out.println("Number of jobs: " +jobs);
		for(int i = actionList.size() - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			jobs--;
			stagesList = jobsList.get(jobs);

			
			int key = Integer.valueOf(actionList.get(i).split("-")[0]);
			String method = actionList.get(i).split("-")[1];
			String rddOrSc = "";
			if (method.contains(".")) {				
				String[] w = method.split("\\.");
				method = w[w.length - 1];				
				rddOrSc = w[0];
			}
  
			char c = file.charAt(key - 2);
			System.out.println("\n\nAction found: " +method+ ", a new job should be generated, jobs = "+jobs+"\n\n");

			// We have to find the block of code that this action refers to
			if(c != ')'){
				if (checkRDD(rddOrSc)) { // If it is applied to an RDD we have to find where and how it was created
					generate(findBlock(rddOrSc), (DefaultMutableTreeNode) root.getChildAt(jobs), null, null);
				} else { // If it is applied to an sc there is an error				
					throw new Exception(method+", An action cannnot be applied to an SC");
				}
			} else {
				int start = searchStartBlock(key);
				generate(new Pair(file.substring(start, key), start), (DefaultMutableTreeNode) root.getChildAt(jobs), null, null);
			}
		}
		prettyPrint(jobsList);
		
		
		nStages = -1;
		for(int i = actionList.size() - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			nStages += jobsList.get(i).size();
		}
		System.out.println("Number of stages: " +nStages);
		for(int i = actionList.size() - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			stagesList = jobsList.get(i);
			updateStages();
		}
		
		printTree();
		
		prettyPrint(jobsList);
		
	}
	
	public static boolean checkCombine(String method){
		for (int i = 0; i < Variables.combineMethods.length; i++) {
			if(method.equals(Variables.combineMethods[i]))
				return true;		
		}
		return false;
	}
	
	public static boolean checkRDD(String word){
//		System.out.println("checkRDD: " +word);
		for(String s: listRDDs){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	public static boolean checkSC(String word){
//		System.out.println("checkSC: " +word);
		for(String s: listSC){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	// Check if the method receive is a shuflle method, an action, a transformation or is some method that is not from spark
	public static MethodsType checkMethod(String method){
		
		// It is important to check first if it is a shuflle method because TEXTFILE is considered a shuffle and action in the variables and here we want it to be a shuffle
		if(method.matches(".*By.*")){ // Check if the method passed will shuffle, considering that any transformation of the kind *By or *ByKey can result in shuffle
			Boolean shuffle = true;
			for (int i = 0; i < Variables.methodsByTransformation.length; i++) {
				if(method.equals(Variables.methodsByTransformation[i])){
					shuffle = false;
					break;
				}
			}
			if(shuffle)
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
	
	public static int checkExistence(Stage stage){
		List<RDD> rdds = stage.getRDDs();
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage1 = entry.getValue();
			if (stage1.id == stage.id)
				continue;
			List<RDD> rdds1 = stage1.getRDDs();
			int equal = 0;
			for (int i = 0; i < rdds1.size(); i++) {
				String callsite = rdds1.get(i).callSite;
				if (rdds.size() > i && callsite.equals(rdds.get(i).callSite)) 
					continue;
				else equal++;
			}
			if (equal == 0 && rdds.size() == rdds1.size()) {
				return stage1.id;
			}
		}
		
		
		return -1;
	}
	

	/*
	 * Finds the block where the rdd passed as parameter is created
	 */
	public static Pair findBlock(String rdd){
		int start = findRDD(rdd);	
		System.out.println("findBlock(" +rdd+") start: " +start);
		return new Pair(file.substring(start, searchEndBlock(start, 0, file)), start);
	}
	
	public static String findCache(int end, String block){
		int start = end - 100;
		if (start < 0) start = 0;
		Pattern r = Pattern.compile("\\w*");
		Matcher m = r.matcher(block.substring(start, end));
		String cache = "";
		while(m.find()){
			if (!m.group().replaceAll(" ", "").equals(""))				
				cache = m.group();			
		}
		return cache;			
	}
	
	/*
	 * Method that finds where an specific RDD is created
	 */
	public static int findRDD(String s){
		
//        System.out.println(">> findRDD: " +s);
        int length = s.length();
        int i = 1;
        
		while (i < (file.length() - length)) {  // Recorremos la expresión carácter a carácter
			
			if(file.substring(i, i + length).equals(s)
					&& !Character.isLetterOrDigit(file.charAt(i + length + 1)) && file.charAt(i + length + 1) != '_'
					&& !Character.isLetterOrDigit(file.charAt(i - 1)) && file.charAt(i - 1) != '_') {
				break;
			} 
			i++;
		}
		
		return i;
		
	}

	
	/*
	 * Method that will find all the variables that represent and RDD
	 */
	public static void findRDDs(){
        System.out.println(">> findRDDs");

		String regex = "\\w*=\\w*\\.";

		for (int i = 0; i < Variables.transformations.length; i++) {
		
			// Create a Pattern object
			Pattern r = Pattern.compile(regex+Variables.transformations[i]);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
		        listRDDs.add(var);
			}
		}
		for (int i = 0; i < Variables.shuffles.length; i++) {
			
			// Create a Pattern object
			Pattern r = Pattern.compile(regex+Variables.shuffles[i]);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
		        listRDDs.add(var);
			}
		}
		
		// Check for RDDs created like newRDD = otherRDD
		regex = "\\w*=";
		List<String> aux = new ArrayList<String>();

		for(String s: listRDDs){
			String search = regex + s;
			// Create a Pattern object
			Pattern r = Pattern.compile(search);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			System.out.println(search);			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
				System.out.println("FIND: " +m.group()+", already: " +listRDDs.contains(var));
		        if(!listRDDs.contains(var))
		        	aux.add(var);
			}			
		}
		listRDDs.addAll(aux);
		for (String s : listRDDs)
			System.out.println(">>>>>> " +s);
	}
	
	public static void findSC(){

        System.out.println(">> findSC");

        String regex = "(JavaSparkContext?)\\s*(\\w*?)\\s*=";
		// Create a Pattern object
		
		Pattern r = Pattern.compile(regex);
		// Now create matcher object.
		Matcher m = r.matcher(file);
		
		while(m.find()){
			if (m.group(2) != null && !m.group(2).replaceAll(" ", "").equals(""))
		        listSC.add(m.group(2));
		}
		for (String s : listSC)
			System.out.println(">>>>>> " +s);
		
	}	
		
	public static void prettyPrint(Object a){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();		
		System.out.println(gson.toJson(a));
	}
	
	public static void printTree(){
	
		String result = "\n";
	    Enumeration<?> enumer = root.preorderEnumeration();
	    while (enumer.hasMoreElements()) {
	        DefaultMutableTreeNode node = (DefaultMutableTreeNode) enumer.nextElement();
	        String nodeValue = String.valueOf(node.getUserObject());
	        String indent = "";
	        while (node.getParent() != null) {
	           indent += "    "; 
	           node = (DefaultMutableTreeNode) node.getParent();
	        }
	        result += indent + nodeValue + "\n";
	    }
        System.out.println(result);
        
	}
	
	private static DefaultMutableTreeNode addChild(String title, DefaultMutableTreeNode parent) {
		DefaultMutableTreeNode child = new DefaultMutableTreeNode(title);
		parent.add(child);
		return child;
	}
	
	
	public static int searchEndHelper(int start, String block){
		int i = start;
		int par = 0;
		while (i < block.length()) {
			if(block.charAt(i)=='(')
				par++;
			else if  (block.charAt(i)==')'){
				par--;
				if (par == 0)
					return i;
			} else if(block.charAt(i) == ';' && par == 0)
				return i;
			i++;
		}
		return i;
	}
	
	/*
	 * This method searchs the end of a block of code given the begining 
	 */
	public static int searchEndBlock(int start, int par2, String block){
//		System.out.println("start at: " +start);
		int i = searchEndHelper(start, block);
		if(block.charAt(i) == ')')
			par2++;
		int par = par2;
		if (i == start) {
			return i;
		}
		while (i < block.length()) {
			if(block.charAt(i) == '('){
				i = searchEndBlock(i, par++, block); // If we find another opening parenthesis we have to reach the end of it again
				break;
			} else if (block.charAt(i) == ')')
				par --;
			else if  (block.charAt(i)==';' && par == 0)
				return i;
			i++;
		}		
		return i;
	}
	
	/*
	 * This method searchs the start of a block of code surrounded of parenthesis
	 */
	public static int searchStartParenthesisBlock(String block, int end){
//		System.out.println("start at: " +start);
		int i = end;
		int par = 0;

		while (i > 0) {
			if(block.charAt(i)==')') par++;                              
			else if  (block.charAt(i)=='('){
				par--;  
				if (par == 0)
					break;
			}
			i--;
		}
		return i;
	}

	/*
	 * This method searchs the start of a block of code surrounded of parenthesis
	 */

	public static int searchStartBlock(int end){
		int i = end;
		int par = 0;
		while (i > 0) {
			if(file.charAt(i)==')') par++;                              
			else if (file.charAt(i)=='(') par--;
			else if (file.charAt(i)==';' && par == 0) return i;
			i--;
		}		
		return i;
	}

	
	/*
	 * Analyzes a block of code in order to find the relations inside the block
	 */
	public static void generate(Pair pair, DefaultMutableTreeNode parent, Stage child2, Integer childId){
		String block = pair.getBlock();
		int beggining = pair.getFirstPos();
		if(jobs == 0){
			System.out.println("\n>> generate");
			System.out.println("Beggining: " +beggining+"\n");
			System.out.println("Block: " +block+"\n");
			System.out.println("PARENT: ");
			prettyPrint(child2);	
		}
		String cache = "";
		int i = 0;
		String rdd = "";
		List<String> forward = new ArrayList<String>(); // The string will be: position - method, that way we don't need to use a TreeMap that is very slow		
		List<DefaultMutableTreeNode> parents = new ArrayList<DefaultMutableTreeNode>();
		List<Stage> parents2 = new ArrayList<Stage>();
		List<Integer> rddsParentsId = new ArrayList<Integer>();

		// First we read the block to create the relations inside this block backwards, in order to do it we need to read it forward and analyze it backwards		
		Pattern r1 = Pattern.compile("\\w*");
		Matcher m1 = r1.matcher(block);
		int end = 0;
		while (m1.find()) {
			if ((m1.start() - 1) >= 0 && block.charAt(m1.start() - 1) == '.' && checkMethod(m1.group()) != MethodsType.others) { // Method found
				if (m1.start() > end) {
					forward.add(String.valueOf(m1.start()) +"-"+ m1.group() +"-"+ String.valueOf(m1.start() + beggining));
					end = searchEndHelper(m1.end(), block);
				}
			}
		}
		/*
		for (int b = 0; b < rddsParentsId.size() ; b++) {
			System.out.println("generate, rddchildId: " +b+": "+rddsParentsId.get(b));
		}
		if (rddsParentsId.isEmpty()) {		int nStages = stagesList.size() - 1;

			System.out.println("generate, rddchildId is empty");			
		}
		*/
		int p = 0;
		int position = 0;
		// Create dependencies of the block passed analyzing backwards
		for (int j = forward.size() - 1; j >= 0; j--) {
			String method = forward.get(j).split("-")[1];
			String pos = forward.get(j).split("-")[2];
			if (child2 == null)
				child2 = new Stage();
			System.out.println("Received Method: " +method+ ", type: " +checkMethod(method)+", childId: " +child2.getId()+", rddsParentsId: " +childId+", pos: " +pos);
			Stage childCache = child2;
			MethodsType type = checkMethod(method);
			if (type == MethodsType.shuffle){
				parent = addChild("+ " +method, parent);
				Stage parent2 = new Stage();
//				child2.addParentId(parent2.getId());

				RDD rdd1 = new RDD(method+" at char " +pos);
//				RDD rdd2 = new RDD(method+" at char " +pos);
				

				
				if (childId != null){
					rdd1.addChildId(childId);

					
					// We have to search for the child RDDs in order to put the parentRDDId
				
//					System.out.println("Stage: "+child2.id);
//					prettyPrint(child2);
					if(child2.getRDDs().size() == 0){ // If in the stage there are still not rdds this means that the rdd will have his child in another stage
						List<Integer> childStages = child2.getChildId();
						for(Integer id: childStages){
							Stage stageChild = stagesList.get(id);
//							System.out.println("Stage list id: " +id+", being search by: " +child2.id);
							List<RDD> rddsChild = stageChild.getRDDs();
							for(RDD rddChild: rddsChild){
//								System.out.println("Rdd child: "+rddChild.getId()+", childId: " +childId);
								if(rddChild.getId() == childId)
									rddChild.addParentId(rdd1.getId());
								
							}
						}						
					} else {
						List<RDD> rddsChild2 = child2.getRDDs();
						for(RDD rddChild2: rddsChild2)
							if(rddChild2.getId() == childId)
								rddChild2.addParentId(rdd1.getId());
					}										
				}

				
				childId = rdd1.getId();
				child2.addChild(rdd1);
				
				// Check if the stage created already exist
				int check = checkExistence(child2);
				
				// If it exist then it means the already existing stage has two childs, so we have to modify the already existing
				// stage instead of creating one
				if(check > -1){
					/*
					System.out.println("CHECK: " +check);
					prettyPrint(stagesList);
					System.out.println("Parent to check");
					prettyPrint(child2);
					System.out.println("Parent equivalent");
					*/
					Stage newParent = stagesList.get(check);
//					prettyPrint(newParent);
					
					RDD lastRDD = newParent.getRDDs().get(0);
					lastRDD.addChildId(child2.getRDDs().get(0).getChildsId());
					newParent.addChildId(child2.getChildId());
					
					stagesList.put(newParent.getId(), newParent);
					
					// If the new Parent already exist we have to modify in the child the parent Id					
					for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
						Stage stage = entry.getValue();
						List<Integer> parentsIds = stage.getParentId();
						for (int z = 0; z < parentsIds.size(); z++) {
							if (parentsIds.get(z) == child2.getId()){
								parentsIds.remove(z);
								parentsIds.add(z, newParent.getId());
							}
						}
					}
					return; // Since the stage already exists the dependecies after this stage are already created
				} else  {
					stagesList.put(child2.getId(), child2);
				}
				
				parent2.addChildId(child2.getId());
				child2 = parent2;
//				prettyPrint(stagesList);
				// In order to be possible 
			} else {

				addChild("- " +method, parent);
				RDD rdd1 = new RDD(method+" at char " +pos);
//				rdd1.addchildId(child2.getId());

				
				if (childId != null){
					rdd1.addChildId(childId);
					
					// We have to search for the child RDDs in order to put the parentRDDId
//					System.out.println("Stage: "+child2.id);
//					prettyPrint(child2);
					if(child2.getRDDs().size() == 0){ // If in the stage there are still not rdds this means that the rdd will have his child in another stage
						List<Integer> childStages = child2.getChildId();
						for(Integer id: childStages){
							Stage stageChild = stagesList.get(id);
//							System.out.println("Stage list id: " +id+", being search by: " +child2.id);
//							prettyPrint(stagesList);
							List<RDD> rddsChild = stageChild.getRDDs();
							for(RDD rddChild: rddsChild){
//								System.out.println("Rdd child: "+rddChild.getId()+", childId: " +childId);
								if(rddChild.getId() == childId)
									rddChild.addParentId(rdd1.getId());								
							}
						}						
					} else {
						List<RDD> rddsChild2 = child2.getRDDs();
						for(RDD rddChild2: rddsChild2)
							if(rddChild2.getId() == childId)
								rddChild2.addParentId(rdd1.getId());
					}										
				}
				childId = rdd1.getId();				
				child2.addChild(rdd1);
			}
//			System.out.println("childId: " +child2.getId()+ " in p2: " +position);
			System.out.println("Save childId: " +child2.getId()+ ", rddsParentsId: " +childId+ " in p2: " +position+"\n");
			parents.add(p++, parent);
			parents2.add(position, child2);
			rddsParentsId.add(position, childId);
			prettyPrint(stagesList);
			System.out.println("PARENT: " +position);
			prettyPrint(child2);
			if (type == MethodsType.shuffle && checkCombine(method)) {
				position++;
				Stage parentNew = new Stage();
				parentNew.addChildId(childCache.getId());
				
//				childCache.addParentId(parentNew.getId());
				
				parents2.add(position, parentNew);
				rddsParentsId.add(position, childId);
				System.out.println("rddsParentsId: " +childId+ " in p2: " +position);
				System.out.println("PARENT COMBINE: " +position);
				System.out.println("PARENT COMBINE");
				prettyPrint(parentNew);
			}
			/*
			for (int b = 0; b < rddsParentsId.size() ; b++) {
				System.out.println("End of loop, rddchildId: " +b+": "+rddsParentsId.get(b));
			}
			*/


//			printTree();
			position++;
		}
		p = 0;
		position = 0;
		Boolean changedPosition = false;
		// Create dependencies with blocks that appear as variables or inside parenthesis
		if (forward.size() > 0){
			for (int j = forward.size(); j >= 0; j--) {
				
				if (j == forward.size() && forward.size() > 0) {
					
					int pos = Integer.valueOf(forward.get(j - 1).split("-")[0]);
					String method = forward.get(j - 1).split("-")[1];
					int start = pos + method.length();
					MethodsType type = checkMethod(method);

//					System.out.println("Second loop, check if last method: "+method+", is combine, start: " +block.charAt(start)+ ", +1: " +block.charAt(start + 1));
					if(block.charAt(start + 1) != ')'){ // The last method of the block is combine Method
						int endBlock = searchEndBlock(start, 1, block);
						cache = findCache(endBlock, block);
						System.out.println("position cero");
						position = position + 1;
						changedPosition = true;
						prettyPrint(parents2);
						prettyPrint(stagesList);
						System.out.println("\n\nChanged position: " +position+ "\n\n ");
						if (checkRDD(cache) || checkSC(cache)) { 
							if(type != MethodsType.shuffle) position = 0; // If the method is not a shuffle the following methods have to be on the same stage as this method
//							System.out.println("Combine method with variable: " +cache+", childId: " +parents2.get(position).getId()+", in 1");
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							generate(findBlock(cache), parents.get(p), parents2.get(position), rddsParentsId.get(position));					
						} else {
							if(!checkCombine(method)){
								System.out.println("\n\nDes Changed position\n\n ");
								position = 0;
								changedPosition  = false;
							}
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							String subBlock = block.substring(start, end +1);
							System.out.println("Written combine method: " +subBlock+", childId: " +parents2.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", rddsParentsId: " +rddsParentsId.get(position));
							generate(new Pair(subBlock, start + beggining), parents.get(p), parents2.get(position), rddsParentsId.get(position));
						}					
					} 
					else 					
						System.out.println("No it is not");
					
					if (changedPosition) {
						position = 0;
						changedPosition = false;
					}
					continue;
				}
				int pos = Integer.valueOf(forward.get(j).split("-")[0]);
				String method = forward.get(j).split("-")[1];
				MethodsType type = checkMethod(method);
				if (j - 1 >= 0) {
					String methodBefore1 = forward.get(j - 1).split("-")[1];
					type = checkMethod(methodBefore1);
				}
				System.out.println("Second loop, method: " +method+ ", p: " +p+ ", rddsParentsId: " +childId+", position: " +position+ ", block:" +block);


				if(block.charAt(pos - 2) == ')'){ // Method not applied directly to a variable, we have to analyze the interior of the parenthesis
					cache = findCache(pos, block);
					System.out.println("Cache: " +cache+", p: " +p+ ", rddsParentsId: " +childId);
					if (block.charAt(pos - 3) == '(') { // There is nothing inside the parenthesis, the method is applied to the result of another method
						System.out.println("Nothing to do here");
					} else { // Inside of the parenthesis there is something what means that the method before (writing) may be a combine method
						// If it is a combine method we have to add one position because we saved the stages there
						if (type == MethodsType.shuffle) {
							position = position + 2;
						} else {
							position = position + 1;							
						}
						changedPosition = true;							
						if (checkRDD(cache) || checkSC(cache)) { 
							String methodBefore ="";
							if (j - 1 >= 0) {
								methodBefore = forward.get(j - 1).split("-")[1];
								System.out.println("Combine method with variable: " +cache+", methodBefore: " +methodBefore+", combine: " +checkCombine(methodBefore));
								if(!checkCombine(methodBefore) && changedPosition){ // If it was not a combine method we have to undo the addition
									position = position - 2;
									changedPosition = false;
								}
							} else{
								if (changedPosition) {
									if (type == MethodsType.shuffle) {
										position = position - 2;
									} else {
										position = position - 1;							
									}
									changedPosition = false;
								}
							}
							System.out.println("Combine method with variable: " +cache+", in p2: " +position+", type: " +checkMethod(methodBefore));
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							System.out.println("Combine method with variable: " +cache+", j: " +j+", childId: " +parents2.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", rddsParentsId: " +rddsParentsId.get(position)+", position: "+position);
							generate(findBlock(cache), parents.get(p), parents2.get(position), rddsParentsId.get(position));					
						} else {
							if (j - 1 >= 0) {
								String methodBefore = forward.get(j - 1).split("-")[1];
								if(!checkCombine(methodBefore) && changedPosition){
									if (type == MethodsType.shuffle) {
										position = position - 2;
									} else {
										position = position - 1;							
									}
									changedPosition = false;
								}
							} else {
								if (changedPosition) {									
									if (type == MethodsType.shuffle) {
										position = position - 2;
									} else {
										position = position - 1;							
									}
									changedPosition = false;
								}
							}
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							int start = searchStartParenthesisBlock(block, pos);
							String subBlock = block.substring(start, pos);
							System.out.println("Written combine method: " +subBlock+", childId: " +parents2.get(position).getId()+", in p2: " +position+ ", rddsParentsId: " +rddsParentsId.get(position));
							generate(new Pair(subBlock, start), parents.get(p), parents2.get(position), rddsParentsId.get(position));
						}	
						if (changedPosition) {
							if (type == MethodsType.shuffle) {
								position = position - 2;
							} else {
								position = position - 1;							
							}
							changedPosition = false;
						}
					}					
				} else { // Method applied directly to a variable
					cache = findCache(pos, block);
					/*
					Boolean combine = checkCombine(forward.get(j).split("-")[1]);
					int a = 0;
					for (Stage stage : parents2) {
						System.out.println("Stage: " +a++);
						prettyPrint(stage);
					}
					prettyPrint(stagesList);
					for (int b = 0; b < rddsParentsId.size() ; b++) {
						System.out.println("rddsParentsId: " +b+": "+rddsParentsId.get(b));
					}
					System.out.println("Changed position: " +changedPosition);
					System.out.println("Position: " +position);
					System.out.println("Applied directly to a variable: " +cache+", childId2: " +parents2.get(position).getId());
					*/
					System.out.println("Applied directly to a variable: " +cache+", childId: " +parents2.get(p).getId()+", p: " +p+", p2: " +position+ ", rddsParentsId: " +rddsParentsId.get(p)+", block: " +block);
					if (checkRDD(cache) || checkSC(cache)) {
						System.out.println("Let's recall: "+findBlock(cache).block);
						generate(findBlock(cache), parents.get(p), parents2.get(position), rddsParentsId.get(position));					
					} else {
						System.err.println("Error in the code? " +cache+", block: " +block);
					}
				}
				p++;
				position++;
				if (checkCombine(method))
					position++;
			} 
		} else { // If there are no spark methods inside the block it may be a case of newRDD = otherRDD;
			Pattern r = Pattern.compile("\\w*=\\w*");
			Matcher m = r.matcher(block.replaceAll(" ", ""));
			while(m.find()){
				System.out.println("RDDS:" +m.group());
				String[] splited = m.group().split("=");
				if(splited.length > 1){
					String rdd1 = m.group().split("=")[0];
					String rdd2 = m.group().split("=")[1];
					if(checkRDD(rdd1) && checkRDD(rdd2)){
						generate(findBlock(rdd2), parent, child2, childId);					
					}				
				}
			}			
		}
	}
	
	
	public static void updateStages(){
		
		List<Stage> childsStagesList = new ArrayList<Stage>();
		List<Integer> keys = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet())
			keys.add(entry.getKey());

		// Find the stage that does not have any child wich will be the last stage, from there we will order the others ("generation" by "generation"), updating childsId and parentsId
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			if(stage.getChildId().isEmpty()){
				int oldId = stage.getId();
				stage.setId(nStages--);
				
//				System.out.println("Stage modified, new Id: " +stage.getId()+", oldId: " +oldId);
				childsStagesList = updateStageschildId(stage.getId(), oldId, stage);
				break; // There can not be more than a final stage
			}				
		}			
		
		int a = 0;
		while(!childsStagesList.isEmpty()){
//			System.out.println("A:" +a++);
			List<Stage> aux = new ArrayList<Stage>();
			for (Stage stage : childsStagesList) {
				int oldId = stage.getId();
				stage.setId(nStages--);
				List<Stage> aux2 = updateStageschildId(stage.getId(), oldId, stage);
				for (Stage stage2 : aux2) {
					if(!aux.contains(stage2)) // If a Stages has already been added to be modified we don't have to add it twice
						aux.add(stage2);
				}
			}
			for(Stage b: childsStagesList){
//				System.out.println("CHILD - Id:" +b.getId());
				b.setUpdatedChild(true);
			}
			childsStagesList = aux;
			if(nStages < -5)
				throw new Error("ERROR");
		}

//		System.out.println("\n\n\n\n Stages ordered in bad position");
//		prettyPrint(stagesList);
//		System.out.println("\n\n\n\n");

		// Once the stages have been renamed we want them to be in the same position as their id
//		System.out.println("Size: "+keys.size());
		for (int i = 0; i < keys.size(); i++) {
			Stage stage ;
			if(stagesList.containsKey(keys.get(i)))
				stage = stagesList.get(keys.get(i)); //Cache where i will put the stage
			else continue;
			
			Stage cache = null;
			if(stagesList.containsKey(stage.getId()))
				cache = stagesList.get(stage.getId());
			if(cache != null){
//				System.out.println("Chache: " +stage.getId());
	//			prettyPrint(cache);
			}
			if(keys.get(i) != stage.getId()){
//				System.out.println("Put in: " +stage.getId()+", remove in: " +keys.get(i));
				stagesList.put(stage.getId(), stage);
				stagesList.remove(keys.get(i));
			}
			int pos = -1;
			while(cache != null && cache.getId() != pos){
				Stage aux = cache;
				pos = aux.getId();

				cache = stagesList.get(pos);
//				System.out.println("Cache: ");
				prettyPrint(cache);
//				System.out.println("Put in: " +pos);
				stagesList.put(pos, aux);
			}
		}

		for(Integer i : keys){
			if(i >= keys.size() + (nStages+1) && stagesList.containsKey(i)){
				System.out.println("\n\n\nremove: "+i+"\n\n\n");
				stagesList.remove(i);				
			}
		}
		
//		System.out.println("\n\n\n\n Stages ordered withoud parents");
//		prettyPrint(stagesList);
//		System.out.println("\n\n\n\n");

		// Once the stages have been ordered we set the parents ids of the stages (if a stage is my child, I am his father)
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			List<Integer> childsIds = stage.getChildId();
//			System.out.println("Stage: "+stage.id);
//			prettyPrint(childsIds);
			for(Integer id: childsIds){
//				System.out.println("Id of a child: " +id);
				Stage child = stagesList.get(id);
				child.addParentId(stage.getId());
			}
			
		}
	}
	
	/*
	 * Change the ids the stages related with the one sent,as we are updating generation by generation and from the last child, we will return a list of stages that we have modified
	 */
	public static List<Stage> updateStageschildId(int newId, int oldId, Stage stageCalling){
		List<Stage> childsStagesList = new ArrayList<Stage>();
//		System.out.println("\nModifie stage, oldId: " +oldId+ ", newId: " +newId);
		
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			if(stage == stageCalling || (stage.updatedChild != null && stage.updatedChild)) continue;
			// Update in parents the childId
			List<Integer> childsIds = stage.getChildId();
//			System.out.println("Stage checking:");
//			prettyPrint(stage);
			for(int i = 0; i < childsIds.size(); i++){
				if(childsIds.get(i) == oldId){
//					System.out.println("Stage " +stage.getId()+ " modified childId, oldId: " +oldId+", newId: " +newId);					
					childsIds.remove(i);
					if(!childsIds.contains(newId))
						childsIds.add(i, newId);
					childsStagesList.add(stage);
				}
			}
			
		}	
		
		return childsStagesList;
	}
	
	public static class Stage{
		private int id;
		private List<RDD> rdds;
		private List<Integer> childId;
		private List<Integer> parentId;
		private Boolean updatedChild;

		public Stage(){
			id = stages++;
			childId = new ArrayList<Integer>();
			parentId = new ArrayList<Integer>();
			rdds = new ArrayList<RDD>();
		}
		public void addChild(RDD child) {
			rdds.add(child);
		}
		public void addChildId(int id){
			childId.add(id);
		}
		public void addChildId(List<Integer> ids){
			childId.addAll(ids);
		}
		public List<Integer> getChildId(){
			return childId;
		}
		public void addParent(RDD parent) {
			rdds.add(parent);
		}
		public void addParentId(int id){
			parentId.add(id);
		}
		public void addParentId(List<Integer> ids){
			parentId.addAll(ids);
		}
		public List<Integer> getParentId(){
			return parentId;
		}
		public int getId(){
			return id;
		}
		public List<RDD> getRDDs(){
			return rdds;
		}
		public void setId(int id){
			this.id = id;
		}
		public void setUpdatedChild(Boolean update){
			updatedChild = update;
		}
		
	}
	
	public static class RDD{
		private String callSite;
		private int id;
		private List<Integer> childsIds;
		private List<Integer> parentsIds;
		
		public RDD(String callSite){
			this.callSite = callSite;
			id = rdds++;
			childsIds = new ArrayList<Integer>();
		}

		public void addChildId(int id){
			childsIds.add(id);
		}
		
		public void addChildId(List<Integer> ids){
			childsIds.addAll(ids);			
		}

		public void addParentId(int id){
			if(parentsIds == null)
				parentsIds = new ArrayList<Integer>();
			parentsIds.add(id);
		}
		
		public void addParentId(List<Integer> ids){
			if(parentsIds == null)
				parentsIds = new ArrayList<Integer>();
			parentsIds.addAll(ids);			
		}

		public int getId(){
			return id;
		}
		
		public String getCallSite(){
			return callSite;
		}	
		
		public List<Integer> getChildsId(){
			return childsIds;
		}
		public List<Integer> getParentsId(){
			if(parentsIds == null)
				parentsIds = new ArrayList<Integer>();
			return parentsIds;
		}
	}	
	
	public static class Pair{
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
}


