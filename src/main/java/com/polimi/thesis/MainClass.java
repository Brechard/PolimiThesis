package com.polimi.thesis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.tree.DefaultMutableTreeNode;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.polimi.thesis.Variables.MethodsType;

public class MainClass {
	
	private static int stages;
	private static int jobs;
	private static int rdds;
	private static String file;
	
	private static Set<String> listRDDs;
	private static Set<String> listSC;
	
	private static List<String> actionList = new ArrayList<String>();
	
	private static List<Map<Integer, Stage>> jobsList = new ArrayList<Map<Integer, Stage>>();
	private static Map<Integer, Stage> stagesList = new HashMap<Integer, Stage>();
	private static int nStages;
	private static List<String> ifs;
	private static List<String> loops;
	private static PairMapInt pairMapInt;

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
		
		PairList pairList = FindHelper.findIfsAndLoops(file);
		ifs = pairList.getIfs();
		loops = pairList.getLoops();
		listRDDs = FindHelper.findRDDs(file);
		listSC = FindHelper.findSC(file);		
		
		// Create a Pattern object
		Pattern r = Pattern.compile("\\w*");
		// Now create matcher object.
		Matcher m = r.matcher(file);
		
		String cache = "";
		while (m.find()) {
			if ((m.start() - 1) >= 0 && file.charAt(m.start() - 1) == '.') {
				// Now we know that m.group() is a method, we have to check if it is a spark method
				MethodsType type = CheckHelper.checkMethod(m.group());
				if (type == MethodsType.action) {
					String s = m.group();
					jobs++;
					if (file.charAt(m.start() - 2) != ')' && (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC))) 
						s = cache+ "." +m.group();
					else {
						int start = SearchHelper.searchStartParenthesisBlock(file, m.start());
						s = file.substring(start, m.start()) +"."+ m.group();
					}
//					map.put(m.start() - 1, s); We are not using TreeMap because it is too slow
					actionList.add(String.valueOf(m.start() +"-"+s));
				} 
			}
			if(!m.group().replaceAll(" ", "").equals(""))
				cache = m.group();
		}

		for(int i = 0; i < actionList.size(); i++) { // We iterate over the map with the spark methods
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
//			prettyPrint(actionList.get(i));

			// We have to find the block of code that this action refers to
			if(c != ')'){
				if (CheckHelper.checkRDD(rddOrSc, listRDDs)) { // If it is applied to an RDD we have to find where and how it was created
					generate(FindHelper.findBlock(rddOrSc, file.length(), file), null, null);
				} else { // If it is applied to an sc there is an error				
					throw new Exception(method+", An action cannnot be applied to an SC: " +rddOrSc+"-"+file.substring(key -20, key - 2)+"-");
				}
			} else {
				int start = SearchHelper.searchStartBlock(key, file);
				List<Pair> pairL = new ArrayList<Pair>();
				pairL.add(new Pair(file.substring(start, key), start));
				generate(pairL, null, null);
			}
			int n = 0;
			if(pairMapInt != null)
				n = pairMapInt.getnStages();
			pairMapInt = CreateStages.create(stagesList, n);
			stagesList = pairMapInt.getStagesList();
			jobsList.set(jobs, stagesList);
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
		
		prettyPrint(jobsList);
		
		System.out.println("ifs   -> " +ifs);
		System.out.println("loops -> " +loops);

	}
		
	public static void prettyPrint(Object a){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();		
		System.out.println(gson.toJson(a));
	}
		
	/*
	 * Analyzes a block of code in order to find the relations inside the block
	 */
	public static void generate(List<Pair> pairs, Stage child2, Integer childId){
		Pair pair = pairs.get(pairs.size() - 1);
		String block = pair.getBlock();
		int beginning = pair.getFirstPos();
		/*
		if(jobs == 0){
			System.out.println("\n>> generate");
			System.out.println("beginning: " +beginning+"\n");
			System.out.println("Block: " +block+"\n");
			System.out.println("PARENT: ");
			prettyPrint(child2);	
		}
		*/
		String cache = "";
		List<String> forward = new ArrayList<String>(); 	
		List<Stage> parentsList = new ArrayList<Stage>();
		List<Integer> rddsParentsId = new ArrayList<Integer>();

		// First we read the block to create the relations inside this block backwards, in order to do it we need to read it forward and analyze it backwards		
		forward = FindHelper.findMethods(block, beginning);
				
//		System.out.print("Forward: ");
//		prettyPrint(forward);
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
				child2 = createStage();
//			System.out.println("Received Method: " +method+ ", type: " +CheckHelper.checkMethod(method)+", childId: " +child2.getId()+", rddsParentsId: " +childId+", pos: " +pos);
			Stage childCache = child2;


			
				RDD rdd1 = createRDD(method+" at char " +pos);
				PairInside inside = CheckHelper.checkInside(Integer.valueOf(pos), "if", ifs, loops);
				if(inside.getInside())
					rdd1.setCondition(inside.getCondition());
				inside = CheckHelper.checkInside(Integer.valueOf(pos), "loop", ifs, loops);
				if(inside.getInside())
					rdd1.setLoop(inside.getCondition());
//				rdd1.addchildId(child2.getId());
				if (childId != null){
					rdd1.addChildId(childId);					
					// We have to search for the child RDDs in order to put the parentRDDId
//					System.out.println("Stage: "+child2.id);
//					prettyPrint(child2);
					if(child2.getRDDs().size() == 0){ // If in the stage there are still not rdds this means that the rdd will have his child in another stage
						List<Integer> childrentages = child2.getChildId();
						for(Integer id: childrentages){
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
				child2.addRDD(rdd1);


				
				
//			System.out.println("childId: " +child2.getId()+ " in p2: " +position);
//			System.out.println("Save childId: " +child2.getId()+ ", rddsParentsId: " +childId+ " in p2: " +position+"\n");
			parentsList.add(position, child2);
			rddsParentsId.add(position, childId);
//			prettyPrint(stagesList);
//			System.out.println("PARENT: " +position);
//			prettyPrint(child2);
			
			if (CheckHelper.checkCombine(method)) {
				position++;

				child2.addChildId(childCache.getId());
				
//				childCache.addParentId(parentNew.getId());
				
				parentsList.add(position, child2);
				rddsParentsId.add(position, childId);
//				System.out.println("rddsParentsId: " +childId+ " in p2: " +position);
//				System.out.println("PARENT COMBINE: " +position);
//				System.out.println("PARENT COMBINE");
//				prettyPrint(child2);
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
					MethodsType type = CheckHelper.checkMethod(method);
					int posReal = Integer.valueOf(forward.get(j - 1).split("-")[2]);
//					System.out.println("Second loop, check if last method: "+method+", is combine, start: " +block.charAt(start)+ ", +1: " +block.charAt(start + 1));
					if(CheckHelper.checkCombine(method)){ // The last method of the block is combine Method
						int endBlock = SearchHelper.searchEndBlock(start - 1, 0, block);
						cache = FindHelper.findCache(endBlock, block);
//						System.out.println("position cero");
						
						if(block.charAt(start + 1) != '('){ // Probably not applied directly to a variable
							int start2= start + 1;
							int end = SearchHelper.searchEndParenthesis(start2, block);
//							System.out.println("Applied to: " +block.substring(start2, end));							
							if(!CheckHelper.checkRDD(block.substring(start2, end).replace(" ","").replaceAll("\\(","").replaceAll("\\)", ""), listRDDs)){
								List<Pair> list = new ArrayList<Pair>();
								list.add(new Pair(block.substring(start2, end +1), posReal +method.length()));
								generate(list, parentsList.get(position), rddsParentsId.get(position));					
								continue;
							}
						}
						position = position + 1;
						changedPosition = true;
//						prettyPrint(parentsList);
//						prettyPrint(stagesList);
						if (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC)) { 
							if(type != MethodsType.shuffle) position = 0; // If the method is not a shuffle the following methods have to be on the same stage as this method
//							System.out.println("Combine method with variable: " +cache+", childId: " +parentsList.get(position).getId()+", in 1");
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
//							prettyPrint(forward);
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), rddsParentsId.get(position));					
						} else {
							if(!CheckHelper.checkCombine(method)){
//								System.out.println("\n\nDes Changed position\n\n ");
								position = 0;
								changedPosition  = false;
							}
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							String subBlock = block.substring(start, block.length());
//							System.out.println("Written combine method: " +subBlock+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", rddsParentsId: " +rddsParentsId.get(position));
							List<Pair> pairL = new ArrayList<Pair>();
							pairL.add(new Pair(subBlock, start + beginning));
							generate(pairL, parentsList.get(position), rddsParentsId.get(position));
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
				int posReal = Integer.valueOf(forward.get(j).split("-")[2]);
				String method = forward.get(j).split("-")[1];
				MethodsType type = CheckHelper.checkMethod(method);
				if (j - 1 >= 0) {
					String methodBefore1 = forward.get(j - 1).split("-")[1];
					type = CheckHelper.checkMethod(methodBefore1);
				}
//				System.out.println("Second loop, method: " +method+ ", p: " +p+ ", rddsParentsId: " +childId+", position: " +position+ ", block:" +block);


				if(block.charAt(pos - 2) == ')'){ // Method not applied directly to a variable, we have to analyze the interior of the parenthesis
					cache = FindHelper.findCache(pos, block);
//					System.out.println("Cache: " +cache+", p: " +p+ ", rddsParentsId: " +childId);
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
						int start2 = SearchHelper.searchStartParenthesisBlock(block, pos);
//						System.out.println("Applied to: " +block.substring(start2, pos));
						Boolean search = true;
						if(!CheckHelper.checkRDD(block.substring(start2, pos).replace(" ","").replaceAll("\\(","").replaceAll("\\)", "").replaceAll("\\.", ""), listRDDs))
							search = false;

						if (search && (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC))) { 
							String methodBefore ="";
							if (j - 1 >= 0) {
								methodBefore = forward.get(j - 1).split("-")[1];
//								System.out.println("Combine method with variable: " +cache+", methodBefore: " +methodBefore+", combine: " +CheckHelper.checkCombine(methodBefore));
								if(!CheckHelper.checkCombine(methodBefore) && changedPosition){ // If it was not a combine method we have to undo the addition
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
//							System.out.println("Combine method with variable: " +cache+", in p2: " +position+", type: " +CheckHelper.checkMethod(methodBefore));
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
//							System.out.println("Combine method with variable: " +cache+", j: " +j+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", rddsParentsId: " +rddsParentsId.get(position)+", position: "+position);
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), rddsParentsId.get(position));					
						} else {
							if (j - 1 >= 0) {
								String methodBefore = forward.get(j - 1).split("-")[1];
								if(!CheckHelper.checkCombine(methodBefore) && changedPosition){
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
							int start = SearchHelper.searchStartParenthesisBlock(block, pos - 2);
//							System.out.println("block: " +block);
							String subBlock = block.substring(start, pos - 2);
							if(subBlock.charAt(0) == '('){
								subBlock = subBlock.substring(1);
							}
//							System.out.println("Written combine method: " +subBlock+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", rddsParentsId: " +rddsParentsId.get(position));
							List<Pair> pairL = new ArrayList<Pair>();
							pairL.add(new Pair(subBlock, posReal+start));
							generate(pairL, parentsList.get(position), rddsParentsId.get(position));
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
					cache = FindHelper.findCache(pos, block);
					/*
					Boolean combine = checkCombine(forward.get(j).split("-")[1]);
					int a = 0;
					for (Stage stage : parentsList) {
						System.out.println("Stage: " +a++);
						prettyPrint(stage);
					}
					prettyPrint(stagesList);
					for (int b = 0; b < rddsParentsId.size() ; b++) {
						System.out.println("rddsParentsId: " +b+": "+rddsParentsId.get(b));
					}
					System.out.println("Changed position: " +changedPosition);
					System.out.println("Position: " +position);
					System.out.println("Applied directly to a variable: " +cache+", childId2: " +parentsList.get(position).getId());
					System.out.println("Applied directly to a variable: " +cache+", childId: " +parentsList.get(p).getId()+", p: " +p+", p2: " +position+ ", rddsParentsId: " +rddsParentsId.get(p)+", block: " +block);
					*/
					if (CheckHelper.checkRDD(cache, listRDDs)) {
//						System.out.println("Let's recall, block: "+FindHelper.findBlock(cache, posReal, file));
						generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), rddsParentsId.get(position));					
					} else if(CheckHelper.checkSC(cache, listSC)) { // If the method is applied directly to an SC it means the stage finishes here so we have to add it to the stageList
//						System.out.println("Method applied to a SC, put in the stageList");
						stagesList.put(parentsList.get(position).getId(), parentsList.get(position));											
					} else {
						System.err.println("Error in the code? " +cache+", block: " +block);
					}
				}
				p++;
				position++;
				if (CheckHelper.checkCombine(method))
					position++;
			} 
		} else { // If there are no spark methods inside the block it may be a case of newRDD = otherRDD;
			Pattern r = Pattern.compile("\\w*=\\w*");
			Matcher m = r.matcher(block.replaceAll(" ", ""));
			while(m.find()){
				String[] splited = m.group().split("=");
				if(splited.length > 1){
					String rdd1 = m.group().split("=")[0];
					String rdd2 = m.group().split("=")[1];
					if(CheckHelper.checkRDD(rdd1, listRDDs) && CheckHelper.checkRDD(rdd2, listRDDs)){
						generate(FindHelper.findBlock(rdd2, file.length(), file), child2, childId);					
					}				
				}
			}			
		}
	}
	
	public static void updateStages(){
		
		List<Stage> childrenStagesList = new ArrayList<Stage>();
		List<Integer> keys = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet())
			keys.add(entry.getKey());

		System.out.println("\n\n\n\n updateStages, " +keys);
		prettyPrint(stagesList);

		// Find the stage that does not have any child wich will be the last stage, from there we will order the others ("generation" by "generation"), updating childrenId and parentsId
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			if(stage.getChildId().isEmpty()){
				int oldId = stage.getId();
				stage.setId(nStages--);
				
				System.out.println("Stage emptyChild modified, new Id: " +stage.getId()+", oldId: " +oldId);
				childrenStagesList = updateStageschildId(stage.getId(), oldId, stage, new ArrayList<Stage>());
				break; // There can not be more than a final stage
			}				
		}			
		
		while(!childrenStagesList.isEmpty()){
//			System.out.println("A:" +a++);
			List<Stage> aux = new ArrayList<Stage>();
			for (Stage stage : childrenStagesList) {
				int oldId = stage.getId();
				stage.setId(nStages--);
				List<Stage> aux2 = updateStageschildId(stage.getId(), oldId, stage, childrenStagesList);
				for (Stage stage2 : aux2) {
					if(!aux.contains(stage2)) // If a Stages has already been added to be modified we don't have to add it twice
						aux.add(stage2);
				}
			}
			for(Stage b: childrenStagesList){
//				System.out.println("CHILD - Id:" +b.getId());
				b.setUpdatedChild(true);
			}
			childrenStagesList = aux;
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
//				prettyPrint(cache);
//				System.out.println("Put in: " +pos);
				stagesList.put(pos, aux);
			}
		}

//		System.out.println("\n\n\n\n Stages ordered withoud parents");
//		prettyPrint(stagesList);

		for(Integer i : keys){
			if(i >= keys.size() + (nStages+1) && stagesList.containsKey(i)){
				System.out.println("\n\n\nremove: "+i+"\n\n\n");
				stagesList.remove(i);				
			}
		}
		
//		System.out.println("\n\n\n\n Stages ordered withoud parents");
//		prettyPrint(stagesList);
//		System.out.println("\n\n\n\n");

		// Once the stages have been ordered we set the parents ids of the stages (if a stage is my child, I am his father), removing the previous one
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			entry.getValue().emptyParents();
		}

		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			List<Integer> childrenIds = stage.getChildId();
			System.out.println("Stage: "+stage.getId()+ ", children: " +childrenIds);
			for(Integer id: childrenIds){
//				System.out.println("Id of a child: " +id);
				Stage child = stagesList.get(id);
//				prettyPrint(child);
//				System.out.println("ID of parent: " +stage.getId());
				child.addParentId(stage.getId());
			}
			
		}
	}
	
	/*
	 * Change the ids the stages related with the one sent,as we are updating generation by generation and from the last child, we will return a list of stages that we have modified
	 */
	public static List<Stage> updateStageschildId(int newId, int oldId, Stage stageCalling, List<Stage> sameLevelStages){
		List<Stage> childrenStagesList = new ArrayList<Stage>();
		System.out.println("\nModifie stage, oldId: " +oldId+ ", newId: " +newId);
		
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			Stage stage = entry.getValue();
			if(stage == stageCalling || (stage.getUpdatedChild() != null && stage.getUpdatedChild())) continue;
			// Update in parents the childId
			List<Integer> childrenIds = stage.getChildId();
			for(int i = 0; i < childrenIds.size(); i++){
				if(childrenIds.get(i) == oldId){
					System.out.println("Stage " +stage.getId()+ " modified childId, oldId: " +oldId+", newId: " +newId);					
					childrenIds.remove(i);
					if(!childrenIds.contains(newId))
						childrenIds.add(i, newId);
					if(!sameLevelStages.contains(stage))
						childrenStagesList.add(stage);
				}
			}
			
		}	
		
		return childrenStagesList;
	}
	
	public static RDD createRDD(String rdd){
		return new RDD(rdd, rdds++);
	}
	
	public static Stage createStage(){
		return new Stage(stages);
	}
}
