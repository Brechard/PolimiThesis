package com.polimi.thesis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.polimi.thesis.Variables.MethodsType;
import com.polimi.thesis.Variables.NodeType;

public class MainClass {
	
	private static int nJobs;
	private static int nNodes;
	private static String file;
	private static int nNodesJobBefore;
	private static Set<String> listRDDs;
	private static Set<String> listSC;
	
	private static List<Job> jobsList = new ArrayList<Job>();
	private static Map<Integer, Stage> stages = new HashMap<Integer, Stage>();
	private static int nStages;
	private static List<Condition> conditions;
	private static List<String> loops;
	private static PairMapInt pairMapInt;

	//  List of conditional cases supported (edges)
	public static void main(String[] args) throws Exception {
		nJobs = 0; nNodes = 0; nNodesJobBefore = 0;

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
		
		ConditionsAndLoops conditionsAndLoops = FindHelper.findIfsAndLoops(file, 0);
		conditions = conditionsAndLoops.getIfs();
		loops = conditionsAndLoops.getLoops();
		listRDDs = FindHelper.findRDDs(file);
		listSC = FindHelper.findSC(file);		

		prettyPrint(conditions);
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
					String callSite = m.group();
					if (file.charAt(m.start() - 2) != ')' && (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC))) 
						callSite = cache+ "." +m.group();
					else {
						int start = SearchHelper.searchStartParenthesisBlock(file, m.start());
						callSite = file.substring(start, m.start()) + m.group();
					}
					callSite = String.valueOf(m.start() +"-"+ callSite);
					Job job = new Job(nJobs++, callSite);
					System.out.println(">>>>>>>>>>>>>>>>> " +callSite);
					Inside inside = CheckHelper.checkInsideIf(m.start(), conditions);
					if(inside.isInside())
						job.setCondition(inside.getCondition());
					inside = CheckHelper.checkInsideLoop(m.start(), loops);
					if(inside.isInside())
						job.setLoop(inside.getCondition());
					jobsList.add(job);					
				}
			}
			if(!m.group().replaceAll(" ", "").equals(""))
				cache = m.group();
		}
		System.out.println("Number of jobs: " +nJobs);
		for(int i = 0; i < nJobs; i++) { // We iterate over the map with the spark methods
			Job job = jobsList.get(i);
			stages = new HashMap<Integer, Stage>();
			
			int key = Integer.valueOf(job.getCallSite().split("-")[0]);
			String method = job.getCallSite().split("-")[1];
			// Method has right now is something.action and this something may be an RDD or an SC in the form of a variable or it may be (something)
			String rddOrSc = "";
			String[] w = method.split("\\.");
			method = w[w.length - 1];				
			rddOrSc = w[0];
  
			char c = rddOrSc.charAt(rddOrSc.length() - 1);

			// We have to find the block of code that this action refers to
			if(c != ')'){
				if (CheckHelper.checkRDD(rddOrSc, listRDDs)) { // If it is applied to an Node we have to find where and how it was created
					generate(FindHelper.findBlock(rddOrSc, file.length(), file), null, null, false, null);
				} else { // If it is applied to an sc there is an error				
					throw new Exception(method+", An action cannnot be applied to an SC: " +rddOrSc+"-"+file.substring(key -20, key - 2)+"-");
				}
			} else {
				/*
				 * Here we want to find the block where the action is called so we can start analyzing the code from here
				 * In order to do so, we first have to check if the action is already inside a parenthesis. We add 2 to the end variable because
				 * 1 corresponds to the dot before the method and another to be able to check what's after the method
				 */
				int end = key + method.length() + 2; 
				if(file.charAt(end) == '(')
						end = SearchHelper.searchEndParenthesis(end, file);
				// Number of parenthesis we can find that the method is already inside of
				int numberOfClosingParenthesis = 0;
				while(file.charAt(end) != ';'){					
					if(file.charAt(end) == ')')
						numberOfClosingParenthesis++;
					else if(file.charAt(end) == '(')
						end = SearchHelper.searchEndParenthesis(end, file);					
					end++;
				}
				int start = SearchHelper.searchStartBlock(file, key, numberOfClosingParenthesis);
				List<BlockOfCode> blocksOfCode = new ArrayList<BlockOfCode>();
				blocksOfCode.add(new BlockOfCode(file.substring(start, key - 1), start, file.substring(start).indexOf('.')));
				generate(blocksOfCode, null, null, false, null);
			}
			updatenodes();
			// Here we should create the stages
			/*
			int n = 0;
			if(pairMapInt != null)
				n = pairMapInt.getnStages();
			pairMapInt = CreateStages.create(stages, n);
			stages = pairMapInt.getStagesList();
			*/
			for (Map.Entry<Integer, Stage> entry : stages.entrySet()){
				// This is done in order to have the nodes as a map with key their id instead of in a list as we have being doing all the time
				job.addStage(entry.getKey(), new StageMap(entry.getValue()));				
			}
			jobsList.set(i, job);
		}
		
		// Here we should update the values of the stages

		/*
			
//		prettyPrint(jobsList);
		nStages = -1;
		for(int i = jobs - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			nStages += jobsList.get(i).getStages().size();
		}
		System.out.println("Number of stages: " +nStages);
		for(int i = jobs - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			stages = jobsList.get(i).getStages();
			updateStages();	
		}
		
		*/

		prettyPrint(jobsList);
		cleanJSON();
		
		System.out.println("\nNumber of jobs: " +jobsList.size());
		prettyPrint(jobsList);
		System.out.print("ifs -> [");
		for(Condition con: conditions)
			System.out.print(" ^^^ " +con.getStart()+" - ("+ con.getCondition()+ ") - "+con.getEnd());
		System.out.println("]");
		System.out.println("loops -> " +loops);

	}
		
	public static void prettyPrint(Object a){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();		
		System.out.println(gson.toJson(a));
	}

	/**
	 * Analyzes the block considering if the methods are inside a condition or not
	 * @param blocksOfCode List containing the blocks of code that may need to be analyze
	 * @param stage Stage with all the nodes
	 * @param childId Identifier of the child node
	 * @param check Boolean parameters that will indicate if we have entered in a condition previously or not. If it is true we will have to check if the node we are going to create 
	 * has already been created and therefore it does not have to be created again. This will happen with the methods after a fork.
	 * @param childType Type of the child node, it can be: RDD, Join, Fork
	 */
	public static void generate(List<BlockOfCode> blocksOfCode, Stage stage, Integer childId, boolean check, NodeType childType){
		System.out.println(">>> ChildId: " +childId+", childType = " +childType);
		int size = blocksOfCode.size() - 1;
		Inside inside = CheckHelper.checkInsideIf(blocksOfCode.get(size).getPosFirstMethod(), conditions);
		List<Integer> idsIfsOriginalUpdated = inside.getIdIfs();
		List<Integer> idsIfsActual = idsIfsOriginalUpdated;
		Node childRDD = null;
		int pos = 0;
		List<Integer> idsIfsChild = new ArrayList<Integer>();
		if(stage != null){
			Node childNode = stage.getNode(childId, childType);
			int idChild;
			// We found a join or fork, so we have to keep searching until rdd is found
			while(childNode != null && !childNode.isRDD() && childNode.getChildrenId().size() > 0){
				idChild = childNode.getChildrenId().get(0);
				childNode = stage.getNode(idChild);
				if(childNode.getChildrenId().size() == 0)
					break;
				idChild = childNode.getChildrenId().get(0);
			}
			childRDD = childNode;
			if(childRDD != null){
				pos = Integer.valueOf(childRDD.getCallSite().split(" at char ")[1]);
				idsIfsChild = CheckHelper.checkInsideIf(pos, conditions).getIdIfs();							
			}
		}
		/*
		 * We have to check the blocks of code in order to find which of those should be analyzed or not. Usually we have to analyze the last occurrence only but if
		 * it inside a condition then more than one block will probably has to be analyze.
		 */
		while(size >= 0 && inside.isInside() || size == blocksOfCode.size() - 1){
			/*
			 * If the last method is inside a condition probably more than a block will be analyzed, if not we just have to analyze the last block
			 */
			if(inside.isInside() && (size < blocksOfCode.size() - 1 || inside.getIsLast())){
				if(!idsIfsActual.equals(idsIfsOriginalUpdated)){
					if(idsIfsActual.size() > idsIfsOriginalUpdated.size()
							&& idsIfsActual.containsAll(idsIfsOriginalUpdated)){
						idsIfsOriginalUpdated = idsIfsActual;
					} else {
						if(idsIfsActual.size() > idsIfsChild.size() 
								&& !inside.getIsLast()){
							size--;
							if(size >= 0){
								inside = CheckHelper.checkInsideIf(blocksOfCode.get(size).getPosFirstMethod(), conditions);
								idsIfsActual = inside.getIdIfs();
							}
							continue;
						}
					}
				} else if(idsIfsActual.equals(idsIfsOriginalUpdated)
						&& idsIfsActual.equals(idsIfsChild)){
						size--;
						if(size >= 0){
							inside = CheckHelper.checkInsideIf(blocksOfCode.get(size).getPosFirstMethod(), conditions);
							idsIfsActual = inside.getIdIfs();
						}
						continue;					
				}
				generate(blocksOfCode.get(size), stage, childId, true, childType);
			} else { 
				generate(blocksOfCode.get(size), stage, childId, check, childType);
			}
			size--;
			if(size >= 0){
				inside = CheckHelper.checkInsideIf(blocksOfCode.get(size).getPosFirstMethod(), conditions);
				idsIfsActual = inside.getIdIfs();
			}			
		}
	}
		
	/**
	 * Analyzes a single block of code in order to find the relations inside the block
	 * @param blockOfCode Block of code that is going to be analyzed
	 * @param stage Stage with all the nodes 
	 * @param childId Unique identifier (id) of the child node
	 * @param check Boolean parameters that will indicate if we have entered in a condition previously or not. If it is true we will have to check if the node we are going to create 
	 * has already been created and therefore it does not have to be created again. This will happen with the methods after a fork.
	 * @param childType Type of the child node, it can be: RDD, Join, Fork
	 */
	public static void generate(BlockOfCode blockOfCode, Stage stage, Integer childId, boolean check, NodeType childType){
		String block = blockOfCode.getBlock();
		int beginning = blockOfCode.getFirstPos(); // We get the position from the object because is the position in the file string and not in the block
		String cache = "";
		List<String> forward = new ArrayList<String>(); 	
		List<Stage> parentsList = new ArrayList<Stage>();
		List<Integer> nodesParentsId = new ArrayList<Integer>();
		List<NodeType> nodesParentsType = new ArrayList<NodeType>();

		// First we read the block to create the relations inside this block backwards, in order to do it we need to read it forward and analyze it backwards		
		forward = FindHelper.findMethods(block, beginning);
		int p = 0;
		int position = 0;
		// Create dependencies of the block passed analyzing backwards
		for (int j = forward.size() - 1; j >= 0; j--) {
			String method = forward.get(j).split("-")[1];
			String pos = forward.get(j).split("-")[2];
			if (stage == null)
				stage = new Stage(0);
			Stage childCache = stage;

			// Find the child Node for the partitioner
			List<Node> nodesChild1 = stage.getNodes();
			Node rddChild1 = null;
			if(nodesChild1.size() > 0)
				rddChild1 = nodesChild1.get(0);
			String methodChild = "";
			if(rddChild1 != null)
				methodChild = rddChild1.getCallSite().split("at")[0].replaceAll(" ", "");
			String callSite = method+" at char " +pos;
			int exists = -1;
			/*
			 * If check is true it means that we have been through a condition, therefore, the Node we want to create may be already created and we have to check if it is like that
			 * in order to not create it and that way we will avoid having duplicated Nodes.
			 * We don't have to check always, sometime nodes have to be duplicated because they are going to be in different stages with different parents or children
			 * Those who are repeated if they end up creating duplicated stages will be deleted when creating the stages
			 */
			if(check)
				exists = CheckHelper.checkExistenceNode(callSite, stage);
			Node rdd1 = null;
			Inside inside = null;
			if(exists >= 0){
				for(Node rdd: nodesChild1)
					if(rdd.getId() == exists){
						rdd1 = rdd; 
						inside = CheckHelper.checkInsideIf(Integer.valueOf(pos), conditions);
					}
			} else {				
				inside = CheckHelper.checkInsideIf(Integer.valueOf(pos), conditions);
				if(inside.isInside()){
					/*
					 * If the method we are analyzing right now is the last of a condition we should create a JOIN node
					 */
					if(inside.getIsLast()){
						List<Integer> idIfsLast = inside.getIdIfsLast(); // List with the ids of the conditions that this methods is the last of
						for(int i = 0; i < idIfsLast.size(); i++){
							int id = CheckHelper.checkExistenceNode(idIfsLast.get(i), stage, NodeType.join);
							if(id > 0){
								Node node = stage.getNode(id, NodeType.join);
								if(!node.getChildrenId().contains(childId))
									node.addChildId(childId);										
								rdd1 = node;
							} else {
								rdd1 = new Node(nNodes++, NodeType.join);							
								rdd1.setConditionId(idIfsLast.get(i));
								rdd1.addChildId(childId);
								stage.addNode(rdd1);		
								Node node = stage.getNode(childId);
								node.addParentId(rdd1.getId());
							}
							// Add the parents relations
							for(Condition con: conditions){
								if(con.getId() == rdd1.getConditionId()){
									for(Integer idParent: con.getIdParentCondition()){
										Node node = stage.getNode(idParent, childType);
										if(node != null && !node.getParentsId().contains(rdd1.getId())){
											node.addParentId(rdd1.getId());
										}
									}
								}
							}
							childId = rdd1.getId();
							childType = NodeType.join;
						}	
						/*
						 * Create the new RDD but as a separate variable so we can add to the join the parent relationship
						 */
						Node rdd2 = createRDD(callSite, method, methodChild);
						if(inside.getIsLast())
							rdd1.addParentId(rdd2.getId());
						rdd1 = rdd2;
					} else {
						rdd1 = createRDD(callSite, method, methodChild);
						if(childId != null && childId > -1){
							Node node = stage.getNode(childId);
							if(node != null)
								node.addParentId(rdd1.getId());
						}
					}
					Inside insideLoop = CheckHelper.checkInsideLoop(Integer.valueOf(pos), loops);
					if(insideLoop.isInside())
						rdd1.setLoop(insideLoop.getCondition());
				} else {
					rdd1 = createRDD(callSite, method, methodChild);
					if(childId != null && childId > -1){
						Node node = stage.getNode(childId);
						if(node != null)
							node.addParentId(rdd1.getId());
					}
					Inside insideLoop = CheckHelper.checkInsideLoop(Integer.valueOf(pos), loops);
					if(insideLoop.isInside())
						rdd1.setLoop(insideLoop.getCondition());
				}
			}
			if(rdd1 == null){
				try{
					throw new Error("Error creating Node");				
				} catch(Error e){
					System.out.println("Error: " +e);
				}
			}
			/*
			 * If childId is not null we have to add the parent relationship
			 */
			if (childId != null){
				if(inside == null || !inside.isInside() || !inside.getIsLast())
					rdd1.addChildId(childId);					
				Node nodeChild = stage.getNode(childId, childType);
				if(nodeChild != null && !nodeChild.getParentsId().contains(rdd1.getId())){
					if(inside == null || !inside.isInside() || !inside.getIsLast()){
						if(inside != null)
							System.out.println("Inside: " +inside.isInside()+", is last: " +inside.getIsLast());
						else System.out.println("Inside is null"); 
						nodeChild.addParentId(rdd1.getId());							
						System.out.println("Added parent " +rdd1.getId()+ " to:" );
						prettyPrint(nodeChild);							
					}
				}										
			}
			// Update the childId with the new value
			childId = rdd1.getId();
			if(exists == -1)
				stage.addNode(rdd1);
			/*
			 * If the method we are analyzing is the first method of a condition we have to create a FORK node
			 */
			if(inside != null && inside.isInside() && inside.getIsFirst()){
				List<Integer> idIfsFirst = inside.getIdIfsFirst();
				for(int i = 0; i < idIfsFirst.size(); i++){
					int id = CheckHelper.checkExistenceNode(idIfsFirst.get(i), stage, NodeType.fork);
					if(id > 0){
						rdd1.addParentId(id);
						Node node = stage.getNode(id, NodeType.fork);
						node.addChildId(rdd1.getId());
						rdd1 = node;
					} else {
						Node node = new Node(nNodes++, NodeType.fork);							
						node.setConditionId(idIfsFirst.get(i));
						node.addChildId(rdd1.getId());
						rdd1.addParentId(node.getId());
						rdd1 = node;
						stage.addNode(rdd1);														
					}
					childId = rdd1.getId();
					childType = NodeType.fork;
				}
			}

			parentsList.add(position, stage);
			nodesParentsId.add(position, childId);
			nodesParentsType.add(position, childType);
			/*
			 * If a method is a combine method of two RDDs we prepare for it and therefore we have to create another space for the second child
			 */
			if (CheckHelper.checkCombine(method)) {
				position++;
				stage.addChildId(childCache.getId());				
				parentsList.add(position, stage);
				nodesParentsId.add(position, childId);
				nodesParentsType.add(position, childType);
			}
			position++;
		}
		p = 0;
		position = 0;
		Boolean changedPosition = false;
		// Create dependencies with blocks that appear as variables or inside parenthesis
		if (forward.size() > 0){
			for (int j = forward.size(); j >= 0; j--) {
				/*
				 * This first case has as objective analyze the last method of the block because if it is a combine method we have to analyze it 
				 */
				if (j == forward.size() && forward.size() > 0) {
					int pos = Integer.valueOf(forward.get(j - 1).split("-")[0]);
					String method = forward.get(j - 1).split("-")[1];
					int start = pos + method.length();
					int posReal = Integer.valueOf(forward.get(j - 1).split("-")[2]);
					if(CheckHelper.checkCombine(method)){ // The last method of the block is combine Method
						int endBlock = SearchHelper.searchEndBlock(start - 1, 0, block);
						cache = FindHelper.findCache(endBlock, block);
						int start2= start + 1;
						int end = SearchHelper.searchEndParenthesis(start2, block);
						if(!CheckHelper.checkRDD(block.substring(start2, end).replace(" ","").replaceAll("\\(","").replaceAll("\\)", ""), listRDDs)){
							List<BlockOfCode> list = new ArrayList<BlockOfCode>();
							list.add(new BlockOfCode(block.substring(start2, end +1), posReal +method.length(), block.substring(start2).indexOf('.') + posReal + method.length() + start2));
							generate(list, parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
							continue;
						}
						/*
						 * Position is added one in case we are with a combine method. This will allow us to after analyzing the method go back to position zero and therefore still having
						 * the information of the childId for the second part of the combination and for the next method
						 */
						position = position + 1;
						changedPosition = true;
						if (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC)) { 
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
						} else {
							String subBlock = block.substring(start, block.length());
							List<BlockOfCode> pairL = new ArrayList<BlockOfCode>();
							pairL.add(new BlockOfCode(subBlock, start + beginning, start + beginning + subBlock.indexOf('.')));
							generate(pairL, parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));
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
				if(block.charAt(pos - 2) == ')'){ // Method not applied directly to a variable, we have to analyze the interior of the parenthesis
					cache = FindHelper.findCache(pos, block);
					if (block.charAt(pos - 3) == '(') { // There is nothing inside the parenthesis, the method is applied to the result of another method
						System.out.println("Nothing to do here");
					} else { // Inside of the parenthesis there is something what means that the method before (writing) may be a combine method
						/*
						 * Position is added one in case we are with a combine method. This will allow us to after analyzing the method go back to initial position and therefore still having
						 * the information of the childId for the second part of the combination and for the next method
						 */
						position = position + 1;							
						changedPosition = true;
						int start2 = SearchHelper.searchStartParenthesisBlock(block, pos);
						Boolean search = true;
						if(!CheckHelper.checkRDD(block.substring(start2, pos).replace(" ","").replaceAll("\\(","").replaceAll("\\)", "").replaceAll("\\.", ""), listRDDs))
							search = false;

						if (search && (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC))) { 
							String methodBefore ="";
							if (j - 1 >= 0) {
								methodBefore = forward.get(j - 1).split("-")[1];
								if(!CheckHelper.checkCombine(methodBefore) && changedPosition){ // If it was not a combine method we have to undo the addition
									position = position - 1;
									changedPosition = false;
								}
							} else{
								if (changedPosition) {
									position = position - 1;							
									changedPosition = false;
								}
							}
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
						} else {
							if (j - 1 >= 0) {
								String methodBefore = forward.get(j - 1).split("-")[1];
								if(!CheckHelper.checkCombine(methodBefore) && changedPosition){
									position = position - 1;							
									changedPosition = false;
								}
							} else {
								if (changedPosition) {									
									position = position - 1;							
									changedPosition = false;
								}
							}
							int start = SearchHelper.searchStartParenthesisBlock(block, pos - 2);
							String subBlock = block.substring(start, pos - 2);
							if(subBlock.charAt(0) == '('){
								subBlock = subBlock.substring(1);
							}
							List<BlockOfCode> pairL = new ArrayList<BlockOfCode>();
							pairL.add(new BlockOfCode(subBlock, posReal+start, posReal + start + subBlock.indexOf('.')));
							generate(pairL, parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));
						}	
						if (changedPosition) {
							position = position - 1;							
							changedPosition = false;
						}
					}					
				} else { // Method applied directly to a variable
					cache = FindHelper.findCache(pos, block);
					if (CheckHelper.checkRDD(cache, listRDDs)) {
						generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
					} else if(CheckHelper.checkSC(cache, listSC)) { // If the method is applied directly to an SC it means the stage finishes here so we have to add it to the stageList
						int key = parentsList.get(position).getId();
						if(stages.containsKey(key))
							stages.get(key).addAll(parentsList.get(position));
						else 
							stages.put(parentsList.get(position).getId(), parentsList.get(position));
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
						generate(FindHelper.findBlock(rdd2, file.length(), file), stage, childId, check, nodesParentsType.get(position));					
					}				
				}
			}			
		}
	}
	
	public static void updateStages(){
		
		List<Stage> childrenStagesList = new ArrayList<Stage>();
		List<Integer> keys = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : stages.entrySet())
			keys.add(entry.getKey());

		System.out.println("\n\n\n\n updateStages, " +keys);
//		prettyPrint(stages);

		// Find the stage that does not have any child wich will be the last stage, from there we will order the others ("generation" by "generation"), updating childrenId and parentsId
		for (Map.Entry<Integer, Stage> entry : stages.entrySet()){
			Stage stage = entry.getValue();
			if(stage.getChildrenId().isEmpty()){
				int oldId = stage.getId();
				stage.setId(nStages--);
				
				System.out.println("Stage emptyChild modified, new Id: " +stage.getId()+", oldId: " +oldId);
				childrenStagesList = updateStageschildId(stage.getId(), oldId, stage, new ArrayList<Stage>());
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
			if(stages.containsKey(keys.get(i)))
				stage = stages.get(keys.get(i)); //Cache where i will put the stage
			else continue;
			
			Stage cache = null;
			if(stages.containsKey(stage.getId()))
				cache = stages.get(stage.getId());
			if(cache != null){
//				System.out.println("Chache: " +stage.getId());
	//			prettyPrint(cache);
			}
			if(keys.get(i) != stage.getId()){
//				System.out.println("Put in: " +stage.getId()+", remove in: " +keys.get(i));
				stages.put(stage.getId(), stage);
				stages.remove(keys.get(i));
			}
			int pos = -1;
			while(cache != null && cache.getId() != pos){
				Stage aux = cache;
				pos = aux.getId();

				cache = stages.get(pos);
//				System.out.println("Cache: ");
//				prettyPrint(cache);
//				System.out.println("Put in: " +pos);
				stages.put(pos, aux);
			}
		}

//		System.out.println("\n\n\n\n Stages ordered without parents");
//		prettyPrint(stagesList);

		for(Integer i : keys){
			if(i >= keys.size() + (nStages+1) && stages.containsKey(i)){
				System.out.println("\n\n\nremove: "+i+"\n\n\n");
				stages.remove(i);				
			}
		}
		
//		System.out.println("\n\n\n\n Stages ordered without parents");
//		prettyPrint(stagesList);
//		System.out.println("\n\n\n\n");

		// Once the stages have been ordered we set the parents ids of the stages (if a stage is my child, I am his father), removing the previous one
		setParentIdsOfStages();
	}
	
	private static void setParentIdsOfStages(){
		for (Map.Entry<Integer, Stage> entry : stages.entrySet()){
			entry.getValue().emptyParents();
		}

		for (Map.Entry<Integer, Stage> entry : stages.entrySet()){
			Stage stage = entry.getValue();
			List<Integer> childrenIds = stage.getChildrenId();
			System.out.println("Stage: "+stage.getId()+ ", children: " +childrenIds);
			for(Integer id: childrenIds){
//				System.out.println("Id of a child: " +id);
				Stage child = stages.get(id);
//				prettyPrint(child);
//				System.out.println("ID of parent: " +stage.getId());
				if(!child.getParentsIds().contains(stage.getId()))
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
		
		for (Map.Entry<Integer, Stage> entry : stages.entrySet()){
			Stage stage = entry.getValue();
			if(stage == stageCalling || (stage.getUpdatedChild() != null && stage.getUpdatedChild())) continue;
			// Update in parents the childId
			List<Integer> childrenIds = stage.getChildrenId();
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
		
	public static void updatenodes(){
		Stage stage;
		int numberOfNodes = nNodes + nNodesJobBefore - 1;
		stage = stages.get(0);
		List<Node> listNodes = stage.getNodes();
		for(Node node: listNodes){
			node.setId(numberOfNodes - node.getId());
				
			List<Integer> childrenIds = node.getChildrenId();
			node.removeChildrenIds();
			for(Integer idChild: childrenIds){
				node.addChildId(numberOfNodes - idChild);
			}
			List<Integer> parentsIds = node.getParentsId();
			node.removeParentsIds();
			for(Integer idParent: parentsIds){
				node.addParentId(numberOfNodes - idParent);
			}				
		}		
		nNodesJobBefore += listNodes.size();
	}
	
	public static void cleanJSON(){
		StageMap stage;
		for(Job i: jobsList){
			for (Map.Entry<Integer, StageMap> entry : i.getStages().entrySet()){
				stage = entry.getValue();
				Map<Integer, Node> listNodes = stage.getNodes();
				for (Map.Entry<Integer, Node> entry2 : listNodes.entrySet()){
					Node node = entry2.getValue();
					node.removeChildrenIds();
					node.removePartitioner();
				}
			}
		}
	}
	
	public static Node createRDD(String rdd, String method, String methodChild){
		return new Node(rdd, nNodes++, new Partitioner(method, methodChild).getPartitioner(), NodeType.rdd);
	}
	
	public static void convertToMap(){
		
	}
}
