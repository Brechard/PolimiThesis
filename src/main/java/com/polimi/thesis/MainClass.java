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
	
	private static int stages;
	private static int jobs;
	private static int nodes;
	private static String file;
	
	private static Set<String> listRDDs;
	private static Set<String> listSC;
	
	private static List<Job> jobsList = new ArrayList<Job>();
	private static Map<Integer, Stage> uniqueStage = new HashMap<Integer, Stage>();
	private static int nStages;
	private static List<Condition> ifs;
	private static List<String> loops;
	private static PairMapInt pairMapInt;

	//  List of conditional cases supported (edges)
	public static void main(String[] args) throws Exception {
		jobs = 0; stages = 0; nodes = 0; 

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
		
		PairList pairList = FindHelper.findIfsAndLoops(file, 0);
		ifs = pairList.getIfs();
		loops = pairList.getLoops();
		listRDDs = FindHelper.findRDDs(file);
		listSC = FindHelper.findSC(file);		
				
		prettyPrint(ifs);
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
					if (file.charAt(m.start() - 2) != ')' && (CheckHelper.checkRDD(cache, listRDDs) || CheckHelper.checkSC(cache, listSC))) 
						s = cache+ "." +m.group();
					else {
						int start = SearchHelper.searchStartParenthesisBlock(file, m.start());
						s = file.substring(start, m.start()) + m.group();
					}
					String callSite = String.valueOf(m.start() +"-"+s);
					Job job = new Job(jobs++, callSite);
					System.out.println(">>>>>>>>>>>>>>>>> " +callSite);
					PairInside inside = CheckHelper.checkInsideIf(m.start(), ifs);
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
		System.out.println("Number of jobs: " +jobs);
		for(int i = jobs - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			Job job = jobsList.get(i);
			uniqueStage = new HashMap<Integer, Stage>();
			
			int key = Integer.valueOf(job.getCallSite().split("-")[0]);
			String method = job.getCallSite().split("-")[1];
			String rddOrSc = "";
			if (method.contains(".")) {				
				String[] w = method.split("\\.");
				method = w[w.length - 1];				
				rddOrSc = w[0];
			}
  
			char c = file.charAt(key - 2);
			System.out.println("\n\nAction found: " +method+ ", a new job should be generated, jobs = "+i+"\n\n");
//			prettyPrint(actionList.get(i));

			// We have to find the block of code that this action refers to
			if(c != ')'){
				if (CheckHelper.checkRDD(rddOrSc, listRDDs)) { // If it is applied to an Node we have to find where and how it was created
					generate(FindHelper.findBlock(rddOrSc, file.length(), file), null, null, false, null);
				} else { // If it is applied to an sc there is an error				
					throw new Exception(method+", An action cannnot be applied to an SC: " +rddOrSc+"-"+file.substring(key -20, key - 2)+"-");
				}
			} else {
				int start = SearchHelper.searchStartBlock(key, file);
				List<Pair> pairL = new ArrayList<Pair>();
				pairL.add(new Pair(file.substring(start, key - 1), start, file.substring(start).indexOf('.')));
				generate(pairL, null, null, false, null);
			}
			int n = 0;
			if(pairMapInt != null)
				n = pairMapInt.getnStages();
			// Here we should create the stages
			updatenodes();
			for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet()){
				job.addStage(entry.getKey(), new StageMap(entry.getValue()));				
			}
		}

			/*
			
			pairMapInt = CreateStages.create(uniqueStage, n);
			uniqueStage = pairMapInt.getStagesList();
			job.setStages(uniqueStage);
			jobsList.set(i, job);
		}

//		prettyPrint(jobsList);
		nStages = -1;
		for(int i = jobs - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			nStages += jobsList.get(i).getStages().size();
		}
		System.out.println("Number of stages: " +nStages);
		for(int i = jobs - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			uniqueStage = jobsList.get(i).getStages();
			updateStages();	
		}
		
		*/

		prettyPrint(jobsList);
		cleanJSON();
		
		System.out.println("\nNumber of jobs: " +jobsList.size());
		prettyPrint(jobsList);
		System.out.print("ifs -> [");
		for(Condition con: ifs)
			System.out.print(" ^^^ " +con.getStart()+" - ("+ con.getCondition()+ ") - "+con.getEnd());
		System.out.println("]");
		System.out.println("loops -> " +loops);

	}
		
	public static void prettyPrint(Object a){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();		
		System.out.println(gson.toJson(a));
	}

	/*
	 * Analyzes the block considering if the methods are inside a condition or not
	 */
	public static void generate(List<Pair> pairs, Stage child2, Integer childId, boolean check, NodeType childType){
		System.out.println(">>> ChildId: " +childId+", childType = " +childType);
		int s = pairs.size() - 1;
		PairInside pairInside = CheckHelper.checkInsideIf(pairs.get(s).getPosFirstMethod(), ifs);
		List<Integer> idsIfsOriginalUpdated = pairInside.getIdIfs();
		List<Integer> idsIfsActual = idsIfsOriginalUpdated;
		Node childRDD = null;
		int pos = 0;
		List<Integer> idsIfsChild = new ArrayList<Integer>();
		if(child2 != null){
			Node childNode = child2.getNode(childId, childType);
			int idChild;
			// We found a join or fork, so we have to keep searching until rdd is found
			while(childNode != null && !childNode.isRDD() && childNode.getChildrenId().size() > 0){
				idChild = childNode.getChildrenId().get(0);
				System.out.println("ChildId: " +idChild);
				childNode = child2.getNode(idChild);
				
				if(childNode.getChildrenId().size() == 0)
					break;
				idChild = childNode.getChildrenId().get(0);
			}
			prettyPrint(childNode);
			childRDD = childNode;
			if(childRDD != null){
				pos = Integer.valueOf(childRDD.getCallSite().split(" at char ")[1]);
				idsIfsChild = CheckHelper.checkInsideIf(pos, ifs).getIdIfs();							
			}
		}
		while(s >= 0 && pairInside.isInside() || s == pairs.size() - 1){
			System.out.println(">>><<<< ChildId: " +childId+", s = " +s+", size: " +(pairs.size() - 1));
			callA(pairs,idsIfsActual, idsIfsOriginalUpdated, idsIfsChild, s);
			if(pairInside.isInside() && (s < pairs.size() - 1 || pairInside.getIsLast())){
				if(!idsIfsActual.equals(idsIfsOriginalUpdated)){
					if(idsIfsActual.size() > idsIfsOriginalUpdated.size()
							&& idsIfsActual.containsAll(idsIfsOriginalUpdated)){
						idsIfsOriginalUpdated = idsIfsActual;
						System.out.println(">>>>>>>>>>>>>0 NO Continue, " +pairs.get(s).getBlock());
					} else {
						if(idsIfsActual.size() > idsIfsChild.size() 
								&& !pairInside.getIsLast()){
							s--;
							System.out.println(">>>>>>>>>>>>>1 Continue, " +pairs.get(s+1).getBlock()+
									", pos: " +pairs.get(s+1).getPosFirstMethod()+
									", pairInside: ");
							prettyPrint(pairInside);
							if(s >= 0){
								pairInside = CheckHelper.checkInsideIf(pairs.get(s).getPosFirstMethod(), ifs);
								idsIfsActual = pairInside.getIdIfs();
							}
							continue;
						} else { 
							System.out.println(">>>>>>>>>>>>>3 NO Continue, " +pairs.get(s).getBlock());
							System.out.println(">> NO Continue, " +(idsIfsActual.size() > idsIfsChild.size())+
									", b: " +!pairInside.getIsLast()+
									", c: " +idsIfsOriginalUpdated.contains(idsIfsActual));
						}

					}
				} else if(idsIfsActual.equals(idsIfsOriginalUpdated)
						&& idsIfsActual.equals(idsIfsChild)){
						s--;
						if(s >= 0){
							pairInside = CheckHelper.checkInsideIf(pairs.get(s).getPosFirstMethod(), ifs);
							idsIfsActual = pairInside.getIdIfs();
						}
						System.out.println(">>>>>>>>>>>>>2 Continue, " +pairs.get(s+1).getBlock());
						continue;					
				}
				System.out.println(">>>>>>>>1 ChildId: " +childId);
				generate(pairs.get(s), child2, childId, true, childType);
				System.out.println("<<<<<<<<1 ChildId: " +childId);
			} else { 
				System.out.println(">>>>>>>>2 ChildId: " +childId);
				generate(pairs.get(s), child2, childId, check, childType);
				System.out.println("<<<<<<<<2 ChildId: " +childId);

			}
			s--;
			if(s >= 0){
				pairInside = CheckHelper.checkInsideIf(pairs.get(s).getPosFirstMethod(), ifs);
				idsIfsActual = pairInside.getIdIfs();
			}			
		}
	}
	private static void callA(List<Pair> pairs, List<Integer> a, List<Integer> b, List<Integer> c, Integer s){
		System.out.print(">>>>>>> Pairs: \n");
		for(Pair p: pairs)
			System.out.println("-" +p.getBlock()+", pos: " +p.getPosFirstMethod());
		System.out.println("Checking: " +pairs.get(s).getBlock());
		System.out.println(">>>>>>>> actual id ifs: " +a);
		System.out.println(">>>>> originalU id ifs: " +b);
		System.out.println(">>>>> originalN id ifs: " +c);
	}
		
	/*
	 * Analyzes a block of code in order to find the relations inside the block
	 */
	public static void generate(Pair pair, Stage child2, Integer childId, boolean check, NodeType childType){
		String block = pair.getBlock();
		int beginning = pair.getFirstPos();
		System.out.println("\n>> generate, beggining: " +beginning+", childId: " +childId+", childType: " +childType);
		System.out.println("Block: " +block+"\n");
//		if(check)
//			System.err.println("CHECK IS TRUE");
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
		List<Integer> nodesParentsId = new ArrayList<Integer>();
		List<NodeType> nodesParentsType = new ArrayList<NodeType>();

		// First we read the block to create the relations inside this block backwards, in order to do it we need to read it forward and analyze it backwards		
		forward = FindHelper.findMethods(block, beginning);
				
//		System.out.print("Forward: ");
//		prettyPrint(forward);
		/*
		for (int b = 0; b < nodesParentsId.size() ; b++) {
			System.out.println("generate, rddchildId: " +b+": "+nodesParentsId.get(b));
		}
		if (nodesParentsId.isEmpty()) {		int nStages = uniqueStage.size() - 1;

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
//			System.out.println("Received Method: " +method+ ", type: " +CheckHelper.checkMethod(method)+", childId: " +child2.getId()+", nodesParentsId: " +childId+", pos: " +pos);
			Stage childCache = child2;

			// Find the child Node for the partitioner
			List<Node> nodesChild1 = child2.getNodes();
			Node rddChild1 = null;
			if(nodesChild1.size() > 0)
				rddChild1 = nodesChild1.get(0);
			String s = "";
			if(rddChild1 != null)
				s = rddChild1.getCallSite().split("at")[0].replaceAll(" ", "");
			String callSite = method+" at char " +pos;
			System.out.println("Method callSite: " +callSite);
			int exists = -1;
			if(check)
				exists = CheckHelper.checkExistenceNode(callSite, child2);
			Node rdd1 = null;
			PairInside inside = null;
			System.out.println("Exist: " +exists);
			if(exists >= 0){
				for(Node rdd: nodesChild1)
					if(rdd.getId() == exists){
						rdd1 = rdd; 
						inside = CheckHelper.checkInsideIf(Integer.valueOf(pos), ifs);
						System.out.println("Node already exists: " +rdd.getId());
					}
			} else {				
				inside = CheckHelper.checkInsideIf(Integer.valueOf(pos), ifs);
				if(inside.isInside()){
					
					if(inside.getIsLast()){
						List<Integer> idIfsLast = inside.getIdIfsLast();
						System.err.println("Method: " +method+" is last, " +idIfsLast);
						for(int i = 0; i < idIfsLast.size(); i++){
							int id = CheckHelper.checkExistenceNode(idIfsLast.get(i), child2, NodeType.join);
							if(id > 0){
								Node node = child2.getNode(id, NodeType.join);
								if(!node.getChildrenId().contains(childId))
									node.addChildId(childId);										
								rdd1 = node;
							} else {
								rdd1 = new Node(nodes++, NodeType.join);							
								rdd1.setConditionId(idIfsLast.get(i));
								rdd1.addChildId(childId);
								child2.addNode(rdd1);		
								Node node = child2.getNode(childId);
								System.out.println("Let's add3 as parent " +rdd1.getId()+" to: ");
								prettyPrint(node);
								node.addParentId(rdd1.getId());
							}
							for(Condition con: ifs){
								if(con.getId() == rdd1.getConditionId()){
									for(Integer idParent: con.getIdParentCondition()){
										Node node = child2.getNode(idParent, childType);
//										Node node = child2.getNode(-idParent);
										System.out.println("Let's add2 as parent " +rdd1.getId()+" to: ");
										prettyPrint(node);
										if(node != null && !node.getParentsId().contains(rdd1.getId())){
											node.addParentId(rdd1.getId());
										}
									}
								}
							}
							if(idIfsLast.size() > 1){
								Node node = child2.getNode(childId, childType);
//								Node node = child2.getNode(childId);
								System.out.println("Let's add as parent " +rdd1.getId()+" to: ");
								prettyPrint(node);
								if(node != null && !node.getParentsId().contains(rdd1.getId())){
									node.addParentId(rdd1.getId());
								}
							}
							childId = rdd1.getId();
							childType = NodeType.join;
						}						
						Node rdd2 = createRDD(callSite, method, s);
						System.out.println("Let's add4 as parent " +rdd2.getId()+" to: ");
						prettyPrint(rdd1);
						if(inside.getIsLast() && !rdd1.getParentsId().contains(rdd2.getId()))
							rdd1.addParentId(rdd2.getId());
						rdd1 = rdd2;
					} else {
						rdd1 = createRDD(callSite, method, s);
						System.out.println("Let's add5 as parent " +rdd1.getId()+" to: ");
						if(childId != null && childId > -1){
							Node node = child2.getNode(childId);
							prettyPrint(node);
							if(node != null)
								node.addParentId(rdd1.getId());
						}
					}
					
					PairInside insideLoop = CheckHelper.checkInsideLoop(Integer.valueOf(pos), loops);
					if(insideLoop.isInside())
						rdd1.setLoop(insideLoop.getCondition());
					System.out.println("HHHHHHHHHHHHHH");
					prettyPrint(child2);
				} else {
					System.err.println("Create rdd");
					rdd1 = createRDD(callSite, method, s);
					PairInside insideLoop = CheckHelper.checkInsideLoop(Integer.valueOf(pos), loops);
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
			if(inside != null){
				System.out.println("0Conditioned: " +inside.isInside()+ ", parentId: " +rdd1.getId()+ ", childId = " +childId);									
				if(inside.isInside())
					System.err.println("0Conditioned: idIfs =" +inside.getIdIfs()+", first: " +inside.getIsFirst()+", last: " +inside.getIsLast());									
				
			}
				
//				rdd1.addchildId(child2.getId());
			if (childId != null){
				if(inside == null || !inside.isInside() || !inside.getIsLast())
					rdd1.addChildId(childId);					
				// We have to search for the child nodes in order to put the parentRDDId
//					System.out.println("Stage: "+child2.id);
//					prettyPrint(child2);
				if(child2.getNodes().size() == 0){ // If in the stage there are still not nodes this means that the rdd will have his child in another stage
					List<Integer> childrenStages = child2.getChildrenId();
					for(Integer id: childrenStages){
						Stage stageChild = uniqueStage.get(id);
//							System.out.println("Stage list id: " +id+", being search by: " +child2.id);
//							prettyPrint(stagesList);
						List<Node> nodesChild = stageChild.getNodes();
						for(Node rddChild: nodesChild){
//								System.out.println("Rdd child: "+rddChild.getId()+", childId: " +childId);
							if(rddChild.getId() == childId && !rddChild.getParentsId().contains(rdd1.getId())){
								if(inside == null || !inside.isInside() || !inside.getIsLast())
									rddChild.addParentId(rdd1.getId());
								/*
								if(inside != null){
									System.out.println("1Conditioned: " +inside.isInside()+ ", parentId: " +rdd1.getId()+ ", childId: " +rddChild.getId());									
								}
								System.out.println("Added parentId to: " +rddChild.getId());
								if(inside != null && inside.isInside()){
									rddChild.addCondition(rdd1.getId()+ ", " +inside.getCondition());
								}
								*/
							}
						}
					}						
				} else {
					Node nodeChild = child2.getNode(childId, childType);
//					Node nodeChild = child2.getNode(childId);
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
			}
			childId = rdd1.getId();			
			if(exists == -1)
				child2.addNode(rdd1);
			if(inside != null && inside.isInside() && inside.getIsFirst()){
				List<Integer> idIfsFirst = inside.getIdIfsFirst();
				System.err.println("Method: " +method+" is first, " +idIfsFirst);
				for(int i = 0; i < idIfsFirst.size(); i++){
					int id = CheckHelper.checkExistenceNode(idIfsFirst.get(i), child2, NodeType.fork);
					if(id > 0){
						rdd1.addParentId(id);
						Node node = child2.getNode(id, NodeType.fork);
						System.err.println("Node to change: ");
						prettyPrint(node);
						node.addChildId(rdd1.getId());
						rdd1 = node;
					} else {
						Node node = new Node(nodes++, NodeType.fork);							
						node.setConditionId(idIfsFirst.get(i));
						node.addChildId(rdd1.getId());
						System.err.println("Node created: ");
						prettyPrint(node);
						System.err.println("Node to update: ");
						prettyPrint(rdd1);
						rdd1.addParentId(node.getId());
						rdd1 = node;
						child2.addNode(rdd1);														
					}
					childId = rdd1.getId();
					childType = NodeType.fork;
					System.out.println("ChildId setted to: " +childId);
				}
			}

//			System.out.println("childId: " +child2.getId()+ " in p2: " +position);
			System.out.println("Save childId: " +child2.getId()+ ", nodesParentsId: " +childId+ " in p2: " +position+"\n");
			parentsList.add(position, child2);
			nodesParentsId.add(position, childId);
			nodesParentsType.add(position, childType);
//			prettyPrint(stagesList);
//			System.out.println("PARENT: " +position);
//			prettyPrint(child2);
			
			if (CheckHelper.checkCombine(method)) {
				position++;

				child2.addChildId(childCache.getId());
				
//				childCache.addParentId(parentNew.getId());
				
				parentsList.add(position, child2);
				nodesParentsId.add(position, childId);
				nodesParentsType.add(position, childType);
//				System.out.println("nodesParentsId: " +childId+ " in p2: " +position);
//				System.out.println("PARENT COMBINE: " +position);
//				System.out.println("PARENT COMBINE");
//				prettyPrint(child2);
			}
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
						System.out.println("Block: " +block);
						System.out.println("position cero, cache: " +cache);
						
						int start2= start + 1;
						int end = SearchHelper.searchEndParenthesis(start2, block);
//							System.out.println("Applied to: " +block.substring(start2, end));							
						if(!CheckHelper.checkRDD(block.substring(start2, end).replace(" ","").replaceAll("\\(","").replaceAll("\\)", ""), listRDDs)){
							List<Pair> list = new ArrayList<Pair>();
							list.add(new Pair(block.substring(start2, end +1), posReal +method.length(), block.substring(start2).indexOf('.') + posReal + method.length() + start2));
							generate(list, parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
							continue;
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
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
						} else {
							if(!CheckHelper.checkCombine(method)){
//								System.out.println("\n\nDes Changed position\n\n ");
								position = 0;
								changedPosition  = false;
							}
//							if (changedPosition)
//								System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nChanged position\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
							String subBlock = block.substring(start, block.length());
//							System.out.println("Written combine method: " +subBlock+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", nodesParentsId: " +nodesParentsId.get(position));
							List<Pair> pairL = new ArrayList<Pair>();
							pairL.add(new Pair(subBlock, start + beginning, start + beginning + subBlock.indexOf('.')));
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
				System.out.println("Second loop, method: " +method+ ", p: " +p+ ", nodesParentsId: " +childId+", position: " +position+ ", block:" +block);


				if(block.charAt(pos - 2) == ')'){ // Method not applied directly to a variable, we have to analyze the interior of the parenthesis
					cache = FindHelper.findCache(pos, block);
					System.out.println("Cache: " +cache+", p: " +p+ ", nodesParentsId: " +nodesParentsId.get(position));
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
							System.out.println("Combine method with variable: " +cache+", j: " +j+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", p: " +p+ ", nodesParentsId: " +nodesParentsId.get(position)+", position: "+position);
							generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
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
							System.out.println("Written combine method: " +subBlock+", childId: " +parentsList.get(position).getId()+", in p2: " +position+ ", nodesParentsId: " +nodesParentsId.get(position));
							List<Pair> pairL = new ArrayList<Pair>();
							pairL.add(new Pair(subBlock, posReal+start, posReal + start + subBlock.indexOf('.')));
							generate(pairL, parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));
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
					for (int b = 0; b < nodesParentsId.size() ; b++) {
						System.out.println("nodesParentsId: " +b+": "+nodesParentsId.get(b));
					}
					System.out.println("Changed position: " +changedPosition);
					System.out.println("Position: " +position);
					System.out.println("Applied directly to a variable: " +cache+", childId2: " +parentsList.get(position).getId());
					*/
					System.out.println("Applied directly to a variable: " +cache+", childId: " +parentsList.get(p).getId()+", p: " +p+", p2: " +position+ ", nodesParentsId: " +nodesParentsId.get(position)+", block: " +block+", position: " +position);
					System.out.println("Block to send: " +FindHelper.findBlock(cache, posReal, file).get(0).getBlock());
					if (CheckHelper.checkRDD(cache, listRDDs)) {
//						System.out.println("Let's recall, block: "+FindHelper.findBlock(cache, posReal, file));
						System.out.println("ChildId: " +nodesParentsId.get(position)+", position: " +position);
						generate(FindHelper.findBlock(cache, posReal, file), parentsList.get(position), nodesParentsId.get(position), check, nodesParentsType.get(position));					
					} else if(CheckHelper.checkSC(cache, listSC)) { // If the method is applied directly to an SC it means the stage finishes here so we have to add it to the stageList
//						System.out.println("Method applied to a SC, put in the stageList");
						int key = parentsList.get(position).getId();
						if(uniqueStage.containsKey(key))
							uniqueStage.get(key).addAll(parentsList.get(position));
						else 
							uniqueStage.put(parentsList.get(position).getId(), parentsList.get(position));
						prettyPrint(uniqueStage);
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
						generate(FindHelper.findBlock(rdd2, file.length(), file), child2, childId, check, nodesParentsType.get(position));					
					}				
				}
			}			
		}
	}
	
	public static void updateStages(){
		
		List<Stage> childrenStagesList = new ArrayList<Stage>();
		List<Integer> keys = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet())
			keys.add(entry.getKey());

		System.out.println("\n\n\n\n updateStages, " +keys);
//		prettyPrint(uniqueStage);

		// Find the stage that does not have any child wich will be the last stage, from there we will order the others ("generation" by "generation"), updating childrenId and parentsId
		for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet()){
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
			if(uniqueStage.containsKey(keys.get(i)))
				stage = uniqueStage.get(keys.get(i)); //Cache where i will put the stage
			else continue;
			
			Stage cache = null;
			if(uniqueStage.containsKey(stage.getId()))
				cache = uniqueStage.get(stage.getId());
			if(cache != null){
//				System.out.println("Chache: " +stage.getId());
	//			prettyPrint(cache);
			}
			if(keys.get(i) != stage.getId()){
//				System.out.println("Put in: " +stage.getId()+", remove in: " +keys.get(i));
				uniqueStage.put(stage.getId(), stage);
				uniqueStage.remove(keys.get(i));
			}
			int pos = -1;
			while(cache != null && cache.getId() != pos){
				Stage aux = cache;
				pos = aux.getId();

				cache = uniqueStage.get(pos);
//				System.out.println("Cache: ");
//				prettyPrint(cache);
//				System.out.println("Put in: " +pos);
				uniqueStage.put(pos, aux);
			}
		}

//		System.out.println("\n\n\n\n Stages ordered without parents");
//		prettyPrint(stagesList);

		for(Integer i : keys){
			if(i >= keys.size() + (nStages+1) && uniqueStage.containsKey(i)){
				System.out.println("\n\n\nremove: "+i+"\n\n\n");
				uniqueStage.remove(i);				
			}
		}
		
//		System.out.println("\n\n\n\n Stages ordered without parents");
//		prettyPrint(stagesList);
//		System.out.println("\n\n\n\n");

		// Once the stages have been ordered we set the parents ids of the stages (if a stage is my child, I am his father), removing the previous one
		setParentIdsOfStages();
	}
	
	private static void setParentIdsOfStages(){
		for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet()){
			entry.getValue().emptyParents();
		}

		for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet()){
			Stage stage = entry.getValue();
			List<Integer> childrenIds = stage.getChildrenId();
			System.out.println("Stage: "+stage.getId()+ ", children: " +childrenIds);
			for(Integer id: childrenIds){
//				System.out.println("Id of a child: " +id);
				Stage child = uniqueStage.get(id);
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
		
		for (Map.Entry<Integer, Stage> entry : uniqueStage.entrySet()){
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
		nodes--;
		stage = uniqueStage.get(0);
		List<Node> listRDDs = stage.getNodes();
		for(Node node: listRDDs){
			node.setId(nodes - node.getId());
				
			List<Integer> childrenIds = node.getChildrenId();
			node.removeChildrenIds();
			for(Integer idChild: childrenIds){
				node.addChildId(nodes - idChild);
			}
			List<Integer> parentsIds = node.getParentsId();
			node.removeParentsIds();
			for(Integer idParent: parentsIds){
				node.addParentId(nodes - idParent);
			}				
		}		
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
		return new Node(rdd, nodes++, new Partitioner(method, methodChild).getPartitioner(), NodeType.rdd);
	}
	
	public static Stage createStage(){
		return new Stage(stages);
	}
	
	public static void convertToMap(){
		
	}
}
