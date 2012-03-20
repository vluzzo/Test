package com.sirius.plugins;

import java.io.BufferedReader;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
//import org.olap4j.layout.RectangularCellSetFormatter;
import com.sirius.plugins.olap4j.layout.PGISRectangularCellSetFormatter;
import com.sirius.plugins.utils.OLAPUtils;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class PGISPlugin extends BaseStep implements StepInterface {

	private PGISPluginData data;
	private PGISPluginMeta meta;
	
	private String dbHost = "";
	private String dbName = "";
	private String dbUserName = "";
	private String dbPassword = "";
	
	Object[] r = null;
	
	public PGISPlugin(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s, stepDataInterface, c, t, dis);
	}
	
	String jsonOuterEnvelopeStartString="",jsonOuterEnvelopeEndString="",geojsonfield="",jsonInnerEnvelopeStartString="{ \n  \"type\": \"Feature\", \n \"properties\": {";
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		
		meta = (PGISPluginMeta) smi;
		data = (PGISPluginData) sdi;
		
		Connection connection = null;
		OlapConnection olapConnection = null;
		
		long startTime;
		long endTime;
		ArrayList<String> errors = new ArrayList<String>();
						
		try {
			r = getRow();
			
			if (r == null) {
				setOutputDone();
				return false;
			}
			
		    if (first) {
		        first = false;
		        data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
		    }
		    
		    // Create a HashMap with all the passed row data
		    HashMap<String,String> parameterMap = OLAPUtils.getParameterMap(getInputRowMeta(), r);
		    
		    // Get the cube list, we will need to create a query for each cube
		    ArrayList<String> cubeList = new ArrayList<String>();
		    StringTokenizer cubeST = new StringTokenizer(parameterMap.get("cubes"), ","); 
		    while (cubeST.hasMoreTokens()) {
		    	cubeList.add(cubeST.nextToken().trim());
		    }
		    
		    // Generate the MDX Query Data.  This is the middle portion of the query, we will add the cube parts as we iterate through the cubes
		    String mdxQueryData = OLAPUtils.generateMDXQueryData(parameterMap);
	    	
	    	// Create the Database Connections
		    connection = OLAPUtils.createDbConnection(dbHost, dbName, dbUserName, dbPassword, parameterMap.get("mondrianSchemaFilePath"));
		    olapConnection = (OlapConnection) connection.unwrap(OlapConnection.class);
		    
		    // Get the max number of results to return
		    long resultSize = Long.valueOf(parameterMap.get("Limit")).longValue();
		    
		    // Create the formatter for the query parsing
		    PGISRectangularCellSetFormatter formatter = new PGISRectangularCellSetFormatter(false);
		    formatter.setMaxResultsSize(resultSize);
	    		    	
	    	// Do the Query on each cube and generate an Array List with the results
		    OlapStatement statement = null;
	    	CellSet result = null;
	    	ArrayList<CellSet> CellSetList = new ArrayList<CellSet>();
		    
	    	Iterator<String> iter = cubeList.iterator();
	    	while (iter.hasNext()) {
	    		// Create the MDX query string
			    StringBuffer mdxQuery = new StringBuffer("SELECT");
			    mdxQuery.append("{[Measures].[Record Count]} ON COLUMNS, ");
			    mdxQuery.append(mdxQueryData);
			   // mdxQuery.append(" HAVING [Location-Start Latitude].currentMember > 1");
			    mdxQuery.append(" ON ROWS FROM [" + iter.next() + "]");
	    		
			    logBasic(mdxQuery.toString()); //TODO Remove Me
			    
			    // do the query
			    statement = olapConnection.createStatement();
			    startTime = System.currentTimeMillis(); //TODO remove me
		    	result =  statement.executeOlapQuery(mdxQuery.toString());
		    	endTime = System.currentTimeMillis(); //TODO remove me
				logBasic("statement.executeOlapQuery execution time: " + (endTime - startTime) + "ms"); //TODO remove me
			    
				// Check to see if we get any results and if so, add it to the cell list
				CellSetAxis tmpCellSetAxis = null;
				if (result.getAxes().size() > 1) {
					tmpCellSetAxis = (CellSetAxis) result.getAxes().get(1);
					resultSize = tmpCellSetAxis.getPositions().size();
					if (resultSize > 0) {
						CellSetList.add(result);
						formatter.setPreviousResultSize(formatter.getPreviousResultSize() + resultSize);
					}
				}
			    
				/*
			    if((formatter.getPreviousResultSize() > 0) && (formatter.getPreviousResultSize() >= formatter.getMaxResultsSize())) {
			    	startTime = System.currentTimeMillis(); //TODO remove me
			    	result =  statement.executeOlapQuery(mdxQuery.toString());
			    	endTime = System.currentTimeMillis(); //TODO remove me
					logBasic("statement.executeOlapQuery execution time: " + (endTime - startTime) + "ms"); //TODO remove me
					CellSetList.add(result);				        
				} else {
				//if(CellSetList.size() == 0) {
					startTime = System.currentTimeMillis(); //TODO remove me
					result =  statement.executeOlapQuery(mdxQuery.toString());
					endTime = System.currentTimeMillis(); //TODO remove me
					logBasic("statement.executeOlapQuery execution time: " + (endTime - startTime) + "ms"); //TODO remove me
					CellSetList.add(result);
				}
				
				// TODO Need to take a look at this, not sure what is being done here, plus the obvious null issue
				final CellSetAxis rowsAxis2;
				if (result.getAxes().size() > 1) {
					rowsAxis2 = (CellSetAxis)result.getAxes().get(1);
				} else {
					rowsAxis2 = null;
				}
				resultSize = rowsAxis2.getPositions().size();
				formatter.setPreviousResultSize(formatter.getPreviousResultSize() + resultSize);
				*/	                
	    	}
		    
	    	// Create the path for the CSV file
	    	Random generator = new Random();
	    	int randomNumber = generator.nextInt();
	    	String outputFilePath = parameterMap.get("outputFilePath") + randomNumber + ".csv";
	    	
	    	// Set the bounding polygon coords
	    	formatter.setPolygon(parameterMap.get("polygon"));
	    	
		    // Process the results
	    	startTime = System.currentTimeMillis(); //TODO remove me
			formatter.processResults(CellSetList, formatter, outputFilePath);
			endTime = System.currentTimeMillis(); //TODO remove me
			logBasic("formatter.processResults execution time: " + (endTime - startTime) + "ms"); //TODO remove me
			
			// Create the geojsonstring
			startTime = System.currentTimeMillis(); //TODO remove me
			jsonOuterEnvelopeStartString = "{ \n \t \"type\": \"FeatureCollection\", \n\t \"features\": [ {";
			String jsonMiddleContentString = developJsonPropertyMembersList(outputFilePath);
			jsonMiddleContentString = jsonMiddleContentString.substring(0, jsonMiddleContentString.length() - 1);
			jsonOuterEnvelopeEndString = " ] \n }";
			geojsonfield = jsonOuterEnvelopeStartString + jsonMiddleContentString + jsonOuterEnvelopeEndString;
			endTime = System.currentTimeMillis(); //TODO remove me
			logBasic("create JSON execution time: " + (endTime - startTime) + "ms"); //TODO remove me
			
			r[18] = geojsonfield;         
			r[12] = outputFilePath;

			//Object[] outputRowData = RowDataUtil.resizeArray(r, data.outputRowMeta.size());  // TODO What is this???

			putRow(data.outputRowMeta, r);
			
		} catch (OlapException e) {
			logError(e.getMessage());
			errors.add("OlapException");
			errors.add(e.getMessage());
		} catch (SQLException e) {
			logError(e.getMessage());
			errors.add("SQLException");
			errors.add(e.getMessage());
		} catch (KettleValueException e) {
			logError(e.getMessage());
			throw e;
		} catch (KettleException e) {
			logError(e.getMessage());
			throw e;
		} catch (Exception e) {
			logError(e.getMessage());
			errors.add("Generic Exception");
			errors.add(e.getMessage());
		} finally {
			try {
				if (olapConnection != null) {
					olapConnection.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				;
			}
		}
		
		// Handle Errors, send a JSON error string.  We can assume if we get here, no rows have already been written
		if (errors.size() > 0) {
			StringBuffer jsonString = new StringBuffer(""); 
			jsonString.append("{\"errors\": [");
			
			for (int i = 0;i < errors.size();i++) {
				if (i == errors.size() - 1) {
					jsonString.append("{ \"error\":\"" + errors.get(i) + "\"}");
				} else {
					jsonString.append("{ \"error\":\"" + errors.get(i) + "\"}, ");
				}
			}
			jsonString.append("]}");
			
			String json = jsonString.toString();
			
			// We need to hide the db info if the exception decides displays it
			json = json.replace(dbPassword, "xxxxx").replace(dbHost, "xxxxx").replace(dbUserName, "xxxxx").replace(dbName, "xxxxx");
			
			r[18] = json; // geojsonfield field
			r[12] = ""; // outputFilePath field
			
			putRow(data.outputRowMeta, r);
		}
		
		return true;
	}
	
	/*
	{
		"errors": [
		{ "error":"Error Description" },
		{ "error":"Error Description" }
		]
		}
*/

	/*
    private ArrayList processDimensionLists(ArrayList mergeDimensionsList,
		ArrayList cubeDisplayList) {
       String mergeDimensionLevelName="";
		Iterator mergeDimensionsListIterator=mergeDimensionsList.iterator();
		while(mergeDimensionsListIterator.hasNext()){
			mergeDimensionLevelName=mergeDimensionsListIterator.next().toString().trim();			
			if(cubeDisplayList.contains(mergeDimensionLevelName) ){
				cubeDisplayList.remove(mergeDimensionLevelName);
			}
		}
		return cubeDisplayList;
	}
	*/

	/*
	private ArrayList generateOnnRowStringList(ArrayList membersList) {
		ArrayList rowMembersList=new ArrayList();
		String memberToken="";
		String membersListString=membersList.toString().trim();
		membersListString=membersListString.substring(1, membersListString.length()-1);
		StringTokenizer rowsTokenizer=new StringTokenizer(membersListString,","); 
		while(rowsTokenizer.hasMoreTokens()){
			memberToken=rowsTokenizer.nextToken().trim();
			if(memberToken.startsWith("[") &&  !(memberToken.endsWith("]")) && !(memberToken.endsWith("Members"))){
				memberToken=memberToken+","+rowsTokenizer.nextToken();
			}
			rowMembersList.add(memberToken);
		}
		return rowMembersList;
	  }
	  */

  
	/*
     private String formRowString(HashMap rowAggregatedMap) {
		ArrayList rowMembersList=new ArrayList();
		HashMap rowMembersResultsMap=new HashMap();
		HashMap tmpMemberResultsMap=new HashMap();
		Set rowMemberResultsSet=null;
		Iterator rowMembersSetIterator=null;
		boolean compareFlag=false,memberExists=false;
		int topLevelcount=0;
		String dimensionLikeString="",localMember="";
		
		rowMemberResultsSet= rowAggregatedMap.keySet();
		rowMembersSetIterator=rowMemberResultsSet.iterator();
		while(rowMembersSetIterator.hasNext()){
			localMember=rowMembersSetIterator.next().toString().trim();
			localMember=rowAggregatedMap.get(localMember).toString().trim();
			localMember=" { "+localMember+" }";
			if(compareFlag == true){
			dimensionLikeString=" nonemptycrossjoin ( "+dimensionLikeString+" , "+localMember+" ) ";
			}
			else {
				dimensionLikeString=localMember;
				compareFlag=true;
			}			
		}
		dimensionLikeString=dimensionLikeString.trim();
		//dimensionLikeString=dimensionLikeString.substring(0, dimensionLikeString.length()-2);
		return dimensionLikeString;
		
	}
	*/

     /*
	 * This method is responsible of returning the "on rows" string for the final MDX query
	 * 
	 */
	/*
	private HashMap generateOnRowString(ArrayList membersList) {
		ArrayList rowMembersList=new ArrayList();
		HashMap rowMembersResultsMap=new HashMap();
		HashMap tmpMemberResultsMap=new HashMap();
		Set rowMemberResultsSet=null;
		Iterator rowStringListIterator=membersList.iterator();
		Iterator rowMembersSetIterator=null;
		boolean compareFlag=false,memberExists=false;
		int topLevelcount=0;
		String membersListString="",rowStringMember="",rowMemberList="";
		while(rowStringListIterator.hasNext()){
			memberExists=false;
			membersListString=rowStringListIterator.next().toString().trim();
			if(topLevelcount==0){
				rowMembersResultsMap.put(membersListString,membersListString);
				topLevelcount++;
			}
			else{
				tmpMemberResultsMap=(HashMap) rowMembersResultsMap.clone();
					rowMemberResultsSet= tmpMemberResultsMap.keySet();
					rowMembersSetIterator=rowMemberResultsSet.iterator();
					while(rowMembersSetIterator.hasNext()){
						rowStringMember=rowMembersSetIterator.next().toString().trim();
						compareFlag=checkSameHierarchy(rowStringMember,membersListString);
						if(compareFlag){
							rowMemberList=rowMembersResultsMap.get(rowStringMember).toString().trim();
							rowMemberList=rowMemberList+","+membersListString;
							rowMembersResultsMap.remove(rowStringMember);
							rowMembersResultsMap.put(rowStringMember, rowMemberList);
							memberExists=true;
							break;
						}						
					}	
					if(! memberExists){
						rowMembersResultsMap.put(membersListString,membersListString);
						memberExists=false;
					}
				}			
			}
		rowMemberResultsSet= rowMembersResultsMap.keySet();
		rowMembersSetIterator=rowMemberResultsSet.iterator();
		while(rowMembersSetIterator.hasNext()){
			rowStringMember=rowMembersSetIterator.next().toString().trim();
			membersList.add(rowMembersResultsMap.get(rowStringMember).toString().trim());
		}
			return rowMembersResultsMap;		
	}
	*/


    /*
	 * Updates the headers of the csv/json outputs with the new columns names for specific cube /static columns list names for all cubes.
	 */
	/*
	private ArrayList UpdateHeadersListWithNewColumns(ArrayList membersList,
			ArrayList CubeDisplayList) {
		String listMember="";
		Iterator listIterator=CubeDisplayList.iterator();
		while(listIterator.hasNext()){
			listMember=listIterator.next().toString().trim();
			membersList.add(listMember);
		}
		return membersList;
	}
	/*

	/*
	 * This method adds additional columns to the rowDimensionString which is the query formed out of PODQuery of the filters list.
	 */
	/*
	private String addAdditionalColumnsToOnRowsString(
			String rowdimensionString, ArrayList CubeDisplayList) {
		String crossJoinStartString=" crossjoin( ",listMember="";
		Iterator listIterator=CubeDisplayList.iterator();
		while(listIterator.hasNext()){
			listMember=listIterator.next().toString().trim();
			rowdimensionString= crossJoinStartString+ rowdimensionString+",{"+listMember+"})";
		}
		return rowdimensionString;
	}
	*/

    /*
	 * Form a header string for csv file
	 */
	/*
	private String formHeaders(String membersList) {
		String headers="",headerList="",headerMembers="",headerLevel="";
		ArrayList headerSet=new ArrayList();
	    headerList=membersList;
	    headerList=headerList.replace("[[", "[");
	    headerList=headerList.replace("]]", "]");
	    StringTokenizer headerTokenizer=new StringTokenizer(headerList, ",");
		while(headerTokenizer.hasMoreElements()){
			headerMembers=headerTokenizer.nextToken();	
			StringTokenizer headerListTokenizer=new StringTokenizer(headerMembers, "]");
			headers=headerListTokenizer.nextToken();
			headers=headers.replace("[", " ").trim();
			if(headers.contains("Measures")){
				headers=headerListTokenizer.nextToken();
				headers=headers.replace("[", " ").trim();
				headers=headers.length()>0 ?headers.substring(1): headers;
			}
			
			headers=headers.trim();
			headers=headers;
			int headerLevelCount=0;
			//if the member is multiple level after 1st level add modified header names
			if( headerSet.contains(headers))
			{
				StringTokenizer headerMultipleLevelTokenizer=new StringTokenizer(headerMembers, "]");
				headerLevelCount=headerMultipleLevelTokenizer.countTokens();
				//make sure we are looking at members which are more than 1 level to be added as an extra
				//column to header string
				int loopCount=0;
				int headerLevelAppend=1;
				loopCount=headerLevelCount-2;
				if(headerLevelCount > 2 && loopCount >0){					
					headerLevel=headerMultipleLevelTokenizer.nextToken();
					headerLevelAppend++;
					loopCount--;
					headers=headerLevel+"- level"+headerLevelAppend;
					headers=headers.replace("[", " ").trim();
					if( ! headerSet.contains(headers)){
						headerSet.add(headers);
					}
				}
			}
			//if the header is just one level of member then add the plain member name
			else{
				headerSet.add(headers);
			}
		}
		headerList=headerSet.toString();
		headerList=headerList.substring(1, headerList.length()-1);
		return headerList;
	}
	/*

    /*
	 * This method is responsible of returning the "on rows" string for the final MDX query
	 * 
	 */
	/*
	private String generateRowString(ArrayList membersList) {
		Iterator mainIterator=null,tmpResultiterator=null;
		mainIterator=membersList.iterator(); 
		ArrayList tmpMembersList=(ArrayList) membersList.clone();
		ArrayList tmpesultMembersList=new ArrayList();
		ArrayList tmpAggregatedMembersList=new ArrayList();
		HashMap hashmap=new HashMap();
		int listCount=membersList.size();
		int topLevelcount=0;
		boolean compareFlag=false;
		String tmpMember="",tmpListMember="",dimensionLikeString="",mergedStrings="",aggregatedStrings="",localMember="";
		while(mainIterator.hasNext()){
			tmpMember=mainIterator.next().toString();			
			listCount=tmpMembersList.size();
			for(int i=0;i<listCount-1;i++){
				tmpListMember=tmpMembersList.get(i).toString();
				if(topLevelcount==i) continue;
				//check to manage if different PODS contains members of the same dimension and shuffle accordingly
				compareFlag=checkSameHierarchy(tmpListMember,tmpMember);
				if(compareFlag){
					mergedStrings=mergeStrings(tmpListMember,tmpMember);
					tmpMembersList.remove(i);
					if(tmpMembersList.size()>0 && tmpMembersList.contains(tmpListMember)){
						tmpMembersList.remove(tmpListMember);
					}
					
					 // remove the entries in the result list tmpesultMembersList which are present in aggegated list.
					 // Aggregated strings is the formed to combine the contents of two different PODS into a single POD
					 // content like structure, something like a master string of a specific POD type.
					 
					tmpResultiterator=tmpesultMembersList.iterator();
					while(tmpResultiterator.hasNext()){
						aggregatedStrings=tmpResultiterator.next().toString();
						if(aggregatedStrings.contains(mergedStrings)){
							mergedStrings=mergeStrings(aggregatedStrings,mergedStrings);
							tmpesultMembersList.remove(aggregatedStrings);
							break;
						}
					}
					tmpesultMembersList.add(mergedStrings);
				}
			}
			topLevelcount++;
		}
		//removing entries in uniquearraylist which are present in tmperesultMembersList
		ArrayList uniqueArrayList=(ArrayList) tmpMembersList.clone();
		tmpResultiterator=tmpMembersList.iterator();
		aggregatedStrings=tmpesultMembersList.toString();
		while(tmpResultiterator.hasNext()){
			localMember=tmpResultiterator.next().toString();
			tmpMember=localMember.replace("[[", "[");
			tmpMember=tmpMember.replace("]]", "]");
			if(aggregatedStrings.contains(tmpMember)){
				uniqueArrayList.remove(localMember);
			}
		}
		
		
		 // filters tmpesultMembersList for any final entries while contatinating two different strings belonging to 
		 // the same dimension.
		
		for(int i=0;i<tmpesultMembersList.size();i++){
			localMember=tmpesultMembersList.get(i).toString();
			if(i>0){
				compareFlag=checkSameHierarchy(aggregatedStrings,localMember);
				if(compareFlag){
					aggregatedStrings=mergeStrings(aggregatedStrings,localMember);
					tmpAggregatedMembersList.add(aggregatedStrings);
					tmpesultMembersList.remove(i);
					i=0;
				}
				else{
					tmpAggregatedMembersList.add(localMember);
				}
			}
			else {
				aggregatedStrings=localMember;
			}
		}
		tmpMembersList.addAll(tmpAggregatedMembersList);
		
		topLevelcount=tmpMembersList.size();
		for(int i=0;i<topLevelcount;i++){
			localMember=tmpMembersList.get(i).toString();
			localMember=localMember.replace("[[", "[");
			localMember=localMember.replace("]]", "]");
			localMember=" { "+localMember+" } ";
			if(i>0){
			dimensionLikeString="crossjoin ( "+dimensionLikeString+" , "+localMember+" ) ";
			}
			else {
				dimensionLikeString=localMember;
			}
		}
		dimensionLikeString=dimensionLikeString.trim();
		dimensionLikeString.substring(0, dimensionLikeString.length()-2);
		aggregatedStrings=dimensionLikeString;
		return aggregatedStrings;
	}
	/*

/*
 *  Merges two strings in the pattern of tmpListMember,tmpMember removing extra braces if any
 * 
 */

	/*
	private String mergeStrings(String tmpListMember, String tmpMember) {
		tmpListMember=tmpListMember.replace("[[", "[");
		tmpListMember=tmpListMember.replace("]]", "]");
		tmpMember=tmpMember.replace("[[", "[");
		tmpMember=tmpMember.replace("]]", "]");
		return tmpListMember + ","+tmpMember;
	}
	*/

/*
 *  This Method checks for different POD's belonging to same Dimension and returns
 *  true : if they both are from the same dimension
 *  falso: if they both are from different dimensions
 *  Parameters : 1st parameter : string in which we need to search for similarity (preferably larger string of the two)
 *               2nd parameter : string which needs to checked for similarity (Preferably smaller string of the two)
 */

	/*
	private boolean checkSameHierarchy(String tmpRowDimensionString,
			String CurrentToken) {
		String tokenCheck="";
		StringTokenizer parentTokenizer=new StringTokenizer(CurrentToken,",");
		String parentToken= parentTokenizer.nextToken().toString().trim();
		parentTokenizer=new StringTokenizer(parentToken,"]");
		parentToken= parentTokenizer.nextToken().toString().trim();
		parentToken=parentToken+"]";
		if(tmpRowDimensionString.contains(parentToken)){
			return true;
		}
		else{
			return false;
		}
	}
	*/

private String developJsonPropertyMembersList(String outputFilePath) {
		int row = 0;
		int col = 0;
		String line = "",token="",memberValue="",memberName="",memberString=" \"type\": \"Feature\", \n \"properties\": { ";
		ArrayList headerList=new ArrayList();
      //   logRowlevel("method output---hhhhhhhhhhhhhh--------------------");
		try {		
		  File file = new File(outputFilePath);
		   PGISRectangularCellSetFormatter formatter =  new PGISRectangularCellSetFormatter(false);
		  BufferedReader bufRdr= formatter.createBufferedReader(file);
 while((line = bufRdr.readLine()) != null)
		{
         if(line.isEmpty() || line.trim().equals("")) {
		  continue;
		}
		StringTokenizer st = new StringTokenizer(line,",");
		while (st.hasMoreTokens())
		{
		//get next token and store it in the array
			token=st.nextToken().toString().trim();
			if(token.startsWith("\"") && ! token.endsWith("\"")){
				token=token+","+st.nextToken().toString().trim();
			}
			if(row==0){
				headerList.add(token);
			}
			else{
				if(col == headerList.size()) continue;
				memberName=headerList.get(col)==null?"" :headerList.get(col).toString().trim();
				memberValue=memberName+":"+token+", ";
				memberString=memberString+memberValue;			
				
			}
			col++;
			}
		if(row!=0){
			memberString=memberString.substring(0,memberString.length()-2);
			if(memberString.contains("\"\"")){
				memberString=memberString.replace("\"\"", "\\\"");
			}
			memberString=memberString+"}";
			//memberString=memberString+"\", \n   geometry\": {\"type\":\"Point\",\"coordinates\":[-9628237.605327462777495,3605053.034072482958436]},";
			memberString=memberString+" \n },";
			memberString=memberString+jsonInnerEnvelopeStartString;
		}
		row++;
		col=0;
		}
		 
		//close the file
		bufRdr.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		memberString=memberString.substring(0,(memberString.length()-jsonInnerEnvelopeStartString.length()));
		return memberString;

	}

	
	
	
	
	
	
	
	
		
		
		
		
		
		

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (PGISPluginMeta) smi;
		data = (PGISPluginData) sdi;
		
		try {
			Map <?,?> kettleProperties = EnvUtil.readProperties(Const.KETTLE_PROPERTIES);
			dbHost = (String) kettleProperties.get("GISPLUGIN_DATABASE_SERVER");
			dbName = (String) kettleProperties.get("GISPLUGIN_DATABASE_NAME");
			dbUserName = (String) kettleProperties.get("GISPLUGIN_DATABASE_USER");
			dbPassword = (String) kettleProperties.get("GISPLUGIN_DATABASE_PASSWORD");
		} catch (KettleException e) {
			logError(e.getMessage());
		}
		
		return super.init(smi, sdi);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (PGISPluginMeta) smi;
		data = (PGISPluginData) sdi;

		super.dispose(smi, sdi);
	}

	//
	// Run is were the action happens!
	public void run() {
		logBasic("Starting to run...");
		try {
			while (processRow(meta, data) && !isStopped())
				;
		} catch (Exception e) {
			logError("Unexpected error : " + e.toString());
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		} finally {
			dispose(meta, data);
			logBasic("Finished, processing " + getLinesRead() + " rows");
			markStop();
		}
	}

}


/*
logError("enter processRow");
logBasic("enter processRow");

meta = (PGISPluginMeta) smi;
data = (PGISPluginData) sdi;

Object[] r = getRow(); // get row, blocks when needed!
if (r == null) // no more input to be expected...
{
	setOutputDone();
	return false;
}

if (first) {
	first = false;

	data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
	meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

	logBasic("template step initialized successfully");

}

Object[] outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, "dummy value");

putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)

if (checkFeedback(getLinesRead())) {
	logBasic("Linenr " + getLinesRead()); // Some basic logging
}

return true; */

/*
// Get all the POD data from the input rows (4 possible PODS, POD0-POD3)
HashMap<String,String> podMap = new HashMap<String, String>();
String pod = "";
for (int i=1;i<9;i=i+2) {
	pod = getInputRowMeta().getString(r, i).trim();
	if (pod.length() != 0) {
		podMap.put(pod, getInputRowMeta().getString(r, i+1).trim());
	}
}

// Do additional Fields
// If any of the additional fields already exist as part of the POD query we do not add the generic
StringTokenizer afST = new StringTokenizer(getInputRowMeta().getString(r, 9), ","); 
while (afST.hasMoreTokens()) {
	String key = afST.nextToken().trim();
	if (!podMap.containsKey(key)) {
		podMap.put(key, key + ".Members");
	}
}

// Do display by field
// If the display by field already exists as part of the POD query we do not add the generic
String displayByFields = getInputRowMeta().getString(r, 10).trim();
if (displayByFields.length() != 0) {
	if (!podMap.containsKey(displayByFields)) {
		podMap.put(displayByFields, displayByFields + ".Members");
	}
}

// Do the defaults and merge the pod values
Iterator<String> iter = null;
ArrayList<String> mdxColumns = new ArrayList<String>();
mdxColumns.add("[legend_text].[legend_text].Members");
mdxColumns.add("[nrda_featurestyle].[nrda_featurestyle].Members");
mdxColumns.add("[Source].[Source].Members");
mdxColumns.add("[LocationGeom].[Location_Geom].Members");
iter = podMap.values().iterator();
while (iter.hasNext()) {
	mdxColumns.add(iter.next());
}

// Generate the column data part of the MDX query		    
StringBuffer mdxPodData = new StringBuffer("");
iter = mdxColumns.iterator();
while (iter.hasNext()) {
	// First one is special since we don't know if we have anything to join yet
	if (mdxPodData.length() == 0) {
		mdxPodData.append("{ " + iter.next() + " }");
	} else {
		mdxPodData.insert(0, "nonemptycrossjoin (");
		mdxPodData.append(", { " + iter.next() + " })");
	}
}	

*/

// Iterate through the cubes and get the results
//long resultSize2 = getInputRowMeta().getInteger(r, 11);
//PGISRectangularCellSetFormatter formatter2 =  new PGISRectangularCellSetFormatter(false);
//formatter2.setMaxResultsSize(resultSize2);
//OlapStatement statement2=null;
//CellSet result2=null;
//String mondrianSchemaFilePath2 = getInputRowMeta().getString(r, 19);
//ArrayList<CellSet> CellSetList2 = new ArrayList<CellSet>();

//String dbConnectionUrlString2 = "Jdbc=jdbc:postgresql://"+dbHost+"/"+dbName+";JdbcUser="+dbUserName+";JdbcPassword="+dbPassword+";";
//final Connection connection2 = DriverManager.getConnection("jdbc:mondrian:" + "JdbcDrivers=org.postgresql.Driver;" + dbConnectionUrlString2 + "Catalog=file:" + mondrianSchemaFilePath2 + ";");
//final OlapConnection olapConnection2 = (OlapConnection)connection2.unwrap(OlapConnection.class);
/*
Iterator iter = cubeList.iterator();
while (iter.hasNext()) {
	 // Create the MDX query string
    StringBuffer mdxQuery1 = new StringBuffer("SELECT");
    mdxQuery1.append("{[Measures].[Record Count]} ON COLUMNS, ");
    mdxQuery1.append(mdxQueryData);
    mdxQuery1.append(" ON ROWS FROM [" + iter.next() + "]");
	
    statement2 = olapConnection2.createStatement();
    if((formatter2.getPreviousResultSize() > 0) && (formatter2.getPreviousResultSize()>=formatter2.getMaxResultsSize())){
		result2 =  statement2.executeOlapQuery(mdxQuery1.toString());
		CellSetList2.add(result2);				        
	}
	if(CellSetList2.size()==0){
		result2 =  statement2.executeOlapQuery(mdxQuery1.toString());
		CellSetList2.add(result2);
	}
	
	final CellSetAxis rowsAxis2;
	if (result2.getAxes().size() > 1) {
		rowsAxis2 = (CellSetAxis)result2.getAxes().get(1);
	} else {
		rowsAxis2 = null;
	}
	resultSize2=rowsAxis2.getPositions().size();
	formatter2.setPreviousResultSize( formatter2.getPreviousResultSize()+resultSize2);	                
}
*/















		    
//pod0Level = [Analysis-Status].[Analysis-Status]
//pod0Members = [Analysis-Status].[Lab Analysis Complete], [Analysis-Status].[Pending]
//pod1Level = [Method-Collection].[Method-Collection]
//pod1Members = [Method-Collection].[Core], [Method-Collection].[Dip Net], [Method-Collection].[Epipelagic Trawl]
//pod2Level = [Time.Time: 1.Day or Night Sample].[Day or Night Sample]
//pod2Members = [Time.Time: 1.Day or Night Sample].[Day]

//SELECT  { [Measures].[Record Count] } ON COLUMNS, nonemptycrossjoin (  nonemptycrossjoin (  nonemptycrossjoin (  nonemptycrossjoin (  nonemptycrossjoin (  nonemptycrossjoin 
//	    	(  nonemptycrossjoin (
//{ [legend_text].[legend_text].Members } ,  { [Analysis-Lab/Result Matrix].[Analysis-Lab/Result Matrix].Members } )  ,  { [Time.Time: 1.Day or Night Sample].[Day] } )  ,  { [Method-Collection].[Core],[Method-Collection].[Dip Net],[Method-Collection].[Epipelagic Trawl] } )  ,  { [Analysis-Status].[Lab Analysis Complete],[Analysis-Status].[Pending] } )  ,  { [nrda_featurestyle].[nrda_featurestyle].Members } )  ,  { [LocationGeom].[Location_Geom].Members } )  ,  { [Source].[Source].Members } ) ON ROWS FROM [Samples]

// nonemptycrossjoin (nonemptycrossjoin (nonemptycrossjoin (nonemptycrossjoin (nonemptycrossjoin (nonemptycrossjoin (nonemptycrossjoin ({ [legend_text].[legend_text].Members }, { [nrda_featurestyle].[nrda_featurestyle].Members }), { [Source].[Source].Members }), { [LocationGeom].[Location_Geom].Members }), { [Method-Collection].[Core], [Method-Collection].[Dip Net], [Method-Collection].[Epipelagic Trawl] }), { [Analysis-Lab/Result Matrix].[Analysis-Lab/Result Matrix].Members }), { [Time.Time: 1.Day or Night Sample].[Day] }), { [Analysis-Status].[Lab Analysis Complete], [Analysis-Status].[Pending] })





/*

String queryString= getInputRowMeta().getString(r, 14);
String displayByField=getInputRowMeta().getString(r, 10);
String additionalFields=getInputRowMeta().getString(r, 9);
String PolygonString=getInputRowMeta().getString(r, 15);
//long resultSize=get(Fields.In, "LIMIT").getInteger(r);
long resultSize=getInputRowMeta().getInteger(r, 11);
String cubeName="",podDimensions="",topToken="",rowdimensionString="",columnDimensionString="";

//queryString="POD0{Collection Form Type} [[Method - 5.Collection Form Type].[Scribe], [Method - 5.Collection Form Type].[Field Sample Form]] @ POD2{Collection Workplan} [[Workgroup].[Chemistry/Sampling].[Forensic Oil], [Workgroup].[Chemistry/Sampling].[No Formal Workplan/TBD], [Workgroup].[Chemistry/Sampling].[Not Defined], [Workgroup].[Chemistry/Sampling].[Source Oil/Product Sampling], [Workgroup].[Deep Water Benthic].[Deep Benthic Camera Trap], [Workgroup].[Deep Water Benthic].[Hardbottom Plan], [Workgroup].[Deep Water Benthic].[Mesophotic Reef Plan], [Workgroup].[Deep Water Benthic].[Natural Hydrocarbon Seeps Plan]] @ CUBES [Samples]";
//queryString = "POD0{Analysis-Lab Rep} [[Analysis-Lab Rep].[1AX]] @ CUBES [Samples]";
//POD0{Analysis-Lab Name} [[Analysis-Lab Name].[CBL]] @ CUBES [Samples]
//String additionalFields="[Workgroup].[Workgroup],[Workgroup].[Collection Study Name],[Workgroup].[Collection Workplan]";

logRowlevel("podquery------------------------------------------------------->r value "+queryString);

String dbConnectionUrlString="Jdbc=jdbc:postgresql://"+dbHost+"/"+dbName+";JdbcUser="+dbUsername+";JdbcPassword="+dbPassword+";";
Random generator = new Random();
int randomNumber = generator.nextInt();
String cube="obs_filecollection";
String outputFilePath = getInputRowMeta().getString(r, 12);
outputFilePath=outputFilePath+randomNumber+".csv";       
Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
String mondrianSchemaFilePath = getInputRowMeta().getString(r, 19);
	
final Connection connection = DriverManager.getConnection("jdbc:mondrian:" + "JdbcDrivers=org.postgresql.Driver;" + dbConnectionUrlString + "Catalog=file:" + mondrianSchemaFilePath + ";"); 
// Relational driver
//+ "Jdbc=jdbc:postgresql://107.22.185.152/noaanrdaDW-Sprint2;JdbcUser=postgres;JdbcPassword=p0stgr3s;"   // Relational DB
//"jdbc:mondrian:"                                                            // Driver ident
//+ "JdbcDrivers=org.postgresql.Driver;"                                      // Relational driver
//+ "Jdbc=jdbc:postgresql://"+dbHost+"/"+dbName+";JdbcUser="+dbUsername+";JdbcPassword="+dbPassword+";"   // Relational DB
// +"Jdbc=jdbc:postgresql://107.22.185.152/noaanrdaDW-Sprint2;JdbcUser=postgres;JdbcPassword=p0stgr3s";
//+ "Catalog=file:"+mondrianSchemaFilePath+";"); 
	
// We are dealing with an olap connection. we must unwrap it.
// We are dealing with an olap connection. we must unwrap it.  107.22.185.152
final OlapConnection olapConnection = (OlapConnection)connection.unwrap(OlapConnection.class);
	       
// String queryString="CUBES [Samples,Visual Observations]";
//queryString="POD0{Contaminant Sample Status} [[Sample Status: 1.Contaminant Chemistry].[In Analysis Queue],[Sample Status: 1.Contaminant Chemistry].[Archived]] @ POD3{Lab Name} [[Analysis: 1.Lab Name].[Alpha Analytical]] @ CUBES [Samples]";
//   String additionalFields="[Analysis: 2.Lab/Result Matrix].[Lab/Result Matrix]";
//queryString="POD0{Analysis-Status} [[Analysis-Status].[Lab Analysis Complete], [Analysis-Status].[Pending]] @ POD1{Method-Collection} [[Method-Collection].[Core], [Method-Collection].[Dip Net], [Method-Collection].[Epipelagic Trawl]] @ POD2{Day or Night Sample} [[Time.Time: 1.Day or Night Sample].[Day]] @ CUBES [Samples]";
	         
StringTokenizer st=new StringTokenizer(queryString, "@");
Cube NRDACube=null;
cubeName="Visual Observations";		        
String levelName="", tmpCubeString="",cubeNamesList="";
ArrayList<String> membersList= new ArrayList<String>();
ArrayList<String> SamplesCubeDisplayList=new ArrayList<String>();
ArrayList<String> commonColumnsList=new ArrayList<String>();
ArrayList<String> CubeDisplayList=new ArrayList<String>();    //dynamic cube which contains the list 
ArrayList<String> MergeDimensionsList=new ArrayList<String>();
ArrayList<CellSet> CellSetList =new ArrayList<CellSet>();
List measureList=null;
CellSet result=null;
String dimensionLevelHierarchy="",dataLevelStructure="",dimensionName="",dimensionLevelName="";
int podIterator=1;
while(st.hasMoreElements()){
	topToken=st.nextToken().trim();
	if(topToken.startsWith("CUBES")){
		cubeNamesList=topToken.substring(7,topToken.length()-1);			        	
	}
}
	        
PGISRectangularCellSetFormatter formatter =  new PGISRectangularCellSetFormatter(false);
formatter.setMaxResultsSize(resultSize);
OlapStatement statement=null;
StringTokenizer cubesTokenizer=new StringTokenizer(cubeNamesList,",");
while(cubesTokenizer.hasMoreTokens()){
	membersList.clear();
	SamplesCubeDisplayList.clear();
	commonColumnsList.clear();
	CubeDisplayList.clear();
	MergeDimensionsList.clear();
	
	//  measureList.clear();
	result=null;
	cubeName=cubesTokenizer.nextToken().trim();
	st=new StringTokenizer(queryString, "@");
		while(st.hasMoreElements()){
		topToken=st.nextToken().trim();
		if(topToken.startsWith("POD")){
			podDimensions=topToken.substring(5, topToken.length()).toString();
			StringTokenizer rowsTokenizer=new StringTokenizer(podDimensions,"}"); 
			levelName="["+rowsTokenizer.nextToken().trim()+"]";
			//dimensionLevelHierarchy
			dataLevelStructure=rowsTokenizer.nextToken().trim();
			dataLevelStructure=dataLevelStructure.replace("[[", "[");
			StringTokenizer dimensionLevelTokenizer=new StringTokenizer(dataLevelStructure,"]"); 
			dimensionName=dimensionLevelTokenizer.nextToken().trim()+"]";
			dimensionLevelName=dimensionName+"."+levelName+".Members";
			MergeDimensionsList.add(dimensionLevelName); // populating the dimension-level based hierarchy to compare with displayfields/common fields list
			dataLevelStructure=dataLevelStructure.replace("]]", "]");
			membersList.add(dataLevelStructure);
		}
		
	}
//POD0{Analysis-Status} [[Analysis-Status].[Lab Analysis Complete], [Analysis-Status].[Pending]] @ POD1{Method-Collection} [[Method-Collection].[Core], [Method-Collection].[Dip Net], [Method-Collection].[Epipelagic Trawl]] @ POD2{Day or Night Sample} [[Time.Time: 1.Day or Night Sample].[Day]] @ CUBES [Samples]				        

		//  Get a cube object.
	NRDACube = 
			(Cube)olapConnection
			.getOlapSchema()
			.getCubes()
			.get(cubeName);
	        	
	// Adding common columns of all the cubes into a list -- *** needs to be implemented generically. hard coded just for testing purpose
	//if(cubeName.equals("Samples") && displayByField.equals("[Analysis-Lab/Result Matrix].[Analysis-Lab/Result Matrix]")){
	//	commonColumnsList.add("[legend_text].[legend_text].Members");
	//	commonColumnsList.add("[nrda_featurestyle].[nrda_featurestyle].Members");
	//	commonColumnsList.add("[Analysis-Lab/Result Matrix].[Analysis-Lab/Result Matrix].Members");
	//	                       [Analysis-Lab/Result Matrix].[Analysis-Lab/Result Matrix]
	//}
	if (cubeName.equals("Samples")) {
		commonColumnsList.add("[legend_text].[legend_text].Members");
		commonColumnsList.add("[nrda_featurestyle].[nrda_featurestyle].Members");
	}
	if (displayByField.length() != 0) {
		commonColumnsList.add(displayByField + ".Members");
	}				
	
	commonColumnsList.add("[Source].[Source].Members");
	commonColumnsList.add("[LocationGeom].[Location_Geom].Members");

	//Adding the common columns across all cubes
	CubeDisplayList.addAll(commonColumnsList); 
	
	//Adding the common columns and specific columns of the cube to the specific cubeList, this should change after the filter and columns logic is implemented
	StringTokenizer additionalColumnsTokenizer=new StringTokenizer(additionalFields,",");
	while(additionalColumnsTokenizer.hasMoreTokens()){
		SamplesCubeDisplayList.add(additionalColumnsTokenizer.nextToken().trim()+".Members");
	}
	        
	//SamplesCubeDisplayList.add("[Analysis-Lab Name].[Analysis-Lab Name].Members");
	
	CubeDisplayList.addAll(SamplesCubeDisplayList); // need to filter this list from the filter list
	      
	// remaining cubes specific list to be added here - *** only after filtering it from the filter list
	
	// The below method call filters the CubeDisplayList with any dimensions present in the MergeDimensionsList
	CubeDisplayList=processDimensionLists(MergeDimensionsList,CubeDisplayList);
	    
	//the below call gets the mdx query format for on rows by passing filters list		    
	// membersList=generateRowString(membersList);
	membersList.addAll(CubeDisplayList);
	// membersList.addAll(CubeDisplayList);
	membersList=generateOnnRowStringList(membersList);
	HashMap rowAggregatedMap=generateOnRowString(membersList);
	//membersList=generateRowString(membersList);
	rowdimensionString=formRowString(rowAggregatedMap);
	        
	//  CubeDisplayList=processDimensionLists(membersList,CubeDisplayList);
	//the below call adds additional cube specific/general list of columns which are not part of the dimension/level in the filters list.
	//   rowdimensionString=addAdditionalColumnsToOnRowsString(rowdimensionString,CubeDisplayList);
	 //Process the POD's to form columns of result
	// fetch the first measure available in the cube (This is currently implemented as no measures are passed in PODS)
	// measureList=NRDACube.getMeasures();
	        
	// mondrian.olap4j.MondrianOlap4jMeasure MondrianOlap4jMeasure=(mondrian.olap4j.MondrianOlap4jMeasure) measureList.get(0);
	//columnDimensionString=measureList.get(0).toString();// + ", "+sampleCubeColumnString;
	columnDimensionString=" [Measures].[Record Count] ";
	// rowdimensionString="crossjoin( crossjoin( crossjoin ( crossjoin("+rowdimensionString+",{[legend_text].[legendText].Members} ), {[nrda_featurestyle].[NrdaStyle].Members}), {[Source].[Source].members}),{[LocationGeom].[Location_Geom].Members})";
	//rowdimensionString=" NON EMPTY crossjoin("+rowdimensionString+",{[LocationGeom].[Location_Geom].Members})";
	mdxQuery="SELECT  {"+columnDimensionString+"} ON COLUMNS, "+rowdimensionString+" ON ROWS FROM ["+cubeName+"] " ;
	logBasic(mdxQuery);
	// queryString=" SELECT  { [Measures].[Record Count] } ON COLUMNS,  crossjoin(nonemptycrossjoin (  { [Source].[Source].members } ,  { [Analysis: 5.Status].[Provisional Results Available] } ),{[LocationGeom].[Location_Geom].Members}) ON ROWS FROM [Samples]";
	try {                      
		statement = olapConnection.createStatement();
		if((formatter.getPreviousResultSize() > 0) && (formatter.getPreviousResultSize()>=formatter.getMaxResultsSize())){
			result =  statement.executeOlapQuery(mdxQuery);
			CellSetList.add(result);				        
		}
		if(CellSetList.size()==0){
			result =  statement.executeOlapQuery(mdxQuery);
			CellSetList.add(result);
		}
	}
	catch(Throwable genericException){  // catching all sort of exception, need to catch only specific types ones 
		System.out.println(genericException.getMessage());                               // developing the list to include : MondrianException,OlapException,		    	 
		logError(genericException.getMessage());
		//performing a minimal logging for errors
		//   genericException.printStackTrace();
	}
	final CellSetAxis rowsAxis;
	if (result.getAxes().size() > 1) {
		rowsAxis = (CellSetAxis)result.getAxes().get(1);
	} else {
		rowsAxis = null;
	}
	resultSize=rowsAxis.getPositions().size();
	formatter.setPreviousResultSize( formatter.getPreviousResultSize()+resultSize);	                
	logRowlevel("mdxquery------------------------------------------------------->r value "+queryString);  
}

*/

//logRowlevel("-------------------start of geojsonstring------------");
//jsonOuterEnvelopeStartString= "{ \n \t \"type\": \"FeatureCollection\", \n\t \"features\": [ {";
//System.out.println(jsonOuterEnvelopeStartString);
//String jsonMiddleContentString=developJsonPropertyMembersList(outputFilePath);
//jsonMiddleContentString=jsonMiddleContentString.substring(0,jsonMiddleContentString.length()-1);
//System.out.println(jsonMiddleContentString);
//jsonOuterEnvelopeEndString= " ] \n }";
//geojsonfield=jsonOuterEnvelopeStartString + jsonMiddleContentString + jsonOuterEnvelopeEndString;
//logRowlevel("geojsonstring------------------------------------------------------->r value "+geojsonfield);
//r[18]=geojsonfield;         
//r[12]=outputFilePath; 	
//r[10]=randomNumber;


//Object[] outputRow = createOutputRow(r, data.outputRowMeta.size());
//	get(Fields.Out, queryString).setValue(outputRow, queryString);
// int outputIndex = 0;







