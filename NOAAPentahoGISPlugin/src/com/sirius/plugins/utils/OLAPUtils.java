package com.sirius.plugins.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;


public class OLAPUtils {

	/**
	 * 
	 * @param rowMeta
	 * @param r
	 * @return
	 * @throws KettleValueException
	 * 
	 * This method puts all of the row data (paramenters) into a HashMap for easy access
	 * 
	 */
	public static HashMap<String,String> getParameterMap(RowMetaInterface rowMeta, Object[] r) throws KettleValueException {
		
		HashMap<String,String> map = new HashMap<String,String>();
		
		try {
			
			map.put("cubes", rowMeta.getString(r, 0).trim());
			map.put("pod0Level", rowMeta.getString(r, 1).trim());
			map.put("pod0Members", rowMeta.getString(r, 2).trim());
			map.put("pod1Level", rowMeta.getString(r, 3).trim());
			map.put("pod1Members", rowMeta.getString(r, 4).trim());
			map.put("pod2Level", rowMeta.getString(r, 5).trim());
			map.put("pod2Members", rowMeta.getString(r, 6).trim());
			map.put("pod3Level", rowMeta.getString(r, 7).trim());
			map.put("pod3Members", rowMeta.getString(r, 8).trim());
			map.put("additionalFields", rowMeta.getString(r, 9).trim());
			map.put("displayByField", rowMeta.getString(r, 10).trim());
			map.put("Limit", rowMeta.getString(r, 11).trim());
			map.put("outputFilePath", rowMeta.getString(r, 12).trim());
			
			map.put("polygon", rowMeta.getString(r, 15).trim());
			
			map.put("mondrianSchemaFilePath", rowMeta.getString(r, 19).trim());
			
		} catch (KettleValueException e) {
			throw e;
		}
		return map;
	}
	
	/**
	 * 
	 * @param parameterMap
	 * @return
	 * 
	 * This method generates the "center" portion of the MDX query
	 * 
	 */
	public static String generateMDXQueryData(HashMap<String,String> parameterMap) {
		
		StringBuffer mdxQueryData = new StringBuffer("");
		String pod = "";
		HashMap<String,String> podMap = new HashMap<String, String>();
		
		// Get all the POD data from the input rows (4 possible PODS, POD0-POD3)
	    for (int i=0;i<4;i++) {
	    	pod = parameterMap.get("pod" + i + "Level");
	    	if (pod.length() != 0) {
	    		podMap.put(pod, parameterMap.get("pod" + i + "Members"));
	    	}	    	
	    }
	    
	    // Do additional Fields
	    // If any of the additional fields already exist as part of the POD query we do not add the generic
	    StringTokenizer afST = new StringTokenizer(parameterMap.get("additionalFields"), ","); 
	    while (afST.hasMoreTokens()) {
	    	String key = afST.nextToken().trim();
	    	if (!podMap.containsKey(key)) {
	    		podMap.put(key, key + ".Members");
	    	}
	    }
	    
	    // Do display by field
	    // If the display by field already exists as part of the POD query we do not add the generic
	    String displayByField = parameterMap.get("displayByField");
	    if (displayByField.length() != 0) {
	    	if (!podMap.containsKey(displayByField)) {
	    		podMap.put(displayByField, displayByField + ".Members");
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
    	iter = mdxColumns.iterator();
    	while (iter.hasNext()) {
    		// First one is special since we don't know if we have anything to join yet
    		if (mdxQueryData.length() == 0) {
    			mdxQueryData.append("{ " + iter.next() + " }");
    		} else {
    			mdxQueryData.insert(0, "nonemptycrossjoin (");
    			mdxQueryData.append(", { " + iter.next() + " })");
    		}
	    }	
    	
		return mdxQueryData.toString();
	}
	
	/**
	 * 
	 * @param dbHost
	 * @param dbName
	 * @param dbUserName
	 * @param dbPassword
	 * @param mondrianSchemaFilePath
	 * @return
	 * @throws SQLException, Exception 
	 * 
	 */
	public static Connection createDbConnection(String dbHost, String dbName, String dbUserName, String dbPassword, String mondrianSchemaFilePath) throws SQLException, Exception {
		
		Connection connection = null;
		String dbConnectionUrlString = "Jdbc=jdbc:postgresql://" + dbHost +
				"/" + dbName + ";JdbcUser=" + dbUserName + ";JdbcPassword=" + dbPassword + ";";
		
    	try {
			connection = DriverManager.getConnection("jdbc:mondrian:" + "JdbcDrivers=org.postgresql.Driver;" +
					dbConnectionUrlString + "Catalog=file:" + mondrianSchemaFilePath + ";");
    	} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
    	
    	return connection;
	}
}





