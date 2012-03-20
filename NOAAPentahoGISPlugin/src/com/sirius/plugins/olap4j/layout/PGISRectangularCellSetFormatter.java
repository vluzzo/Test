package com.sirius.plugins.olap4j.layout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.Position;
import org.olap4j.impl.CoordinateIterator;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.metadata.Member;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class PGISRectangularCellSetFormatter extends RectangularCellSetFormatter {

    private final boolean compact;
    private String polygon= "";
    private long PreviousResultSize = 0;
    public final static String polygonTemplate = "POLYGON(-98.500 31.000, -79.500 31.000, -79.500 23.000, -98.500 23.000, -98.500 31.000)"; 
    private boolean executeCurrentQuery = true;
    private long maxResultsSize = 0;
    private int GeomColumnNumber = 0;
    //private static int polygonString = 0;
    
    public PGISRectangularCellSetFormatter(boolean compact) {
    	super(compact);
        this.compact = compact;
    }
    
    public long getPreviousResultSize() {
		return PreviousResultSize;
	}
    
	public long getMaxResultsSize() {
		return maxResultsSize;
	}
	
	public String getPolygon() {
		return polygon;
	}
	
	public boolean isExecuteCurrentQuery() {
		return executeCurrentQuery;
	}

	public void setExecuteCurrentQuery(boolean executeCurrentQuery) {
		this.executeCurrentQuery = executeCurrentQuery;
	}

	public void setMaxResultsSize(long maxResultsSize) {
		this.maxResultsSize = maxResultsSize;
	}
	
	public void setPolygon(String polygon) {
		this.polygon = polygon;
	}
	
	public void setPreviousResultSize(long previousResultSize) {
		PreviousResultSize = previousResultSize;
	}
	
    public BufferedReader createBufferedReader(File file) throws FileNotFoundException{
    	return new BufferedReader(new FileReader(file));
    }
	    
    public void processResults(ArrayList CellSetList,
			RectangularCellSetFormatter formatter, String outputFilePath) throws Exception {
    	
    	PrintWriter pwcsv=new PrintWriter(outputFilePath);
    	CellSet result=null;
    	PrintWriter pw=null;
    	String tmpToken="";
    	int tokenCount=0;
    	StringBuilder sb=null;
    	boolean headerExecutionFlag=true;
    	Iterator cellSetIterator=CellSetList.iterator();    	 
    	while(cellSetIterator.hasNext()){
		    		sb = new StringBuilder();
		    		StringWriter sw = new StringWriter();
		    		pw = new PrintWriter(sw);
		    		result=(CellSet) cellSetIterator.next();
		    		if(headerExecutionFlag){
		    			headerExecutionFlag=false;
	    			// Compute how many columns are required to display the rows axis.
			         // In the example, this is 3 (the width of USA, CA, Los Angeles)
			         final CellSetAxis rowsAxiss;
			         if (result.getAxes().size() > 1) {
			        	 rowsAxiss = result.getAxes().get(1);
			         } else {
			        	 rowsAxiss = null;
			         }
			         AxisInfo rowsAxisInfo = computeAxisInfo(rowsAxiss,pw);
		    			
			    		final CellSetAxis columnsAxiss;
			         if (result.getAxes().size() > 0) {
			        	 columnsAxiss = result.getAxes().get(0);
			         } else {
			        	 columnsAxiss = null;
			         }
			         pw.print(",");
			         AxisInfo columnsAxisInfo = computeAxisInfo(columnsAxiss,pw);
			    	pw.println(); 
    		}
	    	formatter.format(result,pw);
	    	pw.flush();
	    	sb.append(sw.toString());
	    	pwcsv.print(sb);
	    	pwcsv.flush();
    	}
    	
    	
    //	System.out.println(sb);
    	
	}
    
    /**
     * Formats a two-dimensional page.
     *
     * @param cellSet Cell set
     * @param pw Print writer
     * @param pageCoords Coordinates of page [page, chapter, section, ...]
     * @param columnsAxis Columns axis
     * @param columnsAxisInfo Description of columns axis
     * @param rowsAxis Rows axis
     * @param rowsAxisInfo Description of rows axis
     */
    private void formatPage(
        CellSet cellSet,
        PrintWriter pw,
        int[] pageCoords,
        CellSetAxis columnsAxis,
        AxisInfo columnsAxisInfo,
        CellSetAxis rowsAxis,
        AxisInfo rowsAxisInfo)
    {
        if (pageCoords.length > 0) {
            pw.println();
            for (int i = pageCoords.length - 1; i >= 0; --i) {
                int pageCoord = pageCoords[i];
                final CellSetAxis axis = cellSet.getAxes().get(2 + i);
                pw.print(axis.getAxisOrdinal() + ": ");
                final Position position =
                    axis.getPositions().get(pageCoord);
                int k = -1;
                for (Member member : position.getMembers()) {
                    if (++k > 0) {
                        pw.print(", ");
                    }
                    pw.print(member.getUniqueName().trim());
                }
                pw.println();
            }
        }
        // Figure out the dimensions of the blank rectangle in the top left
        // corner.
        final int yOffset = columnsAxisInfo.getWidth();
        final int xOffsset = rowsAxisInfo.getWidth();

        // Populate a string matrix
        Matrix matrix =
            new Matrix(
                xOffsset
                + (columnsAxis == null
                    ? 1
                    : columnsAxis.getPositions().size()),
                yOffset
                + (rowsAxis == null
                    ? 1
                    : rowsAxis.getPositions().size()));

        // Populate corner
        for (int x = 0; x < xOffsset; x++) {
            for (int y = 0; y < yOffset; y++) {
                matrix.set(x, y, "", false, x > 0);
            }
        }

        // Populate matrix with cells representing axes
        //noinspection SuspiciousNameCombination
        populateAxis(
            matrix, columnsAxis, columnsAxisInfo, true, xOffsset);
        populateAxis(
            matrix, rowsAxis, rowsAxisInfo, false, yOffset);

        // Populate cell values
        for (Cell cell : cellIter(pageCoords, cellSet)) {
            final List<Integer> coordList = cell.getCoordinateList();
            int x = xOffsset;
            if (coordList.size() > 0) {
                x += coordList.get(0);
            }
            int y = yOffset;
            if (coordList.size() > 1) {
                y += coordList.get(1);
            }
            matrix.set(
                x, y, cell.getFormattedValue(), true, false);
        }

        int[] columnWidths = new int[matrix.width];
        int widestWidth = 0;
        for (int x = 0; x < matrix.width; x++) {
            int columnWidth = 0;
            for (int y = 0; y < matrix.height; y++) {
                MatrixCell cell = matrix.get(x, y);
                if (cell != null) {
                    columnWidth =
                        Math.max(columnWidth, cell.value.length());
                }
            }
            columnWidths[x] = columnWidth;
            widestWidth = Math.max(columnWidth, widestWidth);
        }

        // Create a large array of spaces, for efficient printing.
        char[] spaces = new char[widestWidth + 1];
        Arrays.fill(spaces, ' ');
        char[] equals = new char[widestWidth + 1];
        Arrays.fill(equals, '=');
        char[] dashes = new char[widestWidth + 3];
        Arrays.fill(dashes, '-');

        if (this.compact) {
            for (int y = 0; y < matrix.height; y++) {
                for (int x = 0; x < matrix.width; x++) {
                    if (x > 0) {
                        pw.print(' ');
                    }
                    final MatrixCell cell = matrix.get(x, y);
                    final int len;
                    if (cell != null) {
                        if (cell.sameAsPrev) {
                            len = 0;
                        } else {
                            if (cell.right) {
                                int padding =
                                    columnWidths[x] - cell.value.trim().length();
                         //       pw.write(spaces, 0, padding);
                                pw.print("\""+cell.value.trim()+"\"");
                                continue;
                            }
                            pw.print("\""+cell.value.trim()+"\"");
                            len = cell.value.trim().length();
                        }
                    } else {
                        len = 0;
                    }
                    if (x == matrix.width - 1) {
                        // at last column; don't bother to print padding
                        break;
                    }
                    int padding = columnWidths[x] - len;
                //    pw.write(spaces, 0, padding);
                }
                pw.println();
                if (y == yOffset - 1) {
                    for (int x = 0; x < matrix.width; x++) {
                        if (x > 0) {
                            pw.write(' ');
                        }
                        pw.write(equals, 0, columnWidths[x]);
                    }
                    pw.println();
                }
            }
        } else {
       	 String The_Geom = null;
       	 //Implementation of LIMIT for the results
       	 long tmpResultSize=0, maxResults=0;
       	 long maxResultsSize=getMaxResultsSize();
       	maxResults= matrix.height >= maxResultsSize? maxResultsSize : matrix.height;
       	                
       	//For multi mdx queries
       	if(PreviousResultSize > 0){
       		tmpResultSize=getPreviousResultSize();
       		maxResults= (tmpResultSize+matrix.height ) >= maxResultsSize? maxResultsSize :(tmpResultSize+matrix.height );       		
       	}
       	boolean emptyLine=false;
       	boolean iscompared = false;
       	String lineString="";
         for (int y = 1; y <= maxResults; y++) {
        	 iscompared = false;
        	 emptyLine=false;
        	 lineString="";
             for (int x = 0; x < matrix.width; x++) {
                 final MatrixCell cell = matrix.get(x, y);
                 String tmpPolygon=getPolygon();
                 if(! (polygonTemplate.equals(getPolygon()))){
                 if(!iscompared){
                	  The_Geom = (matrix.get(getGeomColumnNumber(), y) == null)? "null" : matrix.get(getGeomColumnNumber(), y).value;
                	  if(The_Geom.contains("null") || (The_Geom.equals(""))) {
                		  break;
                	  }
                	  GeometryFactory fact = new GeometryFactory();
                	  WKTReader wktRdr = new WKTReader(fact);
                	  String wktA = getPolygon();
                	  String wktB = The_Geom;
                	  Geometry A;
					try {
						A = wktRdr.read(wktA);
					    A.setSRID(4326);
                	  Geometry B = wktRdr.read(wktB);
                	  B.setSRID(4326);
                	  iscompared = true;
                	  if(!A.intersects(B)){
                		  emptyLine=true;
                		  break;
                	  }
					} catch (ParseException e) {
						//e.printStackTrace(); 
						 break;
					}
                  }
                }
                    final int len;
                    String cellValue = (cell == null)? "null" : cell.value.trim();
              	  if(cellValue.contains("null") || (cellValue.equals(""))) {
              		  continue;
              	  }
                    //cellValue=cell.value.trim();
                    lineString=lineString+cellValue;
                    if(lineString.trim().length() ==0) {
                		emptyLine= true;
                	} 
                    else emptyLine=false;
                    if(cellValue.equals("")){                    	                   	
                    	continue;
                    }
                    
                    if(cellValue.contains("\"")){
                    	cellValue=cellValue.replace("\"", "\"\"");
                    }
                    if (cell != null) {
                        if (cell.sameAsPrev) {
                            pw.print("  ");
                            len = 0;
                        } else {
                        	if(x !=0){
                            pw.print(",");
                        	}
                            if (cell.right) {
                                int padding =
                                    columnWidths[x] - cellValue.length();
                                pw.print("\""+cellValue+"\"");
                                pw.print(' ');
                                continue;
                            }
                            pw.print("\""+cellValue+"\"");
                            len =cellValue.length();
                        }
                    } 
                }
               if(emptyLine==false){  // dont perform a new line if the current line is empty
                pw.println(' ');
               }
            }
        }
    }
    
    
    /**
     * Populates cells in the matrix corresponding to a particular axis.
     *
     * @param matrix Matrix to populate
     * @param axis Axis
     * @param axisInfo Description of axis
     * @param isColumns True if columns, false if rows
     * @param offset Ordinal of first cell to populate in matrix
     */
    private void populateAxis(
        Matrix matrix,
        CellSetAxis axis,
        AxisInfo axisInfo,
        boolean isColumns,
        int offset)
    {
        if (axis == null) {
            return;
        }
        Member[] prevMembers = new Member[axisInfo.getWidth()];
        Member[] members = new Member[axisInfo.getWidth()];
        for (int i = 0; i < axis.getPositions().size(); i++) {
            final int x = offset + i;
            Position position = axis.getPositions().get(i);
            int yOffset = 0;
            final List<Member> memberList = position.getMembers();
            for (int j = 0; j < memberList.size(); j++) {
                Member member = memberList.get(j);
                final AxisOrdinalInfo ordinalInfo =
                    axisInfo.ordinalInfos.get(j);
                while (member != null) {
                    if (member.getDepth() < ordinalInfo.minDepth) {
                        break;
                    }
                    final int y =
                        yOffset
                        + member.getDepth()
                        - ordinalInfo.minDepth;
                    members[y] = member;
                    member = member.getParentMember();
                }
                yOffset += ordinalInfo.getWidth();
            }
            boolean same = true;
            for (int y = 0; y < members.length; y++) {
                Member member = members[y];
                same =
                    same
                    && i > 0
                    && Olap4jUtil.equal(prevMembers[y], member);
                String value =
                    member == null
                        ? ""
                        : member.getCaption();
                if (isColumns) {
                    matrix.set(x, y, value, false, same);
                } else {
                    if (same) {
                        value = member == null ? "" : member.getCaption();
                    }
                    //noinspection SuspiciousNameCombination
                    matrix.set(y, x, value, false, false);
                }
                prevMembers[y] = member;
                members[y] = null;
            }
        }
    }
    
     private AxisInfo computeAxisInfo(CellSetAxis axis,PrintWriter pw)
    {
    	String levelName="",levelNameList="";
    	
        if (axis == null) {
            return new AxisInfo(0);
        }
        final AxisInfo axisInfo =
            new AxisInfo(axis.getAxisMetaData().getHierarchies().size());
        int p = -1;
        int columnCount=0;
        for (Position position : axis.getPositions()) {
            ++p;            
            int k = -1;
            for (Member member : position.getMembers()) {
                ++k;
                final AxisOrdinalInfo axisOrdinalInfo =
                    axisInfo.ordinalInfos.get(k);
                final int topDepth =
                    member.isAll()
                        ? member.getDepth()
                        : member.getHierarchy().hasAll()
                            ? 1
                            : 0;
                if (axisOrdinalInfo.minDepth > topDepth
                    || p == 0)
                {
                    axisOrdinalInfo.minDepth = topDepth;
                }
                axisOrdinalInfo.maxDepth =
                    Math.max(
                        axisOrdinalInfo.maxDepth,
                        member.getDepth());
                
                levelName=member.getDimension().getUniqueName();
                if(levelName.equals("[LocationGeom]")){
                	setGeomColumnNumber(columnCount);
                }
                
                if(levelName.contains("nrda_featurestyle")){
                	levelName="nrda_featurestyle";
                }
                else if(levelName.contains("legend_text")){
                	levelName="legend_text";
                }
                else if(levelName.equals("[Measures]")){
                	levelName=member.getUniqueName();
                }
                else{
                	//levelName=member.getDimension().getUniqueName()+".["+member.getLevel().getCaption()+"]";
                	levelName = member.getLevel().getUniqueName();
                }
                
                /*
                // Test
                String tmpString = member.getCaption();
                tmpString = member.getDescription();
                tmpString = member.getName();
                tmpString = member.getUniqueName();
                tmpString = member.getDimension().getCaption();
                tmpString = member.getDimension().getDescription();
                tmpString = member.getDimension().getName();
                tmpString = member.getDimension().getUniqueName();
                tmpString = member.getLevel().getCaption();
                tmpString = member.getLevel().getDescription();
                tmpString = member.getLevel().getName();
                tmpString = member.getLevel().getUniqueName();
                */
                
                levelNameList=levelNameList+","+"\""+levelName+"\"";
                columnCount++;
            }
            levelNameList=levelNameList.substring(1,levelNameList.length());
            pw.print(levelNameList);
            break;
        }
        return axisInfo;
    }
    
	public int getGeomColumnNumber() {
		return GeomColumnNumber;
	}

	public void setGeomColumnNumber(int geomColumnNumber) {
		GeomColumnNumber = geomColumnNumber;
	}
	
    
	// ******************************************************************************************	
	// ******************************************************************************************
	// These classes and methods below were copied directly from the olap4j-1.0.0.445 source code
	// ******************************************************************************************
	// ******************************************************************************************
	/**
     * Description of an axis.
     */
    private static class AxisInfo {
        final List<AxisOrdinalInfo> ordinalInfos;

        /**
         * Creates an AxisInfo.
         *
         * @param ordinalCount Number of hierarchies on this axis
         */
        AxisInfo(int ordinalCount) {
            ordinalInfos = new ArrayList<AxisOrdinalInfo>(ordinalCount);
            for (int i = 0; i < ordinalCount; i++) {
                ordinalInfos.add(new AxisOrdinalInfo());
            }
        }

        /**
         * Returns the number of matrix columns required by this axis. The
         * sum of the width of the hierarchies on this axis.
         *
         * @return Width of axis
         */
        public int getWidth() {
            int width = 0;
            for (AxisOrdinalInfo info : ordinalInfos) {
                width += info.getWidth();
            }
            return width;
        }
    }
    
    /**
     * Description of a particular hierarchy mapped to an axis.
     */
    private static class AxisOrdinalInfo {
        int minDepth = 1;
        int maxDepth = 0;

        /**
         * Returns the number of matrix columns required to display this
         * hierarchy.
         */
        public int getWidth() {
            return maxDepth - minDepth + 1;
        }
    }
	
    /**
     * Two-dimensional collection of string values.
     */
    private class Matrix {
        private final Map<List<Integer>, MatrixCell> map =
            new HashMap<List<Integer>, MatrixCell>();
        private final int width;
        private final int height;

        /**
         * Creats a Matrix.
         *
         * @param width Width of matrix
         * @param height Height of matrix
         */
        public Matrix(int width, int height) {
            this.width = width;
            this.height = height;
        }

        /**
         * Sets the value at a particular coordinate
         *
         * @param x X coordinate
         * @param y Y coordinate
         * @param value Value
         */
        void set(int x, int y, String value) {
            set(x, y, value, false, false);
        }

        /**
         * Sets the value at a particular coordinate
         *
         * @param x X coordinate
         * @param y Y coordinate
         * @param value Value
         * @param right Whether value is right-justified
         * @param sameAsPrev Whether value is the same as the previous value.
         * If true, some formats separators between cells
         */
        void set(
            int x,
            int y,
            String value,
            boolean right,
            boolean sameAsPrev)
        {
            map.put(
                Arrays.asList(x, y),
                new MatrixCell(value, right, sameAsPrev));
            assert x >= 0 && x < width : x;
            assert y >= 0 && y < height : y;
        }

        /**
         * Returns the cell at a particular coordinate.
         *
         * @param x X coordinate
         * @param y Y coordinate
         * @return Cell
         */
        public MatrixCell get(int x, int y) {
            return map.get(Arrays.asList(x, y));
        }
    }
    
    /**
     * Contents of a cell in a matrix.
     */
    private static class MatrixCell {
        final String value;
        final boolean right;
        final boolean sameAsPrev;

        /**
         * Creates a matrix cell.
         *
         * @param value Value
         * @param right Whether value is right-justified
         * @param sameAsPrev Whether value is the same as the previous value.
         * If true, some formats separators between cells
         */
        MatrixCell(
            String value,
            boolean right,
            boolean sameAsPrev)
        {
            this.value = value;
            this.right = right;
            this.sameAsPrev = sameAsPrev;
        }
    }
    
    /**
 * Returns an iterator over cells in a result.
 */
private static Iterable<Cell> cellIter(
    final int[] pageCoords,
    final CellSet cellSet)
{
    return new Iterable<Cell>() {
        public Iterator<Cell> iterator() {
            int[] axisDimensions =
                new int[cellSet.getAxes().size() - pageCoords.length];
            assert pageCoords.length <= axisDimensions.length;
            for (int i = 0; i < axisDimensions.length; i++) {
                CellSetAxis axis = cellSet.getAxes().get(i);
                axisDimensions[i] = axis.getPositions().size();
            }
            final CoordinateIterator coordIter =
                new CoordinateIterator(axisDimensions, true);
            return new Iterator<Cell>() {
                public boolean hasNext() {
                    return coordIter.hasNext();
                }

                public Cell next() {
                    final int[] ints = coordIter.next();
                    final AbstractList<Integer> intList =
                        new AbstractList<Integer>() {
                            public Integer get(int index) {
                                return index < ints.length
                                    ? ints[index]
                                    : pageCoords[index - ints.length];
                            }

                            public int size() {
                                return pageCoords.length + ints.length;
                            }
                        };
                    return cellSet.getCell(intList);
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };
}
    
public void format(CellSet cellSet, PrintWriter pw) {
    // Compute how many rows are required to display the columns axis.
    // In the example, this is 4 (1997, Q1, space, Unit Sales)
    final CellSetAxis columnsAxis;
    if (cellSet.getAxes().size() > 0) {
        columnsAxis = cellSet.getAxes().get(0);
    } else {
        columnsAxis = null;
    }
    AxisInfo columnsAxisInfo = computeAxisInfo(columnsAxis);

    // Compute how many columns are required to display the rows axis.
    // In the example, this is 3 (the width of USA, CA, Los Angeles)
    final CellSetAxis rowsAxis;
    if (cellSet.getAxes().size() > 1) {
        rowsAxis = cellSet.getAxes().get(1);
    } else {
        rowsAxis = null;
    }
    AxisInfo rowsAxisInfo = computeAxisInfo(rowsAxis);

    if (cellSet.getAxes().size() > 2) {
        int[] dimensions = new int[cellSet.getAxes().size() - 2];
        for (int i = 2; i < cellSet.getAxes().size(); i++) {
            CellSetAxis cellSetAxis = cellSet.getAxes().get(i);
            dimensions[i - 2] = cellSetAxis.getPositions().size();
        }
        for (int[] pageCoords : CoordinateIterator.iterate(dimensions)) {
            formatPage(
                cellSet,
                pw,
                pageCoords,
                columnsAxis,
                columnsAxisInfo,
                rowsAxis,
                rowsAxisInfo);
        }
    } else {
        formatPage(
            cellSet,
            pw,
            new int[] {},
            columnsAxis,
            columnsAxisInfo,
            rowsAxis,
            rowsAxisInfo);
    }
}
    
/**
 * Computes a description of an axis.
 *
 * @param axis Axis
 * @return Description of axis
 */
private AxisInfo computeAxisInfo(CellSetAxis axis)
{
    if (axis == null) {
        return new AxisInfo(0);
    }
    final AxisInfo axisInfo =
        new AxisInfo(axis.getAxisMetaData().getHierarchies().size());
    int p = -1;
    for (Position position : axis.getPositions()) {
        ++p;
        int k = -1;
        for (Member member : position.getMembers()) {
            ++k;
            final AxisOrdinalInfo axisOrdinalInfo =
                axisInfo.ordinalInfos.get(k);
            final int topDepth =
                member.isAll()
                    ? member.getDepth()
                    : member.getHierarchy().hasAll()
                        ? 1
                        : 0;
            if (axisOrdinalInfo.minDepth > topDepth
                || p == 0)
            {
                axisOrdinalInfo.minDepth = topDepth;
            }
            axisOrdinalInfo.maxDepth =
                Math.max(
                    axisOrdinalInfo.maxDepth,
                    member.getDepth());
        }
    }
    return axisInfo;
}
    
    
    
    
    
    
    
    
    
    
	
    
    
    
	
	
}
