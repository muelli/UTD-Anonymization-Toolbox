package sqlwrapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Implementation of the query ResultSet that supports pageable query for handling 
 * large data sets that does not fit into the memory.
 */

public class QueryResult implements Iterator{

	private String query;
	private Statement stat;
	
	private int currentRecordInd = 0;
	private int currentPageInd = 0;
	private ArrayList currentRecord = new ArrayList();
	private int recordCount;
	private int numCols=0;
	private int pageSize = 10000;
	private int numberOfPages;
	private boolean pageLoaded = false; 
	
	private ResultSet currentPage;
	
	/**
	 * Class constructor
	 * @param stat Statement object for executing sql query
	 * @param sql SQL query
	 */
	
	public QueryResult(Statement stat, String sql) throws SQLException{
		
		this.query = sql;
		this.stat = stat;
		
		ResultSet rs = stat.executeQuery("select count(*) as recordCount from ( " + sql + " ) as temp");
		
		if(rs.next())
			recordCount = rs.getInt(1);
		
		numberOfPages = recordCount/ pageSize;
	}
	
	
	/**
	 * Returns false if resutSet pointer reaches the end of the set, returns true otherwise.
	 * @return success
	 */
	
	public boolean hasNext() {
		
		if(currentRecordInd >= recordCount) {
			try {
				stat.close();
				
				if(currentPage != null)
					currentPage.close();
			} catch(SQLException e) { e.printStackTrace(); }
			return false;
		}		
		return true;
	}

	
	/**
	 * Extract the next data from the resultSet. If the end of the currentPage 
	 * is reached, next page is retrieved from disk to memory.
	 * 
	 * @return next tuple from the resultSet
	 */
	
	public Object next() {
		currentRecord.clear();
		try {
			if (currentPage == null || !currentPage.next()) 
				nextPage();
		} catch (SQLException e1) {	e1.printStackTrace();}
		    
		if (currentPage != null) {
			if (pageLoaded) {
				try {
					currentPage.next();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				pageLoaded = false; 
			}

			currentRecordInd++;
			return currentPage;
		}
		return  null;
	} 
	
	/**
	 * Next page is retrieved from disk to memory
	 */
	
	public void nextPage() throws SQLException {
	         
		int startIndex;
		int endIndex;
		
	        if(currentPageInd <= numberOfPages) {
	        	
	        	startIndex = currentPageInd*pageSize ;
	        	
	        	endIndex = startIndex + pageSize;
	        	
	        	if (endIndex > recordCount+1)
	        		endIndex = recordCount +1 ; 
	        		
	        		String pageQuery;
	        		if(query.toLowerCase().contains(" limit 1 offset ")) {
	        			pageQuery = query;
	        		} else {
	        			pageQuery = query + " limit " + (endIndex- startIndex)+ " offset " +  startIndex;
	        		}
	        		
	        		currentPage = stat.executeQuery(pageQuery);
	        		
	        		if (numCols == 0) {
	        			ResultSetMetaData rsmd = currentPage.getMetaData();
	        			numCols = rsmd.getColumnCount();
	        		}
	        	           		
	        	currentPageInd++;
	        	pageLoaded = true;
	        	
	        }else
	        	currentPage = null; 
	}
	
	/**
	 * Set the maximum number of tuples that resides in a page 
	 * @param pageSize number of tuples 
	 */
	
	public void setPageSize(int pageSize){
		this.pageSize = pageSize;
	}


	public void remove() {		
	}		
}

