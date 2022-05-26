package sqlwrapper;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implementation of the SQLWrapper interface for SQLite embedded database. 
 * 
 */

public class SqLiteSQLWrapper implements SQLWrapper {

    private final String driver = "SQLite.JDBCDriver";
    private final String protocol = "jdbc:sqlite:";
    private final String dbName = "toolbox.dat";
    private String dbPath = null;
	
	private Connection conn = null;
    
	/**
	 * Single instance created upon class loading.
	 */
	 
	private static final SqLiteSQLWrapper sqLiteInstance = new SqLiteSQLWrapper();

    private SqLiteSQLWrapper() {	    	
    }
    
	/**
	 * Returns the singleton sqLite instance
	 * @return sqLiteInstance
	 */
    public static SqLiteSQLWrapper getInstance() {
    	
    	try{
    		if(sqLiteInstance.conn == null || sqLiteInstance.conn.isClosed())
    			sqLiteInstance.conn = sqLiteInstance.getConnection();
    		
    	}catch(Exception ex){
    		ex.printStackTrace();
    		return null;
    	}
        
        return sqLiteInstance;
     }

	/**
	 * Execute SQL statement for data definition and manipulation
	 * @param sql Sql operation
	 * @return success of executed operation
	 */
    
	public boolean execute(String sql) {
		Statement st = null;
		try {
			st =sqLiteInstance.conn.createStatement();
			boolean successful = st.execute(sql);
			st.close();
			return successful;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				st.close();
			} catch(Exception e) {}
		}
	}

	/**
	 * Execute SQL statement for data query
	 * @param sql Sql operation
	 * @return queryResult
	 */
	
	public QueryResult executeQuery(String sql) {

		QueryResult result = null;
		
		try {
			Statement st =sqLiteInstance.conn.createStatement();
			result = new QueryResult(st, sql);			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}

	
	/**
	 * Commit transaction
	 */
	
	public void commit() {
		try {
			conn.commit();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Commit transaction, delete temporary data
	 * @return success of the operation 
	 */
	
	public boolean flush() {
		try {
			conn.commit();
			conn.close();
			
			String dbPath = sqLiteInstance.dbPath +"/" +sqLiteInstance.dbName;
			File file = new File(dbPath);
			boolean succ = file.delete();
			
			if(succ){
				File dir = new File(sqLiteInstance.dbPath);
				dir.delete();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	
	private Connection getConnection() throws SQLException {
		 
	    loadDriver();
	    
	    //if(sqLiteInstance.dbPath == null){
	    	
	    	sqLiteInstance.dbPath= System.getProperty("user.home") + "/.Toolbox" ;
	    	
	    	if (!(new File(sqLiteInstance.dbPath)).exists()){
	    			boolean success = (new File(sqLiteInstance.dbPath)).mkdir();
	    			if(!success)
	    				return null;
	    	}
	    	
	    //}
	    
	    conn = DriverManager.getConnection( sqLiteInstance.protocol + "/" + sqLiteInstance.dbPath +"/" +sqLiteInstance.dbName , "", "" );
	    conn.setAutoCommit(false);
	
		return conn;
	}

    private void loadDriver() throws SQLException {
        try {
        	System.loadLibrary("sqlite_jni");
            Class.forName(sqLiteInstance.driver);
         
        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver ");
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } 
    }
}
