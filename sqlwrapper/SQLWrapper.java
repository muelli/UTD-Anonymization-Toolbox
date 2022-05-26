package sqlwrapper;

/**
 * Interface for embedded database operations.
 * 
 */

public interface SQLWrapper {
	boolean execute (String sql);
	QueryResult executeQuery(String sql) ;
	void commit();
	boolean flush();
}
