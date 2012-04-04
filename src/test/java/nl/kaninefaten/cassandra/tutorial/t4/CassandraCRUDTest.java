package nl.kaninefaten.cassandra.tutorial.t4;

import static org.junit.Assert.fail;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * CRUD test on a Cassandra Collumn Family
 * <p>
 * <ul>
 * 	<li>This class creates an embedded server. Starts it.
 * 	<li>Creates a ColumnFamily
 *  <li>Creates a Row
 * 	<li>Finds the row based on the key
 * 	<li>Updates a value based on the key
 * 	<li>Deletes the row based on the key.
 *  <li>Stops de server and exits the jvm
 * </ul>
 * <p>
 * The code is demonstration code on how the hector api can be used.
 * 
 * @author Patrick van Amstel
 * @date 2012 04 02
 *
 */
public class CassandraCRUDTest  {

	/** Name of test cluster*/
	public static String clusterName = "TestCluster";
	
	/** Name and port of test cluster*/
	public static String host = "localhost:9171";
	
	/** Name of key space to create*/
	public static String keyspaceName = "keySpaceName";
	
	public static Keyspace keyspace = null;
	
	/** Name of the column family*/
	public static String columnFamilyName = "AColumnFamily";
	
	/** Cluster to talk to*/
	private static Cluster cluster = null;
	
	static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();
        
	
	// Before these unit tests are called initialize the junit class with this server
	@BeforeClass
	public static void start() throws Throwable {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();

		// Gets the thrift client with name an host
		cluster = HFactory.getOrCreateCluster(clusterName, host);

		// Creates the keyspace in Cassandra
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, 1, null);
		cluster.addKeyspace(newKeyspace);
		
		// Keyspace to work with
		// A keyspace is like user or schema in Oracle
		keyspace = HFactory.createKeyspace(keyspaceName, cluster);	
	}

	// Break down the server when unit tests are executed.
	@AfterClass
	public static void stop(){
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
		
	// Sets up the sheet in Cassandra
	@Test
	public void testCreateColumnFamily() {
		// Create
		try {
			ColumnFamilyDefinition userColumnFamilyDefinition = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName, ComparatorType.UTF8TYPE);
			cluster.addColumnFamily(userColumnFamilyDefinition);
		} catch (Throwable t) {
			fail(t.toString());
		}
	}
	
	@Test
	public void testCreateRow() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

		// Inserts StringValue1 in a place that is called ColumnName1
		mutator.addInsertion("KEY1", columnFamilyName, HFactory.createStringColumn("ColumnName1", "StringValue1"));
		// Inserts StringValue2 in a place that is called ColumnName2
		mutator.addInsertion("KEY1", columnFamilyName, HFactory.createStringColumn("ColumnName2", "StringValue2"));
		// Inserts StringValue3 in a place that is called ColumnName3
		mutator.addInsertion("KEY1", columnFamilyName, HFactory.createStringColumn("ColumnName3", "StringValue3"));
		// Inserts StringValue4 in a place that is called ColumnName4
		mutator.addInsertion("KEY1", columnFamilyName, HFactory.createStringColumn("ColumnName4", "StringValue4"));

		// Note that the mutator does not differentiate in key values / columnFamilies or column names.
		// It just executes		
		mutator.execute();
	}

	
	
	// Reads the row
	@Test
	public void testReadRow() {
		
		// Create query to fetch a result
		SliceQuery<String, String, String> result = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
		result.setColumnFamily(columnFamilyName);
		result.setKey("KEY1");	
		
		// To order the result
		// Note there is no ordering Cassandra
		String [] columnNames = new String[]{"ColumnName1","ColumnName2","ColumnName3","ColumnName4"};
		result.setColumnNames(columnNames);
		
		QueryResult<ColumnSlice<String, String>> columnSlice = result.execute();
		
		if (columnSlice.get().getColumns().isEmpty()) {
			fail("Could not find created row");
		}
		
		String value1 = columnSlice.get().getColumnByName("ColumnName1").getValue();
		String value2 = columnSlice.get().getColumnByName("ColumnName2").getValue();
		String value3 = columnSlice.get().getColumnByName("ColumnName3").getValue();
		String value4 = columnSlice.get().getColumnByName("ColumnName4").getValue();
		
		Assert.assertEquals("Values must match","StringValue1",value1);
		Assert.assertEquals("Values must match","StringValue2",value2);
		Assert.assertEquals("Values must match","StringValue3",value3);
		Assert.assertEquals("Values must match","StringValue4",value4);
		
	}
	
	@Test
	public void testUpdateRow() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

		mutator.addInsertion("KEY1", columnFamilyName, HFactory.createStringColumn("ColumnName1", "StringValue1Updates"));
		mutator.execute();

		SliceQuery<String, String, String> result = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
		result.setColumnFamily(columnFamilyName);
		result.setKey("KEY1");

		String[] columnNames = new String[] { "ColumnName1", "ColumnName2", "ColumnName3", "ColumnName4" };
		result.setColumnNames(columnNames);

		QueryResult<ColumnSlice<String, String>> columnSlice = result.execute();

		if (columnSlice.get().getColumns().isEmpty()) {
			fail("Could not find created row");
		}

		String value1 = columnSlice.get().getColumnByName("ColumnName1").getValue();
		String value2 = columnSlice.get().getColumnByName("ColumnName2").getValue();
		String value3 = columnSlice.get().getColumnByName("ColumnName3").getValue();
		String value4 = columnSlice.get().getColumnByName("ColumnName4").getValue();
		Assert.assertEquals("Values must match", "StringValue1Updates", value1);
		Assert.assertEquals("Values must match", "StringValue2", value2);
		Assert.assertEquals("Values must match", "StringValue3", value3);
		Assert.assertEquals("Values must match", "StringValue4", value4);

	}

	@Test
	public void testDeleteRow() {

		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.addDeletion("KEY1", columnFamilyName, null, stringSerializer);
		mutator.execute();

		SliceQuery<String, String, String> result = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
		result.setColumnFamily(columnFamilyName);
		result.setKey("KEY1");

		String[] columnNames = new String[] { "ColumnName1", "ColumnName2", "ColumnName3", "ColumnName4" };
		result.setColumnNames(columnNames);

		QueryResult<ColumnSlice<String, String>> columnSlice = result.execute();

		if (!columnSlice.get().getColumns().isEmpty()) {
			fail("Could find deleted row");
		}

	}
	
}
