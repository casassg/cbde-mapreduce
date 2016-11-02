// Standard classes

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
// HBase classes
// Hadoop classes

public class GroupBy extends Configured implements Tool {

    // These are three global variables, which coincide with the three parameters in the call
    public static String inputTable;
    public static String groupByAttribute;
    public static String aggregateAttribute;
    private static String outputTable;


//=================================================================== Main
        /* 
            Explanation: Assume we want to do the cartesian product of tuples stored in two HBase tables (inputTable and rightInputTable).
            
            First, the main method checks the call parameters. There must be 4:
            1.- First (external) HBase input table where to read data from (it must have been created beforehand, as we will see later)
			2.- Second (internal) HBase input table where to read data from (it must have been created beforehand, as we will see later)
            4.- The HBase output table (it will be created, as we will see later)
            5.- A random value 
            
            Assume a call like this:

            yarn jar myJarFile.jar CartesianProduct Username_InputTable1 Username_InputTable2 Username_OutputTable 10
            
            Assume now the following HBase tables:
			
			- The Username_InputTable1 HBase table containing the following data:
				'key1', 'AAA'
				'key2', 'BBB'
			
			- The Username_InputTable2 HBase table containing the following data:			
				'key3', 'CCC'
				'key4', 'DDD'                 
            
            As we will see later in the map and reduce methods, what we want to do is to join all tuples from Username_InputTable1 with all the tuples from Username_InputTable2 (but those from the same table must not be joined).       
            Next, the main calls the checkIOTables method, which connects with HBase and handles it.
            Finally, the MapReduce task is run (and the run method is called).
       */

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Parameters missing: 'inputTable outputTable aggregateAttribute groupByAttribute'");
            System.exit(1);
        }
        inputTable = args[0];
        outputTable = args[1];
        aggregateAttribute = args[2];
        groupByAttribute = args[3];


        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            int ret = ToolRunner.run(new GroupBy(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }


//============================================================== checkTables

    // Explanation: Handles HBase and its relationship with the MapReduce task

    private static int checkIOTables(String[] args) throws Exception {
        // Obtain HBase's configuration
        Configuration config = HBaseConfiguration.create();
        // Create an HBase administrator
        HBaseAdmin hba = new HBaseAdmin(config);

         /* With an HBase administrator we check if the input tables exist.
            You can modify this bit and create it here if you want. */
        if (!hba.tableExists(inputTable)) {
            System.err.println("Input table does not exist");
            return 2;
        }
         /* Check whether the output table exists. 
            Just the opposite as before, we do create the output table.
            Normally, MapReduce tasks attack an existing table (not created on the fly) and 
            they store the result in a new table. */
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }
        // If you want to insert data through the API, do it here
        // -- Inserts

        // Input
        Map<String, String> values = new HashMap();
        values.put("201301010", "100");
        values.put("201301031", "190");
        values.put("201301061", "210");
        values.put("201301020", "50");
        values.put("201301032", "200");
        values.put("201301042", "210");
        HTable table = new HTable(config, inputTable);
        for (String key : values.keySet()) {
            Put put = new Put(Bytes.toBytes(key)); //creates a new row with key 'key1'
            put.add(Bytes.toBytes("cf1"), Bytes.toBytes("attr1"), Bytes.toBytes(values.get(key))); //Add an attribute named Attribute that belongs to the family Family with value Value
            put.add(Bytes.toBytes("cf1"), Bytes.toBytes("attr2"), Bytes.toBytes(String.valueOf(Integer.valueOf(key) % 10))); //Add an attribute named Attribute that belongs to the family Family with value Value
            table.put(put); // Inserts data
        }


        //You may not need this, but if batch loading, set the recovery manager to Not Force (setAutoFlush false) and deactivate the WAL
        //table.setAutoFlush(false); --This is needed to avoid HBase to flush at every put (instead, it will buffer them and flush when told by using FlushCommits)
        //put.setWriteToWAL(false); --This is to deactivate the Write Ahead Logging

        //If you do it through the HBase Shell, this is equivalent to:
        //put 'Username_InputTable1', 'key1', 'Family:Attribute', 'Value'

        // -- Inserts
        // Get an HBase descriptors pointing at the input tables (the HBase descriptors handle the tables' metadata)
        HTableDescriptor htdInput = hba.getTableDescriptor(inputTable.getBytes());

        // Get an HBase descriptor pointing at the new table
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

        // We copy the structure of the input tables in the output one by adding the input columns to the new table
        for (byte[] key : htdInput.getFamiliesKeys()) {
            System.out.println("family-t1 = " + new String(key));
            htdOutput.addFamily(new HColumnDescriptor(key));
        }

        //Create the new output table based on the descriptor we have been configuring
        hba.createTable(htdOutput);
        return 0;
    }


    //============================================================== Job config
    //Create a new job to execute. This is called from the main and starts the MapReduce job
    public int run(String[] args) throws Exception {

        //Create a new MapReduce configuration object.
        Job job = new Job(HBaseConfiguration.create());
        //Set the MapReduce class
        job.setJarByClass(GroupBy.class);
        //Set the job name
        job.setJobName("GroupBy");
        // To pass parameters to the mapper and reducer we must use the setStrings of the Configuration object
        // We pass the names of two input tables as External and Internal tables of the Cartesian product, and a hash random value.
        job.getConfiguration().setStrings("input", inputTable);
        job.getConfiguration().setStrings("aggA", aggregateAttribute);
        job.getConfiguration().setStrings("groupbyA", groupByAttribute);

         /* Set the Map and Reduce function:
            These are special mapper and reducers, which are prepared to read and store data on HBase tables								 
		 */


        // To initialize the mapper, we need to provide two Scan objects (ArrayList of two Scan objects) for two input tables, as follows.
        ArrayList scans = new ArrayList();

        Scan scan1 = new Scan();
        System.out.println("inputTable: " + inputTable);

        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable));
        scans.add(scan1);


        TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        //Then we wait until the MapReduce task finishes? (block the program)
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


//=================================================================== Mapper

       /* This is the mapper class, which implements the map method. 
          The MapReduce framework will call this method automatically when needed.
          Note its header defines the input key-value and an object where to store the resulting key-values produced in the map.
          - ImmutableBytesWritable rowMetadata: The input key
          - Result values: The input value associated to that key
          - Context context: The object where to store all the key-values generated (i.e., the map output) */

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {
            int i = 0;
            String[] input = context.getConfiguration().getStrings("input", "Default");
            String[] groupByAttribute = context.getConfiguration().getStrings("groupbyA", "Default");

            // From the context object we obtain the input TableSplit this row belongs to
            TableSplit currentSplit = (TableSplit) context.getInputSplit();

		   /* 
              From the TableSplit object, we can further extract the name of the table that the split belongs to.
			  We use the extracted table name to distinguish between external and internal tables as explained below. 
		   */
            TableName tableNameB = currentSplit.getTable();
            String tableName = tableNameB.getQualifierAsString();

            // We create a string as follows for each key: tableName#key;family:attributeValue
            String tuple = tableName + "#" + new String(rowMetadata.get(), "US-ASCII");

            KeyValue[] attributes = values.raw();
            String value = null;
            for (i = 0; i < attributes.length; i++) {
                if (new String(attributes[i].getQualifier()).equals(groupByAttribute[0]))
                    value = new String(attributes[i].getValue());

                tuple = tuple + ";" + new String(attributes[i].getFamily()) + ":" + new String(attributes[i].getQualifier()) + ":" + new String(attributes[i].getValue());
            }
            if (value != null) {
                //This writes a key-value pair to the context object
                //If it is external, it gets as key a hash value and it is written only once in the context object
                context.write(new Text(value), new Text(tuple));
            }
            //Is this key external (e.g., from the external table)?
        }
    }

    //================================================================== Reducer
    public static class Reducer extends TableReducer<Text, Text, Text> {

          /* The reduce is automatically called by the MapReduce framework after the Merge Sort step. 
          It receives a key, a list of values for that key, and a context object where to write the resulting key-value pairs. */

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            int i, k;
            Put put;
            String tableTuple;
            String tuple;
            String[] aggregatte = context.getConfiguration().getStrings("aggA", "Default");
            String[] attributes;
            String[] attribute_value;

            //All tuples with the same hash value are stored in a vector
            Vector<String> tuples = new Vector<String>();
            for (Text val : inputList) {
                tuples.add(val.toString());
            }
            Integer value = 0;
            String family = null;
            //In this for, each internal tuple is joined with each external tuple
            //Since the result must be stored in a HBase table, we configure a new Put, fill it with the joined data and write it in the context object
            for (i = 0; i < tuples.size(); i++) {
                tableTuple = tuples.get(i);
                // we extract the information from the tuple as we packed it in the mapper
                tuple = tableTuple.split("#")[1];
                attributes = tuple.split(";");

                //Set the values for the columns of the external table
                for (k = 1; k < attributes.length; k++) {
                    attribute_value = attributes[k].split(":");
                    if (attribute_value[1].equals(aggregatte[0])) {
                        value += Integer.valueOf(attribute_value[2]);
                        family = attribute_value[0];
                    }

                }

                // Put the tuple in the output table through the context object
            }
            assert family != null;
            put = new Put(key.toString().getBytes());
            put.addColumn( family.getBytes(), aggregatte[0].getBytes(),value.toString().getBytes());
            context.write(new Text(key), put);
        }
    }
}


