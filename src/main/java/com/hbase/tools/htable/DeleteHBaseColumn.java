package com.hbase.tools.htable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by hzxijingjing on 15/12/20.
 */
public class DeleteHBaseColumn extends Configured implements Tool {

    public static final String MAPPER_DELETE_COUNTS = "Mapper Delete Counts";
    public static final String CONF_TABLE_NAME = "tableName";
    public static final String CONF_COLUMNS = "columns";
    public static final String CONF_TIMESTAMP = "timestamp";

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("t", "table", true, "need to be deleted table name, eg, test");
        options.addOption("c", "columns", true, "need to be deleted columns, eg, co1,co2,co3");
        options.addOption("d", "date", true, "versions before this time will be deleted. eg. 2015-11-22 00:00:00");
        options.addOption("h", "help", false, "Print usage.");

        GnuParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("help")) {
            new HelpFormatter().printHelp("job", options);
            System.exit(-1);
        }

        String tableName = commandLine.getOptionValue("table");
        String columns = commandLine.getOptionValue("columns");
        if (tableName == null || columns == null) {
            System.err.println("arg table or columns invalid.");
            new HelpFormatter().printHelp("job", options);
            System.exit(-1);
        }

        long timestamp = 0;
        String time = commandLine.getOptionValue("date");
        if(time == null) {
            timestamp = System.currentTimeMillis(); // default: current time
        } else {
            try {
                Date date = dateFormat.parse(time);
                timestamp = date.getTime();
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }

        }
        System.out.println("Delete table: " + tableName + ". columns: " + columns + ". timestamp: " + dateFormat.format(timestamp));

        Configuration conf = HBaseConfiguration.create();
        System.out.println("zk url ===== " + conf.get("hbase.zookeeper.quorum"));
        System.out.println("zk znode ===== " + conf.get("zookeeper.znode.parent"));

        conf.set(CONF_TABLE_NAME, tableName);
        conf.set(CONF_COLUMNS, columns);
        conf.set(CONF_TIMESTAMP, String.valueOf(timestamp));

        Job job = Job.getInstance(conf, "DeleteHBaseColumn");
        job.setJarByClass(DeleteHBaseColumn.class);

        Scan scan = new Scan();
        scan.setCaching(1);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
        //      UserInfoHBaseConsts.TABLE_NAME,        // input HBase table name
                tableName,
                scan,             // Scan instance to control CF and attribute selection
                TaskMapper.class,   // mapper
                null,             // mapper output key
                null,             // mapper output value
                job);
        job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from
        job.setNumReduceTasks(0);
        //TableMapReduceUtil.initTableReducerJob(UserInfoHBaseConsts.TABLE_NAME, null, job);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    public static class TaskMapper extends TableMapper<ImmutableBytesWritable, NullWritable>{

        private HTable myTable;
        private List<Delete> deleteList = new ArrayList<Delete>();
        final private int buffer = 50; /* Buffer size, tune it as desired */
        private String[] columns;
        private long timestamp;

        protected void setup(Context context) throws IOException, InterruptedException {
        /* HTable instance for deletes */
            String table = context.getConfiguration().get(CONF_TABLE_NAME);
            columns = context.getConfiguration().get(CONF_COLUMNS).split(",");
            timestamp = Long.parseLong(context.getConfiguration().get(CONF_TIMESTAMP));
            myTable = new HTable(context.getConfiguration(), table.getBytes());
        }

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            Delete delete = new Delete(row.get());
            for(String c: columns) {
                delete.addColumns("d".getBytes(), c.getBytes(), timestamp); // delete all versions earlier than this timestamp
            }
            deleteList.add(delete); /* Add delete to the batch */
            if (deleteList.size()==buffer) {
                myTable.delete(deleteList); /* Submit batch */
                deleteList.clear(); /* Clear batch */
                System.out.println("---------->" + Bytes.toString(row.get()));
                Thread.sleep(1000);
            }
            context.getCounter(MAPPER_DELETE_COUNTS, "del_columns").increment(1);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (deleteList.size()>0) {
                myTable.delete(deleteList); /* Submit remaining batch */
            }
            myTable.close(); /* Close table */
        }

    }
/*
    // This method can't delete the column, why???
    private static class TaskMapper extends TableMapper<ImmutableBytesWritable, Delete> {
        static int count=0;
        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

            Delete delete = new Delete(row.get());
            delete.addColumns("d".getBytes(), "ssn".getBytes());
            if (count % 1000 == 0) {
                System.out.println("---------->" + Bytes.toString(row.get()));
            }
            count ++;
            context.getCounter("mapper", "del_column").increment(1);
            context.write(row, delete);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("set up mapper");
            super.setup(context);
        }
    }
*/
    public static void main(String[] args) throws Throwable {
        System.exit(ToolRunner.run(new DeleteHBaseColumn(), args));
    }
}

