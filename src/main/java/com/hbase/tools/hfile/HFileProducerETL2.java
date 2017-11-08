package com.hbase.tools.hfile;

import com.hbase.tools.util.DefConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by hzxijingjing on 2016/06/15
 */
public class HFileProducerETL2 extends Configured implements Tool {

    private static final byte[] CF = Bytes.toBytes("d");
    public static final String FILE_DELIMITER_STR="mapreduce.delimeter.name";
    public static final String EXPORT_COLUMNS="mapreduce.export.columns";
    public static final String EXPORT_COLUMNS_INDEX="mapreduce.export.columns.index";
    public static final String ROW_KEY_COLUMN_INDEX="mapreduce.rowkey.column.index";
    public static final String TIMESTAMP_COLUMN_INDEX="mapreduce.timestamp.column.index";
    public static final int TIMESTAMP_COLUMN_DEFAULT_INDEX=-1;
    public static final int ROW_KEY_DEFAULT_INDEX=-2;
    public static final String NONE_COLUMN="__None__";

    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("i", "inputPath", true, "input path.");
        options.addOption("o", "outputPath", true, "output path.");
        options.addOption("t", "table", true, "hbase table");
        options.addOption("r", "reducer", true, "number of reducer.");
        options.addOption("a", "all", true, "all table columns");
        options.addOption("e", "exclude", true, "exclude table columns");
        options.addOption("c", "include", true, "include table columns");
        options.addOption("D", "split", true, "split character");
        options.addOption("T", "time", true, "timestamp column");
        options.addOption("E", "hbaseEnv", true, "hbaseEnv");
        options.addOption("k", "rowKey", true, "rowKey column index");
        options.addOption("h", "help", false, "Print usage.");



        GnuParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("help")) {
            new HelpFormatter().printHelp("job", options);
            System.exit(-1);
        }

        /**
         *  获取外界参数
         */
        String numReducers = commandLine.getOptionValue("reducer", "10");
        Path inputPath = new Path(commandLine.getOptionValue("inputPath"));
        Path outputPath = new Path(commandLine.getOptionValue("outputPath"));
        String tablename = commandLine.getOptionValue("table");
        String allColumns = commandLine.getOptionValue("all","");
        String hbaseEnv = commandLine.getOptionValue("hbaseEnv","ol");

        String timestamp_column = commandLine.getOptionValue("time",NONE_COLUMN);

        if (!timestamp_column.equals(NONE_COLUMN) && !allColumns.contains(timestamp_column)){
            System.err.println("timestamp字段不在所有字段内，请检查");
            System.exit(-1);
        }

        /**
         * 判断传入的参数 是否所有字段是否为空
         */
        if(allColumns.trim().equals("")){
            System.err.println("缺少参数值:--all-column");
            return -1;
        }
        String includeColumns = commandLine.getOptionValue("include","");  // 排除与包含二选一
        String excludeColumns = commandLine.getOptionValue("exclude","");

        /**
         * 包含字段和排除字段不能同时为空
         */

        if (!includeColumns.trim().equals("") && !excludeColumns.equals(""))
        {
            System.err.println("包含字段和排除字段不能同时存在");
            return -1;
        }

        /**
         * 排除字段和包含字段均为空,则为默认,导出所有字段
         */
        char[] exportIndex  = new char[allColumns.split(",").length];
        if (includeColumns.trim().equals("") && excludeColumns.equals("")){
            exportIndex = minus(allColumns.split(","),allColumns.split(","),true);
        }

        /**
         * 如果包含字段为空,排除字段不为空
         */

        if (includeColumns.trim().equals("") && !excludeColumns.equals("")){
            exportIndex = minus(allColumns.split(","),excludeColumns.split(","),false);
        }

        /**
         * 如果包含字段不为空,排除字段为空
         */

        if (!includeColumns.trim().equals("") && excludeColumns.equals("")){
            exportIndex = minus(allColumns.split(","),includeColumns.split(","),true);
        }

        String rowKey = commandLine.getOptionValue("rowKey","");

        String all_column_name[] = allColumns.split(",");

        int rowKeyColumnIndex = ROW_KEY_DEFAULT_INDEX;
        int timestampColumnIndex = TIMESTAMP_COLUMN_DEFAULT_INDEX;

        /**
         * 获取rowkey序号以及timestamp序号
         */
        for (int i = 0;i< all_column_name.length;i++){
            if (all_column_name[i].equals(rowKey)){
                rowKeyColumnIndex = i;
                exportIndex[i] = '0';
            }
            if (all_column_name[i].equals(timestamp_column)){
                timestampColumnIndex = i;
                exportIndex[i] = '0';
            }
        }

        String delimiter = commandLine.getOptionValue("split","\001");

        /**
         *  获取HBase配置信息
         */

        Configuration conf = DefConfiguration.GetConfiguration(hbaseEnv);
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.job.reduces", numReducers);

        /**
         * 自定义传递参数
         */
        conf.set(EXPORT_COLUMNS_INDEX,String.valueOf(exportIndex));
        conf.set(EXPORT_COLUMNS,allColumns);
        conf.setInt(ROW_KEY_COLUMN_INDEX, rowKeyColumnIndex);
        conf.setInt(TIMESTAMP_COLUMN_INDEX, timestampColumnIndex);

        conf.set(FILE_DELIMITER_STR,delimiter);

//        System.out.println(EXPORT_COLUMNS+"="+allColumns);
//        System.out.println(EXPORT_COLUMNS_INDEX+"="+String.valueOf(exportIndex));
//        System.out.println(ROW_KEY_COLUMN_INDEX+"="+rowKeyColumnIndex);
        FileSystem fs = FileSystem.get(conf);

        /**
         * Job Configure
         */
        Job job = Job.getInstance(conf,"HFileProducerETL");
        job.setJarByClass(HFileProducerETL2.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setMapperClass(BulkLoadMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        HTable htable =new HTable(conf, tablename);
        HFileOutputFormat2.configureIncrementalLoad(job,htable,htable);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    private static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private static String[] all_column_name;
        private static char[] export_column_index;
        private static int rowKeyIndex;
        private static int timestampIndex;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            all_column_name = context.getConfiguration().get(EXPORT_COLUMNS).split(",");
            export_column_index = context.getConfiguration().get(EXPORT_COLUMNS_INDEX).toCharArray();
            rowKeyIndex = Integer.parseInt(context.getConfiguration().get(ROW_KEY_COLUMN_INDEX));
            timestampIndex = Integer.parseInt(context.getConfiguration().get(TIMESTAMP_COLUMN_INDEX));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString();
                String[] all_column_values = line.split(context.getConfiguration().get(FILE_DELIMITER_STR));
                String rowKeyString = all_column_values[rowKeyIndex];
                byte[] rowKey=Bytes.toBytes(rowKeyString);

                // rowKey 赋值
                ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);

                if (all_column_name.length!=all_column_values.length){
                    return;
                }
                for (int i = 0; i < all_column_name.length; i++) {
                    if (export_column_index[i] == '1' && !all_column_values[i].trim().equals("") && !all_column_values[i].trim().equals("\\N")){
                        KeyValue kv ;
                        if (timestampIndex == TIMESTAMP_COLUMN_DEFAULT_INDEX){
                            kv = new KeyValue(rowKey, CF, Bytes.toBytes(all_column_name[i]),Bytes.toBytes(all_column_values[i]));
                        }
                        else{
                            Long timestamp_value = Long.parseLong(all_column_values[timestampIndex]);
                            kv = new KeyValue(rowKey, CF, Bytes.toBytes(all_column_name[i]),timestamp_value,Bytes.toBytes(all_column_values[i]));
                        }
                        context.write(rowKeyWritable,kv);
                    }
                }
            }catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        int run = ToolRunner.run(new HFileProducerETL2(), args);
        if (run !=0){
            System.out.println("\n####################Job End####################");
            System.out.println("\n\t\tJob Failed,RC="+run+"\t\t\n");
            System.exit(run);
        }
    }
    public static char[] minus(String[] arr1, String[] arr2 ,boolean isInclude) {
        LinkedList<String> list = new LinkedList<String>();
        String[] longerArr = arr1;
        String[] shorterArr = arr2;
        char index[] = new char[arr1.length];

        /**
         * 初始化
         */
        char includeValue;
        char excludeValue;
        if(isInclude){  //来的是 include
            includeValue = '0';
            excludeValue = '1';
        }else{ //来的是 exclude
            includeValue = '1';
            excludeValue = '0';
        }
        for (int i =0;i<index.length;i++){
            index[i] = includeValue;
        }
        //找出较长的数组来减较短的数组
        if (arr1.length > arr2.length) {
            longerArr = arr2;
            shorterArr = arr1;
        }
        for (String str : longerArr) {
            if (!list.contains(str)) {
                list.add(str);
            }
        }

        for (int i=0;i<shorterArr.length;i++){
            if (list.contains(shorterArr[i])){
                index[i]=excludeValue;
            }
        }
        return index;
    }
}
