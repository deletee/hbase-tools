package com.hbase.tools.hlog;

import com.hbase.tools.util.DefConfiguration;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.LzopCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hzxijingjing on 16/07/06.
 */

/**
 *   用于解析
 */

public class HLogParserETL extends Configured implements Tool {

    private static final String MAPPER_HLOG_PATHS = "mapper.hlog.paths";
    private static final String MAPPER_TABLE_NAME = "mapper.table.name";
    private static final String DW_ODS_PATH="/hz/dc/ods";
    private static final String CTL_PATH="/hz/dc/ctl/mr/input";
    private static final String TAR_LOAD_COLUMNS="mapreduce.load.columns";
    private static final String SPLIT_CHAR=",";
    private static final String MAPPER_COUNTER_PATH = "Mapper path Stat";
    private static final String MAPPER_COUNTER_GROUP = "Mapper Stat";
    private static final String REDUCER_COUNTER_GROUP = "Reducer Stat";

    //入口函数
    public int run(String[] args) throws Exception {

        /**
         * 参数解析
         */
        Options options = new Options();
        options.addOption("p", "distcppath", true, "after distcp path");
        options.addOption("s", "sourcetable", true, "hbase table name");
        options.addOption("t", "targettable", true, "hive table name");
        options.addOption("a", "all", true, "all table columns");
        options.addOption("d", "date", true, "data time");
        options.addOption("r", "reduce", true, "reduce numbers");
        options.addOption("E", "hbaseEnv", true, "hbaseEnv");
        options.addOption("h", "help", false, "Print usage.");

        GnuParser parser = new GnuParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (commandLine.hasOption("help")) {
            new HelpFormatter().printHelp("job", options);
            System.exit(-1);
        }
        String hbaseEnv = commandLine.getOptionValue("hbaseEnv","ol");

        /**
         * 获取外界参数
         */
        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        Date d=cal.getTime();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); //设置日期格式
        String ETL_DT =  commandLine.getOptionValue("date",df.format(d));
        String after_discp_path =  commandLine.getOptionValue("distcppath");
        String hbase_table_name =  commandLine.getOptionValue("sourcetable");
        String allColumns = commandLine.getOptionValue("all","");
        if(!hbase_table_name.startsWith("hz:")){
            hbase_table_name="hz:"+hbase_table_name;
        }
        String hive_table_name =  commandLine.getOptionValue("targettable");
        String hive_table_theme = hive_table_name.split("_")[1];
        String default_tar_path = String.format("%s/%s/%s/%s", DW_ODS_PATH,hive_table_theme, hive_table_name,ETL_DT);


        int reduce = Integer.parseInt(commandLine.getOptionValue("reduce","300"));

        /**
         *
         */
        Configuration conf = DefConfiguration.GetConfiguration(hbaseEnv);
        FileSystem fs = FileSystem.get(conf);
        Path tar_path = new Path(default_tar_path);
        if (fs.exists(tar_path)){
            fs.delete(tar_path,true);
        }

        if (!fs.exists(new Path(CTL_PATH))){
            fs.create(new Path(CTL_PATH));
        }

        /**
         * 将HLog的路径生成到多个文件当中
         */
        String pathList = HLogParserETL.getPathList(fs,after_discp_path);
        String pathArr[] = pathList.split(",");
        int per = 20;
        int groupIndex = 0;
        int curIndex = 0;
        for (int i = 0; i*per < pathArr.length; i++) {
            String tmpPathArr = "";
            for (int j = 0; j < per && i*per +j < pathArr.length; j++) {
                tmpPathArr += pathArr[i*per + j]+",";
                curIndex = j;
            }
            byte[] buff = tmpPathArr.replace(',','\n').getBytes();
            Path ctlInPutFile;
            if (i<10){
                ctlInPutFile = new Path(CTL_PATH+"/part-00000"+i);
            }else{
                ctlInPutFile = new Path(CTL_PATH+"/part-0000"+i);
            }
            FSDataOutputStream outStream = fs.create(ctlInPutFile,true);
            outStream.write(buff,0,buff.length);
            outStream.close();
            groupIndex = i;

        }
        /**
         * 最后一部分路径写入到新文件
         */
        if (groupIndex*per+curIndex+1<pathArr.length){
            String tmpPathArr = "";
            groupIndex += 1;
            for (int j = 0 ; j + groupIndex*per < pathArr.length; j++) {
                tmpPathArr += pathArr[j]+",";
            }
            byte[] buff = tmpPathArr.replace(',','\n').getBytes();
            Path ctlInPutFile ;
            if (groupIndex<10){
                ctlInPutFile = new Path(CTL_PATH+"/part-00000"+(groupIndex+1));
            }else{
                ctlInPutFile = new Path(CTL_PATH+"/part-0000"+(groupIndex+1));
            }
            FSDataOutputStream outStream = fs.create(ctlInPutFile,true);
            outStream.write(buff,0,buff.length);
            outStream.close();
        }


        conf.set(MAPPER_HLOG_PATHS,pathList);
        conf.set(MAPPER_TABLE_NAME,hbase_table_name);
        conf.set(TAR_LOAD_COLUMNS,allColumns);
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("mapred.textoutputformat.separator", SPLIT_CHAR);     //设置输出分隔符
        Job job = Job.getInstance(conf, "HLogParserETL");
        job.setJarByClass(HLogParserETL.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setMapperClass(TaskMapper.class);
        job.setReducerClass(TaskReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reduce);
        FileInputFormat.setInputPaths(job, CTL_PATH);
        FileOutputFormat.setOutputPath(job, new Path(default_tar_path));
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    public static String getPathList(FileSystem fs, String path) throws IOException {
        LinkedList<Path> hLogList = new LinkedList();
        FileStatus[] fList = fs.listStatus(new Path(path));
        for(int i = 0; i < fList.length; i++) {
            hLogList.add(fList[i].getPath());
        }
        Iterator<Path> it = hLogList.iterator();
        String pathList = "";
        while(it.hasNext()) {
            pathList+=it.next().toString()+",";
        }
        return pathList.substring(0,pathList.length()-1); /**除去最后一个逗号*/
    }

    public static void main(String[] args) throws Throwable {
        int run = ToolRunner.run(new HLogParserETL(), args);
        if (run !=0){
            System.out.println("\n####################Job End####################");
            System.out.println("\n\t\tJob Failed,RC="+run+"\t\t\n");
            System.exit(run);
        }
    }

    private static class TaskMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().equals(""))
                context.getCounter(MAPPER_COUNTER_PATH, "Success Write").increment(1);
            ParseHLog(context,value.toString());
        }

        public void ParseHLog(Context context, String path){
            Configuration conf = context.getConfiguration();
            String src_table_name = conf.get(MAPPER_TABLE_NAME);
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
                ProtobufLogReader sequenceFileLogReader = new ProtobufLogReader();
                FSDataInputStream fsDataInputStream = null;
                sequenceFileLogReader.init(fs,new Path(path),conf,fsDataInputStream);
                WAL.Entry entry = sequenceFileLogReader.next();
                while(entry!=null){
                    WALKey walKey= entry.getKey();
                    WALEdit walEdit = entry.getEdit();
                    if (walKey.getTablename().toString().equals(src_table_name)){
                        ArrayList<Cell> li = walEdit.getCells();
                        for (Cell cell:li) {

                            /**
                             * 新的Key是 Row
                             */
                            String newKey = Bytes.toString(cell.getRow());
                            /**
                             * 新的Value是 由 字段名+值+时间戳
                             */
                            String newValue;
                            if (!Bytes.toString(cell.getQualifier()).startsWith("HBASE")){  //HLog中出现 HBASE::FLUSH，HBASE::FLUSH 的column
                                newValue  = Bytes.toString(cell.getQualifier())+SPLIT_CHAR+Bytes.toString(cell.getValue()).replaceAll(",",";")+SPLIT_CHAR+String.valueOf(cell.getTimestamp());
                                context.write(new Text(newKey),new Text(newValue));
                                context.getCounter(MAPPER_COUNTER_GROUP, "Success Write").increment(1);
                            }
                        }
                    }
                    entry = sequenceFileLogReader.next();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TaskReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            /**
             * run before init
             */
            Configuration conf = context.getConfiguration();
            String column_names[] = conf.get(TAR_LOAD_COLUMNS).split(",");
            String column_values[] = new String[column_names.length];
            for (int i=0;i<column_names.length;i++){
                column_values[i] = "";
            }
            /**
             * Start for loop
             */
            Map<String,String> map = new HashMap<String,String>();

            for (Text value : values) {
                String valueArr[] = value.toString().split(SPLIT_CHAR);
                String Qualifier = valueArr[0];
                String cur_Value = valueArr[1];
                Long cur_Timestamp = 0L;
                try {
                    cur_Timestamp = Long.parseLong(valueArr[2]);
                }catch (NumberFormatException e){
                    System.out.println("Reduce-Exception="+value);
                }

                /**
                 *  先判断 key是否存在,如果存在则判断时间戳更大的则为当前值,key 则为 字段, value 则为时间戳 and 值
                 */
                if (!map.containsKey(Qualifier)){  /**不存在 key,则加入*/
                    map.put(Qualifier,cur_Timestamp+SPLIT_CHAR+cur_Value);
                }else{  /**存在,比较时间先后*/

                    /**
                     * 已经存在的值
                     */
                    String pre_Arr[] = map.get(Qualifier).split(SPLIT_CHAR);
                    long pre_Timestamp = Long.parseLong(pre_Arr[0]);

                    //当前时间 大于 已经存储在map中的时间
                    if (cur_Timestamp > pre_Timestamp){
                        map.put(Qualifier,cur_Timestamp+SPLIT_CHAR+cur_Value);
                    }
                }
            }

            /**
             * End for loop
             */

            /**
             * put value
             */
            /**第一个是key，过滤掉*/
            for (int i = 1; i < column_values.length; i++) {
                if (map.containsKey(column_names[i])){
                    try{
                        column_values[i] = map.get(column_names[i]).split(SPLIT_CHAR)[1];
                    }catch(ArrayIndexOutOfBoundsException e){
                        System.out.println("Reduce-Value="+map.get(column_names[i]).split(SPLIT_CHAR));
                    }

                }
            }
            String arr2value = Arrays.toString(column_values).replace(", ",SPLIT_CHAR);
            arr2value = arr2value.substring(2,arr2value.length()-1);

            /**
             * 写入一条记录
             */
            context.getCounter(REDUCER_COUNTER_GROUP, "Success Write").increment(1);
            context.write(key,new Text(arr2value));
        }
    }
}
