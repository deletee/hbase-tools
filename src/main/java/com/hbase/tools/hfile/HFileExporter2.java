package com.hbase.tools.hfile;

import com.hbase.tools.hfile.beans.InterfaceExportBean;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by hzxijingjing on 2016/06/15
 */
@Deprecated
public class HFileExporter2 extends Configured implements Tool {
    private LinkedList<Path> hFileList = new LinkedList();
    private static final String MAPPER_COUNTER_GROUP = "Mapper Stat";
    private static final String MAPPER_TABLE_PATHS = "mapper.table.paths";
    private static final String HBASE_DIR = "/hbase-hz/data/hz/";
    public static final String MAPPER_HBASE_DIR = "mapper.hbase.dir";
    public static final String TABLE_NAME="mapreduce.table.name";
    public static final String CLASS_NAME="mapreduce.class.name";
    private String tableName;
    private Configuration conf;

    public  void setConf(Configuration conf,String tableName){
        this.conf = conf;
        this.tableName = tableName;
    }
    public int run(String[] args) throws Exception {

        /**
         * 参数解析
         */
        Options options = new Options();
        options.addOption("t", "table", true, " export table");
        options.addOption("m", "map", true, "number of map.");
        options.addOption("d", "dir", true, " hbase table dir");
        options.addOption("o", "outPath", true, "output path");
        options.addOption("c", "class", true, "class name");
        options.addOption("h", "help", false, "Print usage.");

        GnuParser parser = new GnuParser();
        CommandLine commandLine = null ;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        /**
        *  获取外界参数
        */
        int numMppers = Integer.parseInt(commandLine.getOptionValue("map", "20"));
        String hBaseDir = commandLine.getOptionValue("dir",HBASE_DIR);
        Path inPutPath = new Path("tmp/input");
        Path outPutPath = new Path(commandLine.getOptionValue("outPath"));
        tableName = commandLine.getOptionValue("table");
        String className = "com.netease.hz.rc.mr.hbase.beans."+commandLine.getOptionValue("class");

        /**
         *  获取HBase配置信息
         */
        Configuration conf = HBaseConfiguration.create();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set(MAPPER_HBASE_DIR,hBaseDir);
        conf.set(CLASS_NAME,className);

        /**
         * Test
         */
        System.out.println("zk url ===== " + conf.get("hbase.zookeeper.quorum"));
        System.out.println("zk znode ===== " + conf.get("zookeeper.znode.parent"));

        /**
         * get hTable's HFile List
         */

        HFileExporter2 hFileExporter2 = new HFileExporter2();
        hFileExporter2.setConf(conf,tableName);

        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(inPutPath)) {
            fs.create(inPutPath);
        }
        Path inPutFile = new Path("tmp/input/0000000");
        FSDataOutputStream outStream = fs.create(inPutFile);

        byte[] buff = hFileExporter2.getPathList().replace(',','\n').getBytes();
        outStream.write(buff,0,buff.length);
        outStream.close();

        /**
         * job Settings
         */
        conf.set(MAPPER_TABLE_PATHS,hFileExporter2.getPathList());
        conf.set(TABLE_NAME,tableName);
        Job job = Job.getInstance(conf, "HFileExporter2");
        job.setJarByClass(HFileExporter2.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setMapperClass(TaskMapper.class);
        job.setReducerClass(TaskReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(numMppers);
        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    public String getPathList(){

        FileSystem fs;
        HTable table ;
        HTableDescriptor desc ;
        HColumnDescriptor[] hcd = null;
        String tablePath ;
        try {
            fs = FileSystem.get(this.conf);
            table = new HTable(this.conf, this.tableName);
            desc = table.getTableDescriptor();
            hcd = desc.getColumnFamilies();

            if (this.tableName.indexOf(":")!=-1){
                tablePath = this.conf.get(MAPPER_HBASE_DIR) + this.tableName.split(":")[1];
            }else{
                tablePath = this.conf.get(MAPPER_HBASE_DIR) + this.tableName;
            }
            for (int i=0;i<hcd.length;i++){
                this.getPathList(fs, tablePath, Bytes.toString(hcd[i].getName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Iterator<Path> it = this.hFileList.iterator();
        String pathList = "";
        while(it.hasNext()) {
            Path path = it.next();
            pathList+=","+path.toString();
        }

        return pathList.substring(1);
    }
    public void getPathList(FileSystem fs, String path, String cf) throws IOException {
        FileStatus[] fList = fs.listStatus(new Path(path));
        for(int i = 0; i < fList.length; i++) {
            if(fList[i].getPath().toString().length() > 60) {
                FileStatus[] subPathList = fs.listStatus(new Path(fList[i].getPath().toString() + "/" + cf));
                for(int j = 0; j < subPathList.length; j++) {
                    this.hFileList.add(subPathList[j].getPath());
                }
            }
        }
    }

    private static class TaskMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) {

            HTable table ;
            HTableDescriptor desc ;
            HColumnDescriptor[] hcd = null;
            try {
                table = new HTable(context.getConfiguration(), context.getConfiguration().get(TABLE_NAME));
                desc = table.getTableDescriptor();
                hcd = desc.getColumnFamilies();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < hcd.length; i++) {

                export(new Path(value.toString()),hcd[i],context);
                context.getCounter(MAPPER_COUNTER_GROUP, "Reduce Success Write").increment(1);
            }
        }

        public void export(Path path,HColumnDescriptor hcd,Context context){

            System.out.println("export(Path path,HColumnDescriptor hcd,String className,Context context)");

            Configuration conf = context.getConfiguration();
            FileSystem fs = null;
            HFile.Reader hreader = null;
            try {
                fs = FileSystem.get(conf);
                hreader = HFile.createReader(fs, path, new CacheConfig(conf,hcd),conf);
                hreader.loadFileInfo();
                HFileScanner e = hreader.getScanner(false, false);
                while((!e.isSeeked() && e.seekTo()) || e.next()) {
                    System.out.println("e.getKeyString()"+e.getValueString());
                    KeyValue kv = (KeyValue)e.getKeyValue();
                    System.out.println("key="+Bytes.toString(kv.getRow())+":"+Bytes.toString(kv.getQualifier())+"="+Bytes.toString(kv.getValue()));
                    context.write(new Text(Bytes.toString(kv.getRow())),new Text(Bytes.toString(kv.getQualifier())+"="+Bytes.toString(kv.getValue())));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TaskReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException {
            Class c = null;
            InterfaceExportBean bean = null;
            try {
                c = Class.forName(context.getConfiguration().get(CLASS_NAME));
                bean= (InterfaceExportBean)c.newInstance();
                bean.setBean(values);
                System.out.println(key.toString()+"bean.toString()==========="+bean.toString());
                context.write(new Text(key.toString()),new Text(bean.toString()));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e){
                e.printStackTrace();

            }catch (IllegalAccessException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public static void main(String[] args) throws Throwable {

        int run = ToolRunner.run(new HFileExporter2(), args);
        if (run !=0){
            System.out.println("MapReduce Error");
            System.exit(run);
        }
    }

}
