package com.hbase.tools.hfile;

import com.hbase.tools.hfile.beans.InterfaceLoaderBean;
import com.netease.urs.rc.util.HashUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

/**
 * Created by hzxijingjing on 2016/06/15
 */
@Deprecated
public class HFileProducer extends Configured implements Tool {

    private static final byte[] CF = Bytes.toBytes("d");
    public static final String CLASS_NAME="mapreduce.class.name";
    public static final String DELIMITER_STR="mapreduce.delimeter.name";
    public static final String SKIP_STR="mapreduce.skips.name";

    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("i", "inputPath", true, "input path.");
        options.addOption("o", "outputPath", true, "output path.");
        options.addOption("t", "table", true, "hbase table");
        options.addOption("r", "reducer", true, "number of reducer.");
        options.addOption("h", "help", false, "Print usage.");
        options.addOption("D", "delimiter", false, "delimiter charater");
        options.addOption("skip", "skip", false, "skip index");
        options.addOption("c", "class", true, "class name");

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
        String className = "com.hbase.beans."+commandLine.getOptionValue("class");
        String delimiter = commandLine.getOptionValue("delimiter","\t");
        String skips = commandLine.getOptionValue("skip","9999");

        /**
         *  获取HBase配置信息
         */
        Configuration conf = HBaseConfiguration.create();

        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.job.reduces", numReducers);
        conf.set(CLASS_NAME,className);
        conf.set(DELIMITER_STR,delimiter);
        conf.set(SKIP_STR,skips);
        FileSystem fs = FileSystem.get(conf);

        /**
         * Test
         */
        System.out.println("zk url ===== " + conf.get("hbase.zookeeper.quorum"));
        System.out.println("zk znode ===== " + conf.get("zookeeper.znode.parent"));

        /**
         * job Configure
         */
        Job job = Job.getInstance(conf,"HFileProducer");
        job.setJarByClass(HFileProducer.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(HFileProducer.class);
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
        HFileOutputFormat2.configureIncrementalLoad(job,htable);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    private static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] words = line.split(context.getConfiguration().get(DELIMITER_STR));
            String[] skips = context.getConfiguration().get(SKIP_STR).split(",");
            String rowKeyString = HashUtils.hashString(words[0], HashUtils.HashAlgorithm.MD5);
            byte[] rowKey=Bytes.toBytes(rowKeyString);
            // rowKey 赋值
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);

            Class c = null;
            InterfaceLoaderBean bean = null;
            try {
                c = Class.forName(context.getConfiguration().get(CLASS_NAME));
                bean= (InterfaceLoaderBean)c.newInstance();
                String cols[] = bean.getColumnNames();
                for (int i = 0; i < bean.getColumnNumbers(); i++) {
                    // KeyValue 赋值
                    if (cols.length==bean.getColumnNumbers() && words.length==bean.getColumnNumbers()){
                        // 如果没有跳过参数
                        for (int j = 0;j < skips.length; j++) {
                            if (i!=Integer.parseInt(skips[j])){
                                KeyValue kv = new KeyValue(rowKey, CF, Bytes.toBytes(cols[i]), Bytes.toBytes(words[i]));
                                context.write(rowKeyWritable,kv);
                            }
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e){
                e.printStackTrace();
            }catch (IllegalAccessException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        int run = ToolRunner.run(new HFileProducer(), args);
        if (run !=0){
            System.out.println("\n####################Job End####################");
            System.out.println("\n\t\tJob Failed,RC="+run+"\t\t\n");
            System.exit(run);
        }
    }
}
