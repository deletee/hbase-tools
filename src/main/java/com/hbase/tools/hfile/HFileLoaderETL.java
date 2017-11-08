package com.hbase.tools.hfile;

import com.hbase.tools.util.DefConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * Created by hzxijingjing on 2016/06/15
 */

/**
 *  该程序需要使用HBase对应的hadoop执行,要求HFile在HBase对应的hadoop上
 */
public class HFileLoaderETL {

    public static void main(String[] args) throws Throwable {
        /**
         * 参数解析
         */
        Options options = new Options();
        options.addOption("t", "table", true, " export table");
        options.addOption("p", "path", true, "table hFile path");
        options.addOption("E", "hbaseEnv", true, "hbaseEnv");
        options.addOption("h", "help", false, "Print usage.");

        GnuParser parser = new GnuParser();
        CommandLine commandLine = null ;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        /**
         * 获取参数
         */

        Path hFilePath = new Path(commandLine.getOptionValue("path"));
        String tableName = commandLine.getOptionValue("table");
        String hbaseEnv = commandLine.getOptionValue("hbaseEnv","ol");

        Configuration conf = DefConfiguration.GetConfiguration(hbaseEnv);

        HTable htable =new HTable(conf, tableName);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(hFilePath, htable);
    }
}
