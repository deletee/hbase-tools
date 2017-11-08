package com.hbase.tools.hlog;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by hzxijingjing on 16/07/06.
 */

/**
 *   用于解析
 */
@Deprecated
public class HLogExporter {
    private static final String HBASE_DIR = "/hbase-hz/oldWALs/";
    private static final String WAL_ENCRYPTION = "hbase.regionserver.wal.encryption";
    public static void main(String[] args) throws IOException {

        /**
         * 参数解析
         */
        Options options = new Options();
        options.addOption("d", "dir", true, " hbase table dir");
        options.addOption("p", "path", true, " path");
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
        final byte[] value = Bytes.toBytes("Test value");
        String path1 = commandLine.getOptionValue("path","/hbase-hz/oldWALs/hz-hbase9.bjyz.163.org%2C60020%2C1468577832462.default.1469558285662");

        Configuration conf = HBaseConfiguration.create();

//        ProtobufLogWriter
//        SequenceFileLogReader sequenceFileLogReader = new SequenceFileLogReader();
//        SequenceFileLogReader sequenceFileLogReader = new SequenceFileLogReader();
        ProtobufLogReader sequenceFileLogReader = new ProtobufLogReader();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(path1);
        FSDataInputStream fsDataInputStream = null;
        sequenceFileLogReader.init(fs,path,conf,fsDataInputStream);

            System.out.println("开始读取了");
            WAL.Entry entry = sequenceFileLogReader.next();
            WALKey walKey= entry.getKey();
            WALEdit walEdit = entry.getEdit();
            System.out.println(walKey.getTablename());
            ArrayList<Cell> li = walEdit.getCells();
            for(int i=0;i<li.size();i++){
                System.out.println(Bytes.toString(li.get(i).getQualifier())+"="+Bytes.toString(li.get(i).getValue()));
            }
            System.out.println("读取结束");
    }

}
