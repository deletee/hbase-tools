package com.hbase.tools.util;

import org.apache.hadoop.conf.Configuration;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by hzxijingjing on 16/10/13.
 */
public class DefConfiguration {
    public static Configuration GetConfiguration(String hbaseEnv){
        Configuration conf = new Configuration();
        Properties prop = new Properties();
        try {
            prop.load(DefConfiguration.class.getResourceAsStream("/env-Resource.properties"));
            conf.addResource(prop.get(hbaseEnv).toString()+"/hbase-site.xml");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        /**
         *  检验zookeeper
         */
        System.out.println("zookeeper的url是：" + conf.get("hbase.zookeeper.quorum"));
        System.out.println("zookeeper的节点是：" + conf.get("zookeeper.znode.parent"));

        return conf;
    }

    public static void main(String args[]){
        DefConfiguration.GetConfiguration("qa");
    }
}
