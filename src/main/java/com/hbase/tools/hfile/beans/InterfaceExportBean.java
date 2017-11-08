package com.hbase.tools.hfile.beans;

import org.apache.hadoop.io.Text;

/**
 * Created by hzxijingjing on 16/7/12.
 */
public interface InterfaceExportBean {
    public void setBean(Iterable<Text> values);
    public String toString();
//  public void setValue(String val);
}
