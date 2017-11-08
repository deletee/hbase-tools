package com.hbase.tools.hfile.beans;

/**
 * Created by hzxijingjing on 16/7/18.
 */
public class MibaoCodeLoaderBean implements InterfaceLoaderBean{

    public String[] columnNames;

    public void setColumnNames() {
        this.columnNames = new String[2];
        this.columnNames[0] = "ssn";
        this.columnNames[1] = "security_code";
    }

    public int getColumnNumbers() {
        setColumnNames();
        return columnNames.length;
    }

    public String[] getColumnNames() {
        this.setColumnNames();
        return columnNames;
    }

    public static void main(String args[]){
        Class c = null;
        InterfaceLoaderBean bean = null;
        try {
            c = Class.forName("com.netease.urs.rc.mr.hbase.beans.MibaoCodeLoaderBean");
            bean = (InterfaceLoaderBean) c.newInstance();
            String cols[] = bean.getColumnNames();
            System.out.println(cols.length+" "+cols[1]);
            String str = "3c0d5eb4edc983c419902cd73bd0bfcc@tencent.163.com        0 z";
            System.out.println(str.split("\\s+")[2]);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e){
            e.printStackTrace();
        }catch (IllegalAccessException e){
            e.printStackTrace();
        }
    }
}
