package com.cmbchina;

import java.io.*;

/**
 * Created by Andrew on 29/12/2016.
 */
public class Utils {
    public static String getLoggerConf(){
        File file=new File("conf/logback.xml");
        if(file.exists())
        {
            return file.getAbsolutePath();
        }
        return null;
    }

    public static String getQuartzConf(){
        File file=new File("conf/quartz.properties");
        if(file.exists())
        {
            return file.getAbsolutePath();
        }
        return null;
    }
}
