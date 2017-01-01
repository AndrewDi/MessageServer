package com.cmbchina;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Created by Andrew on 29/12/2016.
 */
public class MessageProperty {
    private static final Logger logger = LoggerFactory.getLogger(MessageServer.class);
    private Object[] data;
    private String TYPE_INT="INTEGER";
    private String TYPE_SMALLINT="SMALLINT";
    private String TYPE_BIGINT="BIGINT";
    private String TYPE_VARCHAR="VARCHAR";
    private String TYPE_CHAR="CHARACTER";
    private String TYPE_DECIMAL="DECIMAL";
    private String TYPE_TIMESTAMP="TIMESTAMP";
    private String TYPE_DATE="DATE";
    private LocalDateTime currentData;
    private String host;
    private int port;

    public MessageProperty(Object[] data,String host,int port){
        this.data=data;
        this.host=host;
        this.port=port;
        this.currentData = LocalDateTime.now();
    }

    public Object[] getData(){
        return this.data;
    }

    public boolean isValid(){
        if(data==null){
            return false;
        }
        if(Duration.between(currentData,LocalDateTime.now()).getSeconds()>60){
            return false;
        }
        return true;
    }

    public Object[] Convert(TableProperty tableProperty){
        LocalDateTime beginParse = LocalDateTime.now();
        int columnSize=tableProperty.getColumnProperties().size();
        Object[] newMsg = new Object[columnSize];
        StringBuffer dataDump = new StringBuffer("Data Dump {\n");
        for(int i=0;i<data.length&&i<columnSize;i++){
            if(i==0){
                dataDump.append("TableName:"+data[0]+"\n");
                continue;
            }
            ColumnProperty columnProperty = tableProperty.getColumnProperties().get(i-1);
            if(columnProperty.getColType().equals(TYPE_CHAR)||columnProperty.getColType().equals(TYPE_VARCHAR)){
                if(data[i].toString().length()>columnProperty.getLength()){
                    logger.error(String.valueOf(data[i].toString().length()));
                    newMsg[i-1]=data[i].toString().substring(0,columnProperty.getLength()-1);
                    logger.error(String.format("TabName:%s ColName:%s ColType:%s LongData:%s",data[0],columnProperty.getColName(),columnProperty.getColType(),newMsg[i-1]));
                }
                else {
                    newMsg[i-1]=data[i];
                }
            }
            else if(columnProperty.getColType().equals(TYPE_INT)||columnProperty.getColType().equals(TYPE_SMALLINT)||columnProperty.getColType().equals(TYPE_BIGINT)){
                Boolean isEmpty = StringUtils.isBlank(data[i].toString())||StringUtils.isEmpty(data[i].toString());
                if(isEmpty&&columnProperty.isNulls()){
                    newMsg[i-1]=null;
                }
                else if(isEmpty&&!columnProperty.isNulls()){
                    newMsg[i-1]=-1;
                    logger.error(String.format("Host:%s TabName:%s ColName:%s ColType:%s NotNumeric:%s",this.host,data[0],columnProperty.getColName(),columnProperty.getColType(),newMsg[i-1]));
                }
                else{
                    if(columnProperty.getColType().equals(TYPE_INT))
                        newMsg[i-1]=NumberUtils.toInt(data[i].toString(),-1);
                    else if (columnProperty.getColType().equals(TYPE_SMALLINT))
                        newMsg[i-1]=NumberUtils.toShort(data[i].toString(),(short)-1);
                    else if(columnProperty.getColType().equals(TYPE_BIGINT))
                        newMsg[i-1]=NumberUtils.toLong(data[i].toString(),(long)-1);
                }
            }
            else if(columnProperty.getColType().equals(TYPE_TIMESTAMP)){
                if(!data[i].toString().isEmpty()) {
                    try {
                        data[i]=StringUtils.replaceAll(data[i].toString(),":",".");
                        FastDateFormat.getInstance("YYYY-mm-dd-HH.MM.SS").parse(data[i].toString());
                        newMsg[i - 1] = data[i];
                    } catch (ParseException e) {
                        newMsg[i-1]="1900-01-01-00.00.00";
                        logger.error(String.format("Host:%s TabName:%s ColName:%s ColType:%s ErrorTimestamp:%s",this.host,data[0],columnProperty.getColName(),columnProperty.getColType(),data[i]));
                    }
                }
                else if(data[i].toString().isEmpty()&&columnProperty.isNulls()){
                    newMsg[i-1]="1900-01-01-00.00.00";
                    logger.error(String.format("Host:%s TabName:%s ColName:%s Nullable:%s ColType:%s Nullable_Timestamp:%s",this.host,data[0],columnProperty.getColName(),columnProperty.isNulls(),columnProperty.getColType(),data[i]));
                }
                else {
                    newMsg[i-1]="1900-01-01-00.00.00";
                    logger.error(String.format("Host:%s TabName:%s ColName:%s ColType:%s Not_Null_Timestamp:%s",this.host,data[0],columnProperty.getColName(),columnProperty.getColType(),data[i]));
                }
            }
            else if(columnProperty.getColType().equals(TYPE_DATE)){
                if(!data[i].toString().isEmpty()) {
                    try {
                        FastDateFormat.getInstance("YYYY-mm-dd").parse(data[i].toString());
                        newMsg[i - 1] = data[i];
                    } catch (ParseException e) {
                        newMsg[i-1]="1900-01-01";
                        logger.error(String.format("Host:%s TabName:%s ColName:%s ColType:%s ErrorDate:%s",this.host,data[0],columnProperty.getColName(),columnProperty.getColType(),data[i]));
                    }
                }
                else if(data[i].toString().isEmpty()&&columnProperty.isNulls()){
                    newMsg[i-1]="1900-01-01";
                    logger.error(String.format("Host:%s TabName:%s ColName:%s Nullable:%s ColType:%s Nullable_Date:%s",this.host,data[0],columnProperty.getColName(),columnProperty.isNulls(),columnProperty.getColType(),data[i]));
                }
                else {
                    newMsg[i-1]="1900-01-01";
                    logger.error(String.format("Host:%s TabName:%s ColName:%s ColType:%s Not_Null_Date:%s",this.host,data[0],columnProperty.getColName(),columnProperty.getColType(),data[i]));
                }
            }
            else {
                if(data[i].toString().isEmpty()){
                    newMsg[i-1]=null;
                }
                else {
                    newMsg[i-1]=data[i];
                }
            }
            dataDump.append("Index:"+i+" ColName:"+columnProperty.getColName()+" Nullable:"+columnProperty.isNulls()+" Type:"+columnProperty.getColType()+" OldData:"+data[i]+" NewData:"+newMsg[i-1]+"\n");
        }
        dataDump.append("}\n");
        LocalDateTime endParse = LocalDateTime.now();
        Duration duration = Duration.between(beginParse,endParse);
        dataDump.append("Parse Data Cost:"+duration.toMillis());
        logger.debug(dataDump.toString());
        return newMsg;
    }
}