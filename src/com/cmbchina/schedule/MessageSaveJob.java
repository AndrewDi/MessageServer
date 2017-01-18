package com.cmbchina.schedule;

import com.cmbchina.MessageProperty;
import com.cmbchina.MessageQueue;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Andrew on 30/12/2016.
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class MessageSaveJob implements Job {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Logger loggerErrorData = LoggerFactory.getLogger("ErrorData");
    private MessageQueue messageQueue;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        this.messageQueue=(MessageQueue) jobExecutionContext.getJobDetail().getJobDataMap().get(MessageQueue.class.toString());
        this.jdbcTemplate = (JdbcTemplate)  jobExecutionContext.getJobDetail().getJobDataMap().get(JdbcTemplate.class.toString());

        if(!messageQueue.getConcurrent())
        {
            try {
                Boolean lock = messageQueue.getReentrantLock().tryLock(10, TimeUnit.MILLISECONDS);
                if(!lock){
                    return;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
        long begin= Calendar.getInstance().getTimeInMillis();
        List<Object []> datas = new ArrayList<Object[]>();
        int i=0;
        long second=0;
        long parseCost=0;
        while (!messageQueue.isEmpty()){
            MessageProperty messageProperty = messageQueue.poll();
            long beginParse = Calendar.getInstance().getTimeInMillis();
            Object[] data = messageProperty.Convert(this.messageQueue.getTableProperty());
            parseCost+=Math.abs(Calendar.getInstance().getTimeInMillis()-beginParse);
            if(data!=null&&messageProperty.isValid()){
                datas.add(data);
            }
            i++;
            second = (beginParse-begin)/1000;
            if(i>0&&(i>=1000||second>=1||messageQueue.isEmpty())){
                long beginSave = Calendar.getInstance().getTimeInMillis();
                try {
                    this.jdbcTemplate.batchUpdate(this.messageQueue.getSQL(), datas);
                }
                catch (Exception exception){
                    for(Object[] row:datas){
                        try {
                            jdbcTemplate.update(this.messageQueue.getSQL(), row);
                        } catch (Exception ex) {
                            logger.error(ex.getMessage()+"|"+StringUtils.join(row,","));
                            loggerErrorData.error(StringUtils.join(row,","));
                        }
                    }
                }
                finally {
                    datas.clear();
                    long endSave = Calendar.getInstance().getTimeInMillis();
                    long totalCost=endSave-beginSave;
                    logger.info("TabName:"+this.messageQueue.getTableProperty().getTabschema()+"."+this.messageQueue.getTableProperty().getTabname()+" Save Data Count:"+i+" Parse Cost:"+parseCost+" Millisecond Total Cost:"+totalCost+" Millisecond Avg Cost:"+totalCost/i);
                    i=0;
                    second=0;
                    parseCost=0;
                    begin=Calendar.getInstance().getTimeInMillis();
                }
            }
        }
        if(!messageQueue.getConcurrent()&&messageQueue.getReentrantLock().isLocked())
        {
            messageQueue.getReentrantLock().unlock();
        }

        datas.clear();
        this.messageQueue=null;
        this.jdbcTemplate=null;
        datas=null;
    }
}