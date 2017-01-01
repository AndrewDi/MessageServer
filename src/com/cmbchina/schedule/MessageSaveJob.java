package com.cmbchina.schedule;

import com.cmbchina.MessageProperty;
import com.cmbchina.MessageQueue;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Andrew on 30/12/2016.
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class MessageSaveJob implements Job {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private MessageQueue messageQueue;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        this.messageQueue=(MessageQueue) jobExecutionContext.getJobDetail().getJobDataMap().get(MessageQueue.class.toString());
        this.jdbcTemplate = (JdbcTemplate)  jobExecutionContext.getJobDetail().getJobDataMap().get(JdbcTemplate.class.toString());

        LocalDateTime begin=LocalDateTime.now();
        List<Object []> datas = new ArrayList<>();
        int i=0;
        long second=0;
        long parseCost=0;
        while (!messageQueue.isEmpty()){
            MessageProperty messageProperty = messageQueue.poll();
            LocalDateTime beginParse = LocalDateTime.now();
            Object[] data = messageProperty.Convert(this.messageQueue.getTableProperty());
            parseCost+=Math.abs(Duration.between(beginParse,LocalDateTime.now()).toMillis());
            if(data!=null&&messageProperty.isValid()){
                datas.add(data);
            }
            i++;
            second = Duration.between(begin,LocalDateTime.now()).getSeconds();
            if(i>0&&(i>=1000||second>=1||messageQueue.isEmpty())){
                LocalDateTime beginSave = LocalDateTime.now();
                try {
                    this.jdbcTemplate.batchUpdate(this.messageQueue.getSQL(), datas);
                }
                catch (Exception exception){
                    for(Object[] row:datas){
                        try {
                            jdbcTemplate.update(this.messageQueue.getSQL(), row);
                        }
                        catch (Exception ex) {
                            logger.error(ex.getMessage()+"\n"+StringUtils.join(row,","));
                        }
                    }
                }
                finally {
                    datas.clear();
                    LocalDateTime endSave = LocalDateTime.now();
                    long totalCost=Duration.between(beginSave,endSave).toMillis();
                    logger.info("TabName:"+this.messageQueue.getTableProperty().getTabschema()+"."+this.messageQueue.getTableProperty().getTabname()+" Save Data Count:"+i+" Parse Cost:"+parseCost+" Millisecond Total Cost:"+totalCost+" Millisecond Avg Cost:"+totalCost/i);
                    i=0;
                    second=0;
                    parseCost=0;
                    begin=LocalDateTime.now();
                }
            }
        }
    }
}