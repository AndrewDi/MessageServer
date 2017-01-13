package com.cmbchina.schedule;

import com.cmbchina.MessageQueue;
import com.cmbchina.Utils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/**
 * Created by Andrew on 30/12/2016.
 */
public class MessageSchedule {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Logger loggerErrorData = LoggerFactory.getLogger("ErrorData");
    private ConcurrentHashMap<String,MessageQueue> messageQueueConcurrentHashMap;
    private Scheduler scheduler = null;
    private String tabschema;
    private Boolean concurrent=false;
    private JdbcTemplate jdbcTemplate;
    private int maxQueuesize;

    public int getMaxQueuesize() {
        return maxQueuesize;
    }

    public void setMaxQueuesize(int maxQueuesize) {
        this.maxQueuesize = maxQueuesize;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String getTabschema() {
        return tabschema;
    }

    public void setTabschema(String tabschema) {
        this.tabschema = tabschema;
    }

    public Boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(Boolean concurrent) {
        this.concurrent = concurrent;
    }

    public MessageSchedule() {
        logger.info("Start Init MessageSchedule");
        StdSchedulerFactory factory = new StdSchedulerFactory();
        try {
            factory.initialize(Utils.getQuartzConf());
            this.messageQueueConcurrentHashMap=new ConcurrentHashMap<>();
            scheduler = factory.getScheduler();
            scheduler.start();
            logger.info("End Init MessageSchedule");
        } catch (SchedulerException e) {
            logger.error(e.getMessage().toString());
        }
    }

    public void start(){
        logger.info("Begin Init MessageQueue Concurrent:"+this.getConcurrent());
        for(Map<String,Object> tabname:getTabName()){
            MessageQueue messageQueue = new MessageQueue(this.jdbcTemplate,tabschema,tabname.get("TABNAME").toString());
            messageQueue.setMaxQueueSize(this.maxQueuesize);
            messageQueue.setConcurrent(this.getConcurrent());
            this.messageQueueConcurrentHashMap.put(tabname.get("TABNAME").toString().trim(),messageQueue);

            JobDetail jobDetail = newJob(MessageSaveJob.class)
                    .withIdentity(tabname.get("TABNAME").toString())
                    .build();
            jobDetail.getJobDataMap().put(MessageQueue.class.toString(),messageQueue);
            jobDetail.getJobDataMap().put(JdbcTemplate.class.toString(),jdbcTemplate);
            logger.info("Build Job "+ jobDetail.getKey().getName());

            Trigger trigger = newTrigger()
                    .withIdentity(tabname.get("TABNAME").toString())
                    .startNow()
                    .withSchedule(simpleSchedule()
                            .withIntervalInSeconds(1)
                            .repeatForever())
                    .build();
            logger.info("Build Trigger " + trigger.getKey().getName());
            try {
                this.scheduler.scheduleJob(jobDetail,trigger);
            } catch (SchedulerException e) {
                logger.error(e.getStackTrace().toString());
            }
        }
        logger.info("End Init MessageQueue");
    }

    public void stop() throws SchedulerException {
        scheduler.clear();
        scheduler.shutdown();
    }

    private List<Map<String, Object>> getTabName(){
        String sql="SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA=? AND TYPE='T'";
        return jdbcTemplate.queryForList(sql,this.tabschema.toUpperCase());
    }

    public void processMessage(String msg,String host,int port){
        String[] msgs = StringUtils.split(msg,"\r\n");
        for(String message:msgs){
            Object[] messageobject = StringUtils.splitPreserveAllTokens(message,",");
            if(this.messageQueueConcurrentHashMap.containsKey(messageobject[0])){
                this.messageQueueConcurrentHashMap.get(messageobject[0]).push(messageobject,host,port);
            }
            else {
                loggerErrorData.error(StringUtils.join(messageobject,","));
            }
        }

        /**
        String[] msgs=msg.split("\\\\\\\\r\\\\\\\\n");
        for(String message:msgs){
            i++;
            Object [] messages = message.split(",");
            if(this.messageQueueConcurrentHashMap.containsKey(messages[0])){
                this.messageQueueConcurrentHashMap.get(messages[0]).push(messages,host,port);
            }
        }
         **/
    }
}