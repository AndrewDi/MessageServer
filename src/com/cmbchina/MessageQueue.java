package com.cmbchina;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.instrument.Instrumentation;

/**
 * Created by Andrew on 30/12/2016.
 */
public class MessageQueue {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private TableProperty tableProperty;
    private ConcurrentLinkedQueue<MessageProperty> messages;
    private String tabschema;
    private String tabname;
    private Boolean concurrent=false;
    private ReentrantLock reentrantLock;
    private int maxQueueSize=1000;

    public MessageQueue(JdbcTemplate jdbcTemplate,String tabschema, String tabname){
        this.tabschema=tabschema;
        this.tabname=tabname;
        this.tableProperty = new TableProperty(jdbcTemplate,tabschema,tabname);
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public MessageProperty poll(){
        return this.messages.poll();
    }

    public void push(Object[] message,String host,int port){
        MessageProperty messageProperty=new MessageProperty(message,host,port);
        if(this.messages.size()<this.maxQueueSize){
            messages.add(messageProperty);
        }
        else {
            logger.error(String.format("Too many messages was input to the queue,please add more application,following message will be delete:%s",StringUtils.join(message,",")));
        }
    }

    public void reInit(JdbcTemplate jdbcTemplate){
        this.tableProperty = new TableProperty(jdbcTemplate,tabschema,tabname);
    }

    public Boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(Boolean concurrent) {
        this.concurrent = concurrent;
        reentrantLock=new ReentrantLock();
    }

    public ReentrantLock getReentrantLock() {
        return reentrantLock;
    }

    public void setReentrantLock(ReentrantLock reentrantLock) {
        this.reentrantLock = reentrantLock;
    }

    public boolean isEmpty(){
        return messages.isEmpty();
    }

    public String getSQL(){
        return this.tableProperty.getSql();
    }

    public TableProperty getTableProperty() {
        return tableProperty;
    }
}