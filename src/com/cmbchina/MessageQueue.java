package com.cmbchina;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastDateParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

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

    public MessageQueue(JdbcTemplate jdbcTemplate,String tabschema, String tabname){
        this.tabschema=tabschema;
        this.tabname=tabname;
        this.tableProperty = new TableProperty(jdbcTemplate,tabschema,tabname);
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public MessageProperty poll(){
        return this.messages.poll();
    }

    public void push(Object[] message,String host,int port){
        MessageProperty messageProperty=new MessageProperty(message,host,port);
        messages.add(messageProperty);
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