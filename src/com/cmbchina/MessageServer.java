package com.cmbchina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MessageServer {
    private static final Logger log = LoggerFactory.getLogger(MessageServer.class);
    public static ApplicationContext context;

    public static void main(String[] args) {
        configuration();
        context = new ClassPathXmlApplicationContext(
                new String[]
                        { "classpath*:applicationContext.xml" });
    }

    private static void configuration(){
        try {
            if(Utils.getLoggerConf()!=null) {
                log.info(String.format("Using Log Conf %s instead",Utils.getLoggerConf()));
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                JoranConfigurator jc = new JoranConfigurator();
                jc.setContext(context);
                context.reset();
                jc.doConfigure(Utils.getLoggerConf());
            }
        } catch (JoranException e) {
            log.error(e.getMessage().toString());
        }
    }
}