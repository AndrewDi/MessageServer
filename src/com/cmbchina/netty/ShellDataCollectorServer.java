package com.cmbchina.netty;

import com.cmbchina.schedule.MessageSchedule;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;

/**
 * Created by Andrew on 29/12/2016.
 */
public class ShellDataCollectorServer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected ServerBootstrap serverBootstrap;

    private int port = 1860;
    private int workerThreads = 10;
    private static NioEventLoopGroup bossGroup = null;
    private static NioEventLoopGroup workerGroup = null;
    private boolean isStarted = false;
    private ShellDataCollectorPipelineFactory shellDataCollectorPipelineFactory=null;


    public boolean isStarted() {
        return isStarted;
    }

    public void setIsStarted(boolean isStarted) {
        this.isStarted = isStarted;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public ShellDataCollectorPipelineFactory getShellDataCollectorPipelineFactory() {
        return shellDataCollectorPipelineFactory;
    }

    public void setShellDataCollectorPipelineFactory(ShellDataCollectorPipelineFactory shellDataCollectorPipelineFactory) {
        this.shellDataCollectorPipelineFactory = shellDataCollectorPipelineFactory;
    }

    public void Start()
    {
        try {
            logger.info("Begin DataCollectorServer Started.");
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup(workerThreads);
            serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.childHandler(this.shellDataCollectorPipelineFactory);
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
            serverBootstrap.option(ChannelOption.SO_BACKLOG, 128);
            ChannelFuture cf = serverBootstrap.bind(new InetSocketAddress(this.port)).sync();
            this.isStarted = true ;
            logger.info("End DataCollectorServer End.");
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            logger.error("tcp start fault:"+this.port);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void Stop()
    {
        logger.info("Begin to stop DataCollectorServer");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        this.isStarted=false;
        logger.info("End to stop DataCollectorServer");
    }
}
