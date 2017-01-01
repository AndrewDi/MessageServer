package com.cmbchina.netty;


import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by Andrew on 15/8/16.
 */
public class ShellDataCollectorPipelineFactory extends ChannelInitializer {

    @Autowired
    private ShellDataCollectorHandler shellDataCollectorHandler=null;

    private int maxLength=32767;

    public ShellDataCollectorHandler getShellDataCollectorHandler() {
        return shellDataCollectorHandler;
    }

    public void setShellDataCollectorHandler(ShellDataCollectorHandler shellDataCollectorHandler) {
        this.shellDataCollectorHandler = shellDataCollectorHandler;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
                this.maxLength, Delimiters.lineDelimiter()));
        pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());
        pipeline.addLast("idlehandler", new ShellIdleStateCheckHandler(120,120,120));
        pipeline.addLast(this.shellDataCollectorHandler);
    }
}