package com.cmbchina.netty;

import com.cmbchina.schedule.MessageSchedule;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;


/**
 * Created by Andrew on 15/8/15.
 */
@ChannelHandler.Sharable
public class ShellDataCollectorHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MessageSchedule messageSchedule;
    public MessageSchedule getMessageSchedule() {
        return messageSchedule;
    }

    public void setMessageSchedule(MessageSchedule messageSchedule) {
        this.messageSchedule = messageSchedule;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String host = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
        int port = ((InetSocketAddress)ctx.channel().remoteAddress()).getPort();
        String msgStr = String.valueOf(msg);
        try {
            logger.debug(msgStr);
            messageSchedule.processMessage(msgStr,host,port);
        }
        catch (Exception ex){
            logger.error(ex.getMessage());
            ctx.channel().close();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(cause.getMessage());
        ctx.channel().close();
    }
}
