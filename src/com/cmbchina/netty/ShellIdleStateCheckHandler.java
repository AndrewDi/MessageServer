package com.cmbchina.netty;

/**
 * Created by Andrew on 29/12/2016.
 */
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ShellIdleStateCheckHandler extends IdleStateHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    public ShellIdleStateCheckHandler(int readerIdleTimeSeconds, int writerIdleTimeSeconds, int allIdleTimeSeconds) {
        super(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        if(evt.state()== IdleState.ALL_IDLE)
        {
            //logger.warn("Channel has been idle, it will be disconnected now: " + ctx.channel());
            ctx.channel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//    	LOG.error(cause.getMessage(),cause);
        ctx.channel().close();
    }
}
