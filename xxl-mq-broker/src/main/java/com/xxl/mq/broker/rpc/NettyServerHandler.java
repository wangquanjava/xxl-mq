package com.xxl.mq.broker.rpc;

import com.xxl.mq.client.rpc.netcom.codec.model.RpcRequest;
import com.xxl.mq.client.rpc.netcom.codec.model.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rpc netty server handler
 * @author xuxueli 2015-10-29 20:07:37
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<RpcRequest> {
    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, RpcRequest request) throws Exception {
    	
    	// invoke
        RpcResponse response = NetComServerFactory.invokeService(request, null);
    	
        ctx.writeAndFlush(response);
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    	logger.error(">>>>>>>>>>> xxl-rpc provider netty server caught exception", cause);
        ctx.close();
    }
}
