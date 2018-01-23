package com.xxl.mq.broker.rpc;

import com.xxl.mq.client.rpc.netcom.codec.NettyDecoder;
import com.xxl.mq.client.rpc.netcom.codec.NettyEncoder;
import com.xxl.mq.client.rpc.netcom.codec.model.RpcRequest;
import com.xxl.mq.client.rpc.netcom.codec.model.RpcResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * netty rpc server
 * @author xuxueli 2015-10-29 18:17:14
 */
public class NettyServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    
	public void start(final int port) throws Exception {
    	new Thread(new Runnable() {
			@Override
			public void run() {
				EventLoopGroup bossGroup = new NioEventLoopGroup();
		        EventLoopGroup workerGroup = new NioEventLoopGroup();
		        try {
		            ServerBootstrap bootstrap = new ServerBootstrap();
		            bootstrap.group(bossGroup, workerGroup)
							.channel(NioServerSocketChannel.class)
		                	.childHandler(new ChannelInitializer<SocketChannel>() {
								@Override
								public void initChannel(SocketChannel channel) throws Exception {
								channel.pipeline()
									.addLast(new NettyDecoder(RpcRequest.class))
									.addLast(new NettyEncoder(RpcResponse.class))
									.addLast(new NettyServerHandler());
		                    }
		                })
		                .option(ChannelOption.SO_BACKLOG, 128)
		                .option(ChannelOption.TCP_NODELAY, true)
		                .option(ChannelOption.SO_REUSEADDR, true)
		                .childOption(ChannelOption.SO_KEEPALIVE, true);
		            ChannelFuture future = bootstrap.bind(port).sync();
					logger.info(">>>>>>>>>>> xxl-rpc server start success, netcon={}, port={}", NettyServer.class.getName(), port);
					future.channel().closeFuture().sync();
		        } catch (InterruptedException e) {
		        	logger.error("", e);
				} finally {
		            workerGroup.shutdownGracefully();
		            bossGroup.shutdownGracefully();
		        }
			}
		}).start();
    	
    }

}
