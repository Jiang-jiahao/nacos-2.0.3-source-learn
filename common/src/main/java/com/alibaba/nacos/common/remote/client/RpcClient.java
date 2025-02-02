/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.remote.client;

import com.alibaba.nacos.api.ability.ClientAbilities;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.PayloadRegistry;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.RequestFuture;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.request.ConnectResetRequest;
import com.alibaba.nacos.api.remote.request.HealthCheckRequest;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.ClientDetectionResponse;
import com.alibaba.nacos.api.remote.response.ConnectResetResponse;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * abstract remote client to connect to server.
 *
 * @author liuzunfei
 * @version $Id: RpcClient.java, v 0.1 2020年07月13日 9:15 PM liuzunfei Exp $
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class RpcClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.alibaba.nacos.common.remote.client");

    /**
     * 服务器工厂
     * 作用：在这主要是获取服务器列表的
     */
    private ServerListFactory serverListFactory;

    /**
     * rpc连接事件阻塞队列
     */
    protected LinkedBlockingQueue<ConnectionEvent> eventLinkedBlockingQueue = new LinkedBlockingQueue<ConnectionEvent>();

    /**
     * rpc客户端状态
     */
    protected volatile AtomicReference<RpcClientStatus> rpcClientStatus = new AtomicReference<RpcClientStatus>(
            RpcClientStatus.WAIT_INIT);

    /**
     * 周期性任务线程池
     */
    protected ScheduledExecutorService clientEventExecutor;

    /**
     * 重试服务阻塞队列
     */
    private final BlockingQueue<ReconnectContext> reconnectionSignal = new ArrayBlockingQueue<ReconnectContext>(1);

    /**
     * 当前rpc连接
     */
    protected volatile Connection currentConnection;

    /**
     * 用于标注客户端请求的标签
     */
    protected Map<String, String> labels = new HashMap<String, String>();

    private String name;

    private String tenant;

    /**
     * rpc客户端连接服务器重试次数
     */
    private static final int RETRY_TIMES = 3;

    /**
     * rpc客户端请求默认的超时时间
     */
    private static final long DEFAULT_TIMEOUT_MILLS = 3000L;

    /**
     * 表示rpc客户端能力的对象
     */
    protected ClientAbilities clientAbilities;

    /**
     * default keep alive time 5s.
     * rpc客户端与服务端连接默认保持时间为5s
     */
    private long keepAliveTime = 5000L;

    /**
     * rpc客户端与服务端连接最后存活时间
     */
    private long lastActiveTimeStamp = System.currentTimeMillis();

    /**
     * listener called where connection's status changed.
     * 在连接状态改变的地方调用监听器
     */
    protected List<ConnectionEventListener> connectionEventListeners = new ArrayList<ConnectionEventListener>();

    /**
     * handlers to process server push request.
     * 处理服务器推送请求的处理程序
     */
    protected List<ServerRequestHandler> serverRequestHandlers = new ArrayList<ServerRequestHandler>();

    static {
        PayloadRegistry.init();
    }

    public RpcClient(String name) {
        this.name = name;
    }

    public RpcClient(ServerListFactory serverListFactory) {
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        LoggerUtils.printIfInfoEnabled(LOGGER, "RpcClient init in constructor, ServerListFactory ={}",
                serverListFactory.getClass().getName());
    }

    public RpcClient(String name, ServerListFactory serverListFactory) {
        this(name);
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        LoggerUtils.printIfInfoEnabled(LOGGER, "RpcClient init in constructor, ServerListFactory ={}",
                serverListFactory.getClass().getName());
    }

    /**
     * init client abilities.
     *
     * @param clientAbilities clientAbilities.
     */
    public RpcClient clientAbilities(ClientAbilities clientAbilities) {
        this.clientAbilities = clientAbilities;
        return this;
    }

    /**
     * init server list factory.only can init once.
     * 初始化服务器列表工厂。只能初始化一次
     *
     * @param serverListFactory serverListFactory
     */
    public RpcClient serverListFactory(ServerListFactory serverListFactory) {
        // 如果初始化过，则直接返回
        if (!isWaitInitiated()) {
            return this;
        }
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);

        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init, ServerListFactory ={}", name,
                serverListFactory.getClass().getName());
        return this;
    }

    /**
     * init labels.
     *
     * @param labels labels
     */
    public RpcClient labels(Map<String, String> labels) {
        this.labels.putAll(labels);
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init label, labels={}", name, this.labels);
        return this;
    }

    /**
     * init keepalive time.
     *
     * @param keepAliveTime keepAliveTime
     * @param timeUnit      timeUnit
     */
    public RpcClient keepAlive(long keepAliveTime, TimeUnit timeUnit) {
        this.keepAliveTime = keepAliveTime * timeUnit.toMillis(keepAliveTime);
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init keepalive time, keepAliveTimeMillis={}", name,
                keepAliveTime);
        return this;
    }

    /**
     * Notify when client disconnected.
     */
    protected void notifyDisConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Notify disconnected event to listeners", name);
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                connectionEventListener.onDisConnect();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Notify disconnect listener error,listener ={}", name,
                        connectionEventListener.getClass().getName());
            }
        }
    }

    /**
     * Notify when client new connected.
     */
    protected void notifyConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Notify connected event to listeners.", name);
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                connectionEventListener.onConnected();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Notify connect listener error,listener ={}", name,
                        connectionEventListener.getClass().getName());
            }
        }
    }

    /**
     * check is this client is initiated.
     *
     * @return is wait initiated or not.
     */
    public boolean isWaitInitiated() {
        return this.rpcClientStatus.get() == RpcClientStatus.WAIT_INIT;
    }

    /**
     * check is this client is running.
     *
     * @return is running or not.
     */
    public boolean isRunning() {
        return this.rpcClientStatus.get() == RpcClientStatus.RUNNING;
    }

    /**
     * check is this client is shutdown.
     *
     * @return is shutdown or not.
     */
    public boolean isShutdown() {
        return this.rpcClientStatus.get() == RpcClientStatus.SHUTDOWN;
    }

    /**
     * check if current connected server is in serverlist ,if not switch server.
     */
    public void onServerListChange() {
        if (currentConnection != null && currentConnection.serverInfo != null) {
            ServerInfo serverInfo = currentConnection.serverInfo;
            boolean found = false;
            for (String serverAddress : serverListFactory.getServerList()) {
                if (resolveServerInfo(serverAddress).getAddress().equalsIgnoreCase(serverInfo.getAddress())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LoggerUtils.printIfInfoEnabled(LOGGER,
                        "Current connected server {}  is not in latest server list,switch switchServerAsync",
                        serverInfo.getAddress());
                switchServerAsync();
            }

        }
    }

    /**
     * Start this client.
     */
    public final void start() throws NacosException {
        // 修改rpc客户端状态为STARTING
        boolean success = rpcClientStatus.compareAndSet(RpcClientStatus.INITIALIZED, RpcClientStatus.STARTING);
        if (!success) {
            return;
        }

        // 创建周期性任务线程池
        clientEventExecutor = new ScheduledThreadPoolExecutor(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.remote.worker");
                t.setDaemon(true);
                return t;
            }
        });

        // connection event consumer.
        // 连接事件消费者，给对应监听器发送事件
        clientEventExecutor.submit(new Runnable() {
            @Override
            public void run() {
                // 判断线程池是否被关闭，被关闭则不再执行
                while (!clientEventExecutor.isTerminated() && !clientEventExecutor.isShutdown()) {
                    ConnectionEvent take = null;
                    try {
                        // 从连接事件阻塞队列拿连接事件
                        // 在连接成功时，会往队列中存放连接成功的事件；连接断开时会存放连接断开的事件
                        take = eventLinkedBlockingQueue.take();
                        // 判断rpc客户端是否已经连接
                        if (take.isConnected()) {
                            // 执行连接成功的监听器（通知重试机制已经连接，不需要再重试）
                            notifyConnected();
                        } else if (take.isDisConnected()) {
                            // 执行连接断开的监听器（通知重试机制未连接，需要重试）
                            notifyDisConnected();
                        }
                    } catch (Throwable e) {
                        //Do nothing
                    }
                }
            }
        });

        // 服务端健康检查及重连服务，有重连的情况则不会进行健康检查，健康检查的前提是所有连接都健康
        clientEventExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        // 如果rpc客户端关闭则终止循环
                        if (isShutdown()) {
                            break;
                        }
                        // 获取ReconnectContext（重连上下文对象）对象，默认5秒内取不到则返回null
                        ReconnectContext reconnectContext = reconnectionSignal
                                .poll(keepAliveTime, TimeUnit.MILLISECONDS);
                        // 1、如果没有拿到ReconnectContext对象，则表示没有重连的需要，则会检查server的健康状况，
                        // 如果不健康，则会创建一个空的重连上下文对象，后面会选择一个server重连
                        if (reconnectContext == null) {
                            //check alive time.
                            // 检查存活时间
                            // 如果没有重连的需求，这里每隔默认5秒会去检查server健康情况
                            if (System.currentTimeMillis() - lastActiveTimeStamp >= keepAliveTime) {
                                // 判断当前连接是否健康
                                boolean isHealthy = healthCheck();
                                if (!isHealthy) {
                                    // 如果currentConnection为空，表明还在启动连接server的情况，并不是server那边不健康的问题
                                    if (currentConnection == null) {
                                        continue;
                                    }
                                    // 执行到这里表示，rpc客户端和服务端连接有异常
                                    LoggerUtils.printIfInfoEnabled(LOGGER,
                                            "[{}]Server healthy check fail,currentConnection={}", name,
                                            currentConnection.getConnectionId());

                                    RpcClientStatus rpcClientStatus = RpcClient.this.rpcClientStatus.get();
                                    // 在发起重连之前，这里再次判断rpc客户端是否被关闭（这里有可能客户端被关闭了，如果不判断的话SHUTDOWN又被改为UNHEALTHY是不合理的）
                                    if (RpcClientStatus.SHUTDOWN.equals(rpcClientStatus)) {
                                        // 如果已经关闭连接，则中断循环
                                        break;
                                    }
                                    // 设置客户端不健康状态（这里必须使用cas来更新，因为此时状态可能被修改了，可能会存在SHUTDOWN->UNHEALTHY）
                                    boolean success = RpcClient.this.rpcClientStatus
                                            .compareAndSet(rpcClientStatus, RpcClientStatus.UNHEALTHY);
                                    // TODO 这个地方状态可能会变成RUNNING
                                    if (success) {
                                        // 如果客户端不健康状态设置成功，则设置一个空的重新连接上下文对象（相当于重新选一个随机的server来重连）
                                        reconnectContext = new ReconnectContext(null, false);
                                    } else {
                                        // 如果设置失败，表示状态在更新之前发生了改变，则选择重试
                                        // 可能是状态期间被改为SHUTDOWN了，也可能是状态期间被改为RUNNING
                                        // 因为在代码①处是先赋值新连接，后修改状态的。可能会出现这个问题，所以这里选择continue
                                        continue;
                                    }

                                } else {
                                    // 健康则更新最后的活跃时间
                                    lastActiveTimeStamp = System.currentTimeMillis();
                                    continue;
                                }
                            } else {
                                continue;
                            }

                        }

                        // 2、有重连的需求，如果ReconnectContext对象是有指定的server，则检查server是否存在，如果不存在则将ReconnectContext设置为空
                        // 之后自己会选取一个server
                        if (reconnectContext.serverInfo != null) {
                            //clear recommend server if server is not in server list.
                            boolean serverExist = false;
                            // 从serverListFactory中获取服务器列表
                            for (String server : getServerListFactory().getServerList()) {
                                ServerInfo serverInfo = resolveServerInfo(server);
                                // 判断服务器是否存在
                                if (serverInfo.getServerIp().equals(reconnectContext.serverInfo.getServerIp())) {
                                    serverExist = true;
                                    reconnectContext.serverInfo.serverPort = serverInfo.serverPort;
                                    break;
                                }
                            }
                            // 如果给到的server信息不存在，则自己选取一个
                            if (!serverExist) {
                                LoggerUtils.printIfInfoEnabled(LOGGER,
                                        "[{}] Recommend server is not in server list ,ignore recommend server {}", name,
                                        reconnectContext.serverInfo.getAddress());

                                reconnectContext.serverInfo = null;

                            }
                        }

                        // 3、重新连接server，关闭之前的连接
                        reconnect(reconnectContext.serverInfo, reconnectContext.onRequestFail);
                    } catch (Throwable throwable) {
                        //Do nothing
                    }
                }
            }
        });

        //connect to server ,try to connect to server sync once, async starting if fail.
        // 连接到服务器，尝试连接到服务器同步一次，如果失败异步启动
        Connection connectToServer = null;
        // TODO 这段我认为没什么必要，在这期间状态是不会被改变的
        rpcClientStatus.set(RpcClientStatus.STARTING);

        int startUpRetryTimes = RETRY_TIMES;
        while (startUpRetryTimes > 0 && connectToServer == null) {
            try {
                startUpRetryTimes--;
                // 获取服务器信息
                ServerInfo serverInfo = nextRpcServer();

                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Try to connect to server on start up, server: {}", name,
                        serverInfo);

                connectToServer = connectToServer(serverInfo);
            } catch (Throwable e) {
                LoggerUtils.printIfWarnEnabled(LOGGER,
                        "[{}]Fail to connect to server on start up, error message={}, start up retry times left: {}",
                        name, e.getMessage(), startUpRetryTimes);
            }

        }

        // 判断是否连接成功
        if (connectToServer != null) {
            LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Success to connect to server [{}] on start up,connectionId={}",
                    name, connectToServer.serverInfo.getAddress(), connectToServer.getConnectionId());
            this.currentConnection = connectToServer; // ①
            rpcClientStatus.set(RpcClientStatus.RUNNING);
            // 添加连接成功事件
            eventLinkedBlockingQueue.offer(new ConnectionEvent(ConnectionEvent.CONNECTED));
        } else {
            // 要是没有一个服务端能连接，则尝试随机重连
            switchServerAsync();
        }
        // 监听重新设置连接请求
        registerServerRequestHandler(new ConnectResetRequestHandler());

        //register client detection request.
        registerServerRequestHandler(new ServerRequestHandler() {
            @Override
            public Response requestReply(Request request) {
                if (request instanceof ClientDetectionRequest) {
                    return new ClientDetectionResponse();
                }

                return null;
            }
        });

    }

    class ConnectResetRequestHandler implements ServerRequestHandler {

        @Override
        public Response requestReply(Request request) {

            if (request instanceof ConnectResetRequest) {

                try {
                    synchronized (RpcClient.this) {
                        // 判断是否在运行中
                        if (isRunning()) {
                            ConnectResetRequest connectResetRequest = (ConnectResetRequest) request;
                            if (StringUtils.isNotBlank(connectResetRequest.getServerIp())) {
                                ServerInfo serverInfo = resolveServerInfo(
                                        connectResetRequest.getServerIp() + Constants.COLON + connectResetRequest
                                                .getServerPort());
                                switchServerAsync(serverInfo, false);
                            } else {
                                // 往reconnectionSignal中设置空的重连上下文
                                switchServerAsync();
                            }
                        }
                    }
                } catch (Exception e) {
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Switch server error ,{}", name, e);
                }
                return new ConnectResetResponse();
            }
            return null;
        }
    }

    @Override
    public void shutdown() throws NacosException {
        LOGGER.info("Shutdown rpc client ,set status to shutdown");
        rpcClientStatus.set(RpcClientStatus.SHUTDOWN);
        LOGGER.info("Shutdown  client event executor " + clientEventExecutor);
        clientEventExecutor.shutdownNow();
        LOGGER.info("Close current connection " + currentConnection.getConnectionId());
        closeConnection(currentConnection);
    }

    private boolean healthCheck() {
        HealthCheckRequest healthCheckRequest = new HealthCheckRequest();
        if (this.currentConnection == null) {
            return false;
        }
        try {
            Response response = this.currentConnection.request(healthCheckRequest, 3000L);
            //not only check server is ok ,also check connection is register.
            return response != null && response.isSuccess();
        } catch (NacosException e) {
            //ignore
        }
        return false;
    }

    // 一般是rpc客户端请求失败时触发
    public void switchServerAsyncOnRequestFail() {
        switchServerAsync(null, true);
    }

    // 一般是rpc客户端启动时触发或者是定时任务检查到不健康时触发
    public void switchServerAsync() {
        switchServerAsync(null, false);
    }

    protected void switchServerAsync(final ServerInfo recommendServerInfo, boolean onRequestFail) {
        reconnectionSignal.offer(new ReconnectContext(recommendServerInfo, onRequestFail));
    }

    /**
     * switch server .
     */
    protected void reconnect(final ServerInfo recommendServerInfo, boolean onRequestFail) {

        try {

            AtomicReference<ServerInfo> recommendServer = new AtomicReference<ServerInfo>(recommendServerInfo);
            // 判断服务端是否健康，当前请求是否失败
            // onRequestFail为true，则表示rpc请求失败了，如果请求失败了，之后再次检查server是否健康
            // 如果是false，则可能是服务端的重连请求，则不再检查服务端的健康状态
            // 如果健康，则重新设置回RUNNING状态，不继续进行重连行为
            if (onRequestFail && healthCheck()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Server check success,currentServer is{} ", name,
                        currentConnection.serverInfo.getAddress());
                // 有可能会出现这样的情况，在shutdown方法中设置状态为SHUTDOWN之后，这里还会继续执行改为RUNNING。但是实际上也只是状态的改变，关闭线程池之后不会影响
                // 之前修改状态代码如下：
                // RpcClientStatus rpcClientStatus = RpcClient.this.rpcClientStatus.get();
                // if (RpcClientStatus.SHUTDOWN.equals(rpcClientStatus)) {
                //     break;
                // }
                // boolean success = RpcClient.this.rpcClientStatus.compareAndSet(rpcClientStatus, RpcClientStatus.UNHEALTHY);
                // 使用cas来保证状态是因为是怕在关闭线程池之后还创建了重连对象，之后因为状态是UNHEALTHY，还会继续往下执行重连的操作，
                // 因为rpc连接被关闭后再次调用该连接去访问服务器会导致后续一些资源浪费（比如说关闭之前又创建了rpc连接，浪费性能）。
                // 而这里但存的将状态改为RUNNING是不会产生这些的，虽然会导致最后状态有问题，但是最后还是关闭了，不会影响。
                rpcClientStatus.set(RpcClientStatus.RUNNING);
                return;
            }
            LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] try to re connect to a new server ,server is {}", name,
                    recommendServerInfo == null ? " not appointed,will choose a random server."
                            : (recommendServerInfo.getAddress() + ", will try it once."));

            // loop until start client success.
            // 循环直到客户端启动成功
            boolean switchSuccess = false;
            // 重试次数
            int reConnectTimes = 0;
            // 重试了几轮
            int retryTurns = 0;
            Exception lastException = null;
            // 如果没有切换成功并且client没有关闭，则继续连接
            while (!switchSuccess && !isShutdown()) {

                //1.get a new server
                // 获取到新的服务端
                ServerInfo serverInfo = null;
                try {
                    // 如果没有指定server信息，则会重新轮询选取一个
                    serverInfo = recommendServer.get() == null ? nextRpcServer() : recommendServer.get();
                    //2.create a new channel to new server
                    // 根据server信息，创建新服务端的新通道
                    Connection connectionNew = connectToServer(serverInfo);
                    // 成功创建新连接，则如果有老连接对象则替换，并发布连接成功事件
                    if (connectionNew != null) {
                        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] success to connect a server  [{}],connectionId={}",
                                name, serverInfo.getAddress(), connectionNew.getConnectionId());
                        //successfully create a new connect.
                        // 成功创建一个新连接
                        if (currentConnection != null) {
                            LoggerUtils.printIfInfoEnabled(LOGGER,
                                    "[{}] Abandon prev connection ,server is  {}, connectionId is {}", name,
                                    currentConnection.serverInfo.getAddress(), currentConnection.getConnectionId());
                            //set current connection to enable connection event.
                            // 标注废弃该连接
                            currentConnection.setAbandon(true);
                            // 关闭当前连接
                            closeConnection(currentConnection);
                        }
                        // 新连接替换旧连接
                        currentConnection = connectionNew;
                        rpcClientStatus.set(RpcClientStatus.RUNNING);
                        switchSuccess = true;
                        // 发布已连接事件
                        boolean s = eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.CONNECTED));
                        return;
                    }

                    //close connection if client is already shutdown.
                    // 如果客户端已经关闭，则关闭连接
                    if (isShutdown()) {
                        closeConnection(currentConnection);
                    }

                    lastException = null;

                } catch (Exception e) {
                    lastException = e;
                } finally {
                    recommendServer.set(null);
                }

                if (reConnectTimes > 0
                        && reConnectTimes % RpcClient.this.serverListFactory.getServerList().size() == 0) {
                    LoggerUtils.printIfInfoEnabled(LOGGER,
                            "[{}] fail to connect server,after trying {} times, last try server is {},error={}", name,
                            reConnectTimes, serverInfo, lastException == null ? "unknown" : lastException);
                    if (Integer.MAX_VALUE == retryTurns) {
                        retryTurns = 50;
                    } else {
                        // 服务器循环了一轮则加一
                        retryTurns++;
                    }
                }

                reConnectTimes++;

                try {
                    //sleep x milliseconds to switch next server.
                    if (!isRunning()) {
                        // first round ,try servers at a delay 100ms;second round ,200ms; max delays 5s. to be reconsidered.
                        // 第一轮，服务器延迟100ms;第二轮200ms，最后每轮最高一直是5000毫秒重连一次
                        Thread.sleep(Math.min(retryTurns + 1, 50) * 100L);
                    }
                } catch (InterruptedException e) {
                    // Do  nothing.
                }
            }

            if (isShutdown()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Client is shutdown ,stop reconnect to server", name);
            }

        } catch (Exception e) {
            LoggerUtils.printIfWarnEnabled(LOGGER, "[{}] Fail to  re connect to server ,error is {}", name, e);
        }
    }

    private void closeConnection(Connection connection) {
        if (connection != null) {
            connection.close();
            eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.DISCONNECTED));
        }
    }

    /**
     * get connection type of this client.
     *
     * @return ConnectionType.
     */
    public abstract ConnectionType getConnectionType();

    /**
     * increase offset of the nacos server port for the rpc server port.
     *
     * @return rpc port offset
     */
    public abstract int rpcPortOffset();

    /**
     * get current server.
     *
     * @return server info.
     */
    public ServerInfo getCurrentServer() {
        if (this.currentConnection != null) {
            return currentConnection.serverInfo;
        }
        return null;
    }

    /**
     * send request.
     *
     * @param request request.
     * @return response from server.
     */
    public Response request(Request request) throws NacosException {
        return request(request, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * send request.
     *
     * @param request request.
     * @return response from server.
     */
    public Response request(Request request, long timeoutMills) throws NacosException {
        int retryTimes = 0;
        Response response = null;
        Exception exceptionThrow = null;
        long start = System.currentTimeMillis();
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < timeoutMills + start) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_DISCONNECT,
                            "Client not connected,current status:" + rpcClientStatus.get());
                }
                response = this.currentConnection.request(request, timeoutMills);
                if (response == null) {
                    throw new NacosException(SERVER_ERROR, "Unknown Exception.");
                }
                if (response instanceof ErrorResponse) {
                    if (response.getErrorCode() == NacosException.UN_REGISTER) {
                        synchronized (this) {
                            waitReconnect = true;
                            if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
                                LoggerUtils.printIfErrorEnabled(LOGGER,
                                        "Connection is unregistered, switch server,connectionId={},request={}",
                                        currentConnection.getConnectionId(), request.getClass().getSimpleName());
                                switchServerAsync();
                            }
                        }

                    }
                    throw new NacosException(response.getErrorCode(), response.getMessage());
                }
                // return response.
                lastActiveTimeStamp = System.currentTimeMillis();
                return response;

            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(Math.min(100, timeoutMills / 3));
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }

                LoggerUtils.printIfErrorEnabled(LOGGER, "Send request fail, request={}, retryTimes={},errorMessage={}",
                        request, retryTimes, e.getMessage());

                exceptionThrow = e;

            }
            retryTimes++;

        }

        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }

        if (exceptionThrow != null) {
            throw (exceptionThrow instanceof NacosException) ? (NacosException) exceptionThrow
                    : new NacosException(SERVER_ERROR, exceptionThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request fail, unknown Error");
        }
    }

    /**
     * send async request.
     *
     * @param request request.
     */
    public void asyncRequest(Request request, RequestCallBack callback) throws NacosException {
        int retryTimes = 0;

        Exception exceptionToThrow = null;
        long start = System.currentTimeMillis();
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < start + callback.getTimeout()) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                this.currentConnection.asyncRequest(request, callback);
                return;
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(Math.min(100, callback.getTimeout() / 3));
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }
                LoggerUtils
                        .printIfErrorEnabled(LOGGER, "[{}]Send request fail, request={}, retryTimes={},errorMessage={}",
                                name, request, retryTimes, e.getMessage());
                exceptionToThrow = e;

            }
            retryTimes++;

        }

        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "AsyncRequest fail, unknown error");
        }
    }

    /**
     * send async request.
     *
     * @param request request.
     * @return request future.
     */
    public RequestFuture requestFuture(Request request) throws NacosException {
        int retryTimes = 0;
        long start = System.currentTimeMillis();
        Exception exceptionToThrow = null;
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < start + DEFAULT_TIMEOUT_MILLS) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                return this.currentConnection.requestFuture(request);
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(100L);
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }
                LoggerUtils
                        .printIfErrorEnabled(LOGGER, "[{}]Send request fail, request={}, retryTimes={},errorMessage={}",
                                name, request, retryTimes, e.getMessage());
                exceptionToThrow = e;

            }
        }

        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }

        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request future fail, unknown error");
        }

    }

    /**
     * connect to server.
     *
     * @param serverInfo server address to connect.
     * @return return connection when successfully connect to server, or null if failed.
     * @throws Exception exception when fail to connect to server.
     */
    public abstract Connection connectToServer(ServerInfo serverInfo) throws Exception;

    /**
     * handle server request.
     *
     * @param request request.
     * @return response.
     */
    protected Response handleServerRequest(final Request request) {

        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]receive server push request,request={},requestId={}", name,
                request.getClass().getSimpleName(), request.getRequestId());
        lastActiveTimeStamp = System.currentTimeMillis();
        // 处理来自服务器端匹配的请求
        for (ServerRequestHandler serverRequestHandler : serverRequestHandlers) {
            try {
                Response response = serverRequestHandler.requestReply(request);

                if (response != null) {
                    LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]ack server push request,request={},requestId={}", name,
                            request.getClass().getSimpleName(), request.getRequestId());
                    return response;
                }
            } catch (Exception e) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]handleServerRequest:{}, errorMessage={}", name,
                        serverRequestHandler.getClass().getName(), e.getMessage());
            }

        }
        return null;
    }

    /**
     * Register connection handler. Will be notified when inner connection's state changed.
     *
     * @param connectionEventListener connectionEventListener
     */
    public synchronized void registerConnectionListener(ConnectionEventListener connectionEventListener) {

        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Registry connection listener to current client:{}", name,
                connectionEventListener.getClass().getName());
        this.connectionEventListeners.add(connectionEventListener);
    }

    /**
     * Register serverRequestHandler, the handler will handle the request from server side.
     *
     * @param serverRequestHandler serverRequestHandler
     */
    public synchronized void registerServerRequestHandler(ServerRequestHandler serverRequestHandler) {
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Register server push request handler:{}", name,
                serverRequestHandler.getClass().getName());

        this.serverRequestHandlers.add(serverRequestHandler);
    }

    /**
     * Getter method for property <tt>name</tt>.
     *
     * @return property value of name
     */
    public String getName() {
        return name;
    }

    /**
     * Setter method for property <tt>name</tt>.
     *
     * @param name value to be assigned to property name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Getter method for property <tt>serverListFactory</tt>.
     *
     * @return property value of serverListFactory
     */
    public ServerListFactory getServerListFactory() {
        return serverListFactory;
    }

    protected ServerInfo nextRpcServer() {
        String serverAddress = getServerListFactory().genNextServer();
        return resolveServerInfo(serverAddress);
    }

    protected ServerInfo currentRpcServer() {
        String serverAddress = getServerListFactory().getCurrentServer();
        return resolveServerInfo(serverAddress);
    }

    /**
     * resolve server info.
     *
     * @param serverAddress address.
     * @return
     */
    @SuppressWarnings("PMD.UndefineMagicConstantRule")
    private ServerInfo resolveServerInfo(String serverAddress) {
        String property = System.getProperty("nacos.server.port", "8848");
        int serverPort = Integer.parseInt(property);
        ServerInfo serverInfo = null;
        if (serverAddress.contains(Constants.HTTP_PREFIX)) {
            String[] split = serverAddress.split(Constants.COLON);
            String serverIp = split[1].replaceAll("//", "");
            if (split.length > 2 && StringUtils.isNotBlank(split[2])) {
                serverPort = Integer.parseInt(split[2]);
            }
            serverInfo = new ServerInfo(serverIp, serverPort);
        } else {
            String[] split = serverAddress.split(Constants.COLON);
            String serverIp = split[0];
            if (split.length > 1 && StringUtils.isNotBlank(split[1])) {
                serverPort = Integer.parseInt(split[1]);
            }
            serverInfo = new ServerInfo(serverIp, serverPort);
        }
        return serverInfo;
    }

    public static class ServerInfo {

        protected String serverIp;

        protected int serverPort;

        public ServerInfo() {

        }

        public ServerInfo(String serverIp, int serverPort) {
            this.serverPort = serverPort;
            this.serverIp = serverIp;
        }

        /**
         * get address, ip:port.
         *
         * @return address.
         */
        public String getAddress() {
            return serverIp + Constants.COLON + serverPort;
        }

        /**
         * Setter method for property <tt>serverIp</tt>.
         *
         * @param serverIp value to be assigned to property serverIp
         */
        public void setServerIp(String serverIp) {
            this.serverIp = serverIp;
        }

        /**
         * Setter method for property <tt>serverPort</tt>.
         *
         * @param serverPort value to be assigned to property serverPort
         */
        public void setServerPort(int serverPort) {
            this.serverPort = serverPort;
        }

        /**
         * Getter method for property <tt>serverIp</tt>.
         *
         * @return property value of serverIp
         */
        public String getServerIp() {
            return serverIp;
        }

        /**
         * Getter method for property <tt>serverPort</tt>.
         *
         * @return property value of serverPort
         */
        public int getServerPort() {
            return serverPort;
        }

        @Override
        public String toString() {
            return "{serverIp='" + serverIp + '\'' + ", server main port=" + serverPort + '}';
        }
    }

    public class ConnectionEvent {

        public static final int CONNECTED = 1;

        public static final int DISCONNECTED = 0;

        int eventType;

        public ConnectionEvent(int eventType) {
            this.eventType = eventType;
        }

        public boolean isConnected() {
            return eventType == CONNECTED;
        }

        public boolean isDisConnected() {
            return eventType == DISCONNECTED;
        }
    }

    /**
     * Getter method for property <tt>labels</tt>.
     *
     * @return property value of labels
     */
    public Map<String, String> getLabels() {
        return labels;
    }

    class ReconnectContext {

        public ReconnectContext(ServerInfo serverInfo, boolean onRequestFail) {
            this.onRequestFail = onRequestFail;
            this.serverInfo = serverInfo;
        }

        boolean onRequestFail;

        ServerInfo serverInfo;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
}
