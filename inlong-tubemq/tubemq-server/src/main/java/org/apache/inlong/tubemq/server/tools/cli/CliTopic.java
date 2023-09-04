/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.tools.cli;

import org.apache.inlong.tubemq.client.common.PeerInfo;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.MessageListener;
import org.apache.inlong.tubemq.client.consumer.PushMessageConsumer;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.manager.controller.TubeMQResult;
import org.apache.inlong.tubemq.manager.controller.node.request.AddTopicReq;
import org.apache.inlong.tubemq.manager.controller.node.request.BaseReq;
import org.apache.inlong.tubemq.manager.controller.node.request.QueryTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.DeleteTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.ModifyTopicReq;
import org.apache.inlong.tubemq.manager.service.TubeConst;
import org.apache.inlong.tubemq.manager.utils.ConvertUtils;
import org.apache.inlong.tubemq.server.common.fielddef.CliArgDef;

import com.google.gson.Gson;
import lombok.Data;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.tubemq.manager.controller.TubeMQResult.errorResult;

public class CliTopic extends CliAbstractBase {

    private static final String PRODUCE= "produce";
    private static final String CONSUME= "consume";

    private static final Logger logger =
            LoggerFactory.getLogger(CliTopic.class);

    // 配置文件读取
    public String masterIp;
    public String masterPort;
    public String port;
    public String token;
    public String brokerId;
    public String user;

    private Integer topicStatusId;
    private String methodType;
    private String topicName;
    private Integer numPartitions;
    private Integer numTopicStores;
    private String deleteWhen;
    private Integer unflushInterval;
    private Integer unflushThreshold;
    private Integer unflushDataHold;
    private Integer memCacheMsgCntInK;
    private Integer memCacheFlushIntvl;
    private Integer maxMsgSizeInMB;
    private Integer memCacheMsgSizeInMb;
    private String deletePolicy;
    private Boolean acceptPublish;
    private Boolean acceptSubscribe;

    public CliTopic() {
        super("tubemq-topic.sh");
        initCommandOptions();
    }

    @Override
    protected void initCommandOptions() {
        // add the cli required parameters

        // topic's CRUD
        addCommandOption(CliArgDef.TUBEMQMETHOD);
        addCommandOption(CliArgDef.TOPICNAME);
        addCommandOption(CliArgDef.DELETEWHEN);
        addCommandOption(CliArgDef.DELETEPOLICY);
        addCommandOption(CliArgDef.NUMPARTITIONS);
        addCommandOption(CliArgDef.UNFLUSHTHRESHOLD);
        addCommandOption(CliArgDef.UNFLUSHINTERVAL);
        addCommandOption(CliArgDef.UNFLUSHDATAHOLD);
        addCommandOption(CliArgDef.NUMTOPICSTORES);
        addCommandOption(CliArgDef.MEMCACHEFLUSHINTVL);
        addCommandOption(CliArgDef.MEMCACHEMSGCNTINK);
        addCommandOption(CliArgDef.MEMCACHEMSGSIZEINMB);
        addCommandOption(CliArgDef.ACCEPTPUBLISH);
        addCommandOption(CliArgDef.ACCEPTSUBSCRIBE);
        addCommandOption(CliArgDef.MAXMSGSIZEINMB);

        // 生产和消费消息
        addCommandOption(CliArgDef.SENDMESSAGE);
        addCommandOption(CliArgDef.GROUP);

    }

    @Override
    public boolean processParams(String[] args) throws Exception {
        return true;
    }

    public void initConfig(String base_url) throws Exception {
        Properties properties = new Properties();
        FileInputStream fileInputStream = null;

        try {
            // 加载配置文件
            fileInputStream = new FileInputStream(base_url + "/conf/topicConfig.properties");
            properties.load(fileInputStream);
            // 读取配置项
            masterIp = properties.getProperty("TUBE_MASTER_IP");
            masterPort = properties.getProperty("TUBE_MASTER_WEBPORT");
            port = properties.getProperty("TUBE_MASTER_PORT");
            token = properties.getProperty("TUBE_MASTER_TOKEN");
            user = properties.getProperty("TUBE_MASTER_USER");
            brokerId = properties.getProperty("TUBE_BROKER_ID");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 根据命令行传过来的args  解析成req
     * @param args
     * @return
     * @throws Exception
     */
    public BaseReq argsToReq(String[] args, boolean isNeedTopicName) throws Exception {

        // parse parameters and check value
        CommandLine cli = parser.parse(options, args);

        if (cli == null) {
            throw new ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }

        // 几种操作里面都需要做的
        methodType = cli.getOptionValue(CliArgDef.TUBEMQMETHOD.longOpt);
        if (TStringUtils.isBlank(methodType)) {
            throw new Exception(CliArgDef.TOPICNAME.longOpt + " is required!");
        }

        topicName = cli.getOptionValue(CliArgDef.TOPICNAME.longOpt);
        if (TStringUtils.isBlank(topicName)) {
            if (isNeedTopicName) {
                throw new Exception(CliArgDef.TOPICNAME.longOpt + " is required!");
            }
        }

        // addCommandOption(CliArgDef.TUBEMQMETHOD);
        // addCommandOption(CliArgDef.TOPICNAME);

        deleteWhen = cli.getOptionValue(CliArgDef.DELETEWHEN.longOpt);
        deletePolicy = cli.getOptionValue(CliArgDef.DELETEPOLICY.longOpt);
        String numPartitionsStr = cli.getOptionValue(CliArgDef.NUMPARTITIONS.longOpt);
        if (TStringUtils.isNotBlank(numPartitionsStr)) {
            numPartitions = Integer.parseInt(numPartitionsStr);
        }

        String unflushThresholdStr = cli.getOptionValue(CliArgDef.UNFLUSHTHRESHOLD.longOpt);
        if (TStringUtils.isNotBlank(unflushThresholdStr)) {
            unflushThreshold = Integer.parseInt(unflushThresholdStr);
        }

        String unflushIntervalStr = cli.getOptionValue(CliArgDef.UNFLUSHINTERVAL.longOpt);
        if (TStringUtils.isNotBlank(unflushIntervalStr)) {
            unflushInterval = Integer.parseInt(unflushIntervalStr);
        }

        String unflushDataHoldStr = cli.getOptionValue(CliArgDef.UNFLUSHDATAHOLD.longOpt);
        if (TStringUtils.isNotBlank(unflushDataHoldStr)) {
            unflushDataHold = Integer.parseInt(unflushDataHoldStr);
        }

        String numTopicStoresStr = cli.getOptionValue(CliArgDef.NUMTOPICSTORES.longOpt);
        if (TStringUtils.isNotBlank(numTopicStoresStr)) {
            numTopicStores = Integer.parseInt(numTopicStoresStr);
        }

        String memCacheFlushIntvlStr = cli.getOptionValue(CliArgDef.MEMCACHEFLUSHINTVL.longOpt);
        if (TStringUtils.isNotBlank(memCacheFlushIntvlStr)) {
            memCacheFlushIntvl = Integer.parseInt(memCacheFlushIntvlStr);
        }

        String memCacheMsgCntInKStr = cli.getOptionValue(CliArgDef.MEMCACHEMSGCNTINK.longOpt);
        if (TStringUtils.isNotBlank(memCacheMsgCntInKStr)) {
            memCacheMsgCntInK = Integer.parseInt(memCacheMsgCntInKStr);
        }

        String memCacheMsgSizeInMbStr = cli.getOptionValue(CliArgDef.MEMCACHEMSGSIZEINMB.longOpt);
        if (TStringUtils.isNotBlank(memCacheMsgSizeInMbStr)) {
            memCacheMsgSizeInMb = Integer.parseInt(memCacheMsgSizeInMbStr);
        }

        String maxMsgSizeInMBStr = cli.getOptionValue(CliArgDef.MAXMSGSIZEINMB.longOpt);
        if (TStringUtils.isNotBlank(maxMsgSizeInMBStr)) {
            maxMsgSizeInMB = Integer.parseInt(maxMsgSizeInMBStr);
        }

        String acceptPublishStr = cli.getOptionValue(CliArgDef.ACCEPTPUBLISH.longOpt);
        if (TStringUtils.isNotBlank(acceptPublishStr)) {
            acceptPublish = Boolean.parseBoolean(acceptPublishStr);
        }

        String acceptSubscribeStr = cli.getOptionValue(CliArgDef.ACCEPTSUBSCRIBE.longOpt);
        if (TStringUtils.isNotBlank(acceptSubscribeStr)) {
            acceptSubscribe = Boolean.parseBoolean(acceptSubscribeStr);
        }

        String topicStatusStr = cli.getOptionValue(CliArgDef.TOPICSTATUS.longOpt);
        if (TStringUtils.isNotBlank(topicStatusStr)) {
            topicStatusId = Integer.parseInt(topicStatusStr);
        }

        switch (methodType) {
            case TubeConst.ADD: {
                AddTopicReq addTopicReq = new AddTopicReq();
                // BaseReq必填
                addTopicReq.setType(TubeConst.OP_MODIFY);
                addTopicReq.setMethod("admin_add_new_topic_record");
                // 必填项
                addTopicReq.setTopicName(topicName);
                // 配置文件的注入
                addTopicReq.setBrokerId(brokerId);
                addTopicReq.setCreateUser(user);
                // 选填项
                addTopicReq.setAcceptPublish(acceptPublish);
                addTopicReq.setAcceptSubscribe(acceptSubscribe);
                addTopicReq.setDeletePolicy(deletePolicy);
                addTopicReq.setDeleteWhen(deleteWhen);
                addTopicReq.setUnflushInterval(unflushInterval);
                addTopicReq.setNumTopicStores(numTopicStores);
                addTopicReq.setUnflushThreshold(unflushThreshold);
                addTopicReq.setMemCacheFlushIntvl(memCacheFlushIntvl);
                addTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
                addTopicReq.setMaxMsgSizeInMB(maxMsgSizeInMB);
                addTopicReq.setUnflushDataHold(unflushDataHold);
                addTopicReq.setNumPartitions(numPartitions);

                return addTopicReq;
            }
            case TubeConst.DELETE: {
                DeleteTopicReq deleteTopicReq = new DeleteTopicReq();
                // BaseReq必填
                deleteTopicReq.setType(TubeConst.OP_MODIFY);
                deleteTopicReq.setMethod("admin_delete_topic_info");
                // 必填项
                deleteTopicReq.setTopicName(topicName);
                // 配置文件的注入
                deleteTopicReq.setBrokerId(brokerId);
                deleteTopicReq.setModifyUser(user);
                // 选填项

                return deleteTopicReq;
            }
            case TubeConst.REMOVE: {
                DeleteTopicReq deleteTopicReq = new DeleteTopicReq();
                // BaseReq必填
                deleteTopicReq.setType(TubeConst.OP_MODIFY);
                deleteTopicReq.setMethod("admin_remove_topic_info");
                // 必填项
                deleteTopicReq.setTopicName(topicName);
                // 配置文件的注入
                deleteTopicReq.setBrokerId(brokerId);
                deleteTopicReq.setModifyUser(user);
                // 选填项

                return deleteTopicReq;
            }
            case TubeConst.QUERY: {
                QueryTopicReq queryTopicReq = new QueryTopicReq();
                // BaseReq必填
                queryTopicReq.setType("op_query");
                queryTopicReq.setMethod("admin_query_topic_info");
                // 必填项
                queryTopicReq.setTopicName(topicName);
                // 配置文件的注入
                queryTopicReq.setBrokerId(brokerId);
                queryTopicReq.setModifyUser(user);
                // 选填项
                queryTopicReq.setTopicStatusId(topicStatusId);
                queryTopicReq.setDeletePolicy(deletePolicy);
                queryTopicReq.setDeleteWhen(deleteWhen);
                queryTopicReq.setUnflushInterval(unflushInterval);
                queryTopicReq.setNumTopicStores(numTopicStores);
                queryTopicReq.setUnflushInterval(unflushInterval);
                queryTopicReq.setUnflushThreshold(unflushThreshold);
                queryTopicReq.setMemCacheMsgSizeInMB(memCacheMsgSizeInMb);
                queryTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
                queryTopicReq.setMemCacheFlushIntvl(memCacheFlushIntvl);
                queryTopicReq.setNumPartitions(numPartitions);
                queryTopicReq.setUnflushDataHold(unflushDataHold);

                return queryTopicReq;
            }
            case TubeConst.MODIFY: {
                ModifyTopicReq modifyTopicReq = new ModifyTopicReq();
                // BaseReq必填
                modifyTopicReq.setType(TubeConst.OP_MODIFY);
                modifyTopicReq.setMethod("admin_modify_topic_info");
                // 必填项
                modifyTopicReq.setTopicName(topicName);
                // 配置文件的注入
                modifyTopicReq.setBrokerId(brokerId);
                modifyTopicReq.setModifyUser(user);
                // 选填项
                modifyTopicReq.setAcceptPublish(acceptPublish);
                modifyTopicReq.setAcceptSubscribe(acceptSubscribe);
                modifyTopicReq.setDeletePolicy(deletePolicy);
                modifyTopicReq.setDeleteWhen(deleteWhen);
                modifyTopicReq.setUnflushInterval(unflushInterval);
                modifyTopicReq.setNumTopicStores(numTopicStores);
                modifyTopicReq.setUnflushInterval(unflushInterval);
                modifyTopicReq.setUnflushThreshold(unflushThreshold);
                modifyTopicReq.setMemCacheMsgSizeInMB(memCacheMsgSizeInMb);
                modifyTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
                modifyTopicReq.setNumPartitions(numPartitions);
                modifyTopicReq.setUnflushDataHold(unflushDataHold);
                return modifyTopicReq;

            }
            default: {
                return new BaseReq();
            }
        }

    }

    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    private static Gson gson = new Gson();

    public TubeMQResult baseRequestMaster(BaseReq req) {

        // 需要配置文件中获取
        // 不需要ClusterId
        // req.setClusterId("");
        // ClusterId不知道要不要
        // if (req.getClusterId() == null) {
        // return TubeMQResult.errorResult("please input clusterId");
        // }
        String url = TubeConst.SCHEMA + masterIp + ":" + masterPort
                + "/" + TubeConst.TUBE_REQUEST_PATH + "?" + TubeConst.CONF_MOD_AUTH_TOKEN + token + "&"
                + ConvertUtils.convertReqToQueryStr(req);
        return requestMaster(url);
    }

    @Data
    public class TubeHttpReq {

        private boolean result;

        private String errMsg;

        private int errCode;

        private Object data;

        private int count;
    }

    public TubeMQResult requestMaster(String url) {
        logger.info("start to request {}", url);
        HttpGet httpGet = new HttpGet(url);
        TubeMQResult defaultResult = new TubeMQResult();

        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity);
            TubeHttpReq tubeHttpReq = gson.fromJson(responseBody, TubeHttpReq.class);
            // 判断请求是否是正常的
            if (tubeHttpReq.getErrCode() == TubeConst.SUCCESS_CODE
                    && tubeHttpReq.getErrMsg().equals("OK")) {
                // 获取返回信息 并放入TubeMqResult中
                defaultResult = TubeMQResult.successResult(tubeHttpReq.getData());
                // 判断操作是否正常
                return defaultResult;
            } else {
                defaultResult = errorResult(tubeHttpReq.getErrMsg());
            }
        } catch (Exception ex) {
            logger.error("exception caught while requesting broker status", ex);
            defaultResult = TubeMQResult.errorResult(ex.getMessage());
        }
        return defaultResult;
    }

    public void produceMsg(String[] args) throws Throwable {

        // parse parameters and check value
        CommandLine cli = parser.parse(options, args);
        if (cli == null) {
            throw new ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }

        topicName = cli.getOptionValue(CliArgDef.TOPICNAME.longOpt);
        if (TStringUtils.isBlank(topicName)) {
            throw new Exception(CliArgDef.TOPICNAME.longOpt + " is required!");
        }

        String msg = cli.getOptionValue(CliArgDef.SENDMESSAGE.longOpt);
        if (TStringUtils.isBlank(msg)) {
            throw new Exception(CliArgDef.SENDMESSAGE.longOpt + " is required!");
        }
        final String masterHostAndPort = masterIp + ":" + port;
        final TubeClientConfig clientConfig = new TubeClientConfig(masterHostAndPort);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        final MessageProducer messageProducer = messageSessionFactory.createProducer();
        final String topic = topicName;
        final String body = msg;
        byte[] bodyData = StringUtils.getBytesUtf8(body);
        messageProducer.publish(topic);
        Message message = new Message(topic, bodyData);
        MessageSentResult result = messageProducer.sendMessage(message);
        if (result.isSuccess()) {
            System.out.println("sync send message : " + message);
        }
        messageProducer.shutdown();
    }
    public void consumeMsg(String[] args) throws Throwable {
        // parse parameters and check value
        CommandLine cli = parser.parse(options, args);
        if (cli == null) {
            throw new ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }

        final String masterHostAndPort = masterIp + ":" + port;
        topicName = cli.getOptionValue(CliArgDef.TOPICNAME.longOpt);
        if (TStringUtils.isBlank(topicName)) {
            throw new Exception(CliArgDef.TOPICNAME.longOpt + " is required!");
        }

        String groupName = cli.getOptionValue(CliArgDef.GROUP.longOpt);
        if (TStringUtils.isBlank(groupName)) {
            throw new Exception(CliArgDef.GROUP.longOpt + " is required!");
        }
        final String topic = topicName;
        final String group = groupName;
        final ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        final PushMessageConsumer pushConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
        pushConsumer.subscribe(topic, null, new MessageListener() {

            @Override
            public void receiveMessages(PeerInfo peerInfo, List<Message> messages) throws InterruptedException {
                for (Message message : messages) {
                    System.out.println("received message : " + new String(message.getData()));
                }
            }
            @Override
            public Executor getExecutor() {
                return null;
            }
            @Override
            public void stop() {
                //
            }
        });
        pushConsumer.completeSubscribe();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.MINUTES);
    }

    public static void main(String[] args) {

        String base_url = args[0];

        CliTopic cliTopic = new CliTopic();
        try {
            cliTopic.initConfig(base_url);

            CommandLine cli = cliTopic.parser.parse(cliTopic.options, args);
            String methodType = cli.getOptionValue(CliArgDef.TUBEMQMETHOD.longOpt);
            if (TStringUtils.isBlank(methodType)) {
                throw new Exception(CliArgDef.TUBEMQMETHOD.longOpt + " is required!");
            }
            TubeMQResult tubeMQResult = null;
            String describe = "";
            switch (methodType) {
                case TubeConst.ADD: {
                    AddTopicReq addTopicReq = (AddTopicReq) cliTopic.argsToReq(args, true);
                    tubeMQResult = cliTopic.baseRequestMaster(addTopicReq);
                    describe = "\t添加Topic请求成功\t\n返回体如下：";
                    System.out.println("请求体：" + addTopicReq);
                    break;
                }
                case TubeConst.DELETE: {
                    DeleteTopicReq deleteTopicReq = (DeleteTopicReq) cliTopic.argsToReq(args, true);
                    tubeMQResult = cliTopic.baseRequestMaster(deleteTopicReq);
                    describe = "\t软删除Topic请求成功\t\n返回体如下：";
                    System.out.println("请求体：" + deleteTopicReq);
                    break;
                }
                case TubeConst.REMOVE: {
                    DeleteTopicReq deleteTopicReq = (DeleteTopicReq) cliTopic.argsToReq(args, true);
                    tubeMQResult = cliTopic.baseRequestMaster(deleteTopicReq);
                    describe = "\t强删除Topic请求成功\t\n返回体如下：";
                    System.out.println("请求体：" + deleteTopicReq);
                    break;
                }
                case TubeConst.QUERY: {
                    QueryTopicReq queryTopicReq = (QueryTopicReq) cliTopic.argsToReq(args, false);
                    tubeMQResult = cliTopic.baseRequestMaster(queryTopicReq);
                    describe = "\t查询Topic请求成功\t\n返回体如下：";
                    System.out.println("请求体：" + queryTopicReq);
                    break;
                }
                case TubeConst.MODIFY: {
                    ModifyTopicReq modifyTopicReq = (ModifyTopicReq) cliTopic.argsToReq(args, true);
                    tubeMQResult = cliTopic.baseRequestMaster(modifyTopicReq);
                    describe = "\t修改Topic请求成功\t\n返回体如下：";
                    System.out.println("请求体：" + modifyTopicReq);
                    break;
                }
                case PRODUCE: {
                    cliTopic.produceMsg(args);
                    break;
                }
                case CONSUME: {
                    cliTopic.consumeMsg(args);
                    break;
                }
                default:
                    throw new Exception(CliArgDef.TUBEMQMETHOD.longOpt + " is wrong!");
            }
            if (!tubeMQResult.isError()) {
                System.out.println(describe + tubeMQResult.getData());
            } else {
                System.out.println(tubeMQResult.getErrMsg());
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            cliTopic.help();
        }
    }
}
