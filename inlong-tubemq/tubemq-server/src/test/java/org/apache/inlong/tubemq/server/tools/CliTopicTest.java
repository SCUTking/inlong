package org.apache.inlong.tubemq.server.tools;

import com.sleepycat.je.Durability;
import org.apache.inlong.tubemq.manager.controller.node.request.AddTopicReq;
import org.apache.inlong.tubemq.manager.controller.node.request.QueryTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.DeleteTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.ModifyTopicReq;
import org.apache.inlong.tubemq.manager.service.TubeConst;
import org.apache.inlong.tubemq.server.common.fileconfig.BdbMetaConfig;
import org.apache.inlong.tubemq.server.master.MasterConfig;
import org.apache.inlong.tubemq.server.master.nodemanage.nodeconsumer.ConsumerEventManager;
import org.apache.inlong.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import org.apache.inlong.tubemq.server.tools.cli.CliTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

import static org.mockito.Mockito.mock;

public class CliTopicTest {

    private CliTopic cliTopic;

    @Before
    public void setUp() throws Exception {
        //初始化命令行工具
        cliTopic=new CliTopic();
        cliTopic.masterIp="192.168.10.139";
        cliTopic.user="lqh";
        cliTopic.brokerId="1";
        cliTopic.port="8080";
        cliTopic.masterPort="8080";
    }


    @Test
    public void testTopicConfig() throws Exception {
        Properties properties = new Properties();
        FileInputStream fileInputStream = null;

        try {
            // 加载配置文件
            fileInputStream = new FileInputStream( "/conf/topicConfig.properties");
            properties.load(fileInputStream);
            // 读取配置项
            Assert.assertEquals("192.168.10.139", properties.getProperty("TUBE_MASTER_IP"));
            Assert.assertEquals("8080", properties.getProperty("TUBE_MASTER_WEBPORT"));
            Assert.assertEquals("8715", properties.getProperty("TUBE_MASTER_PORT"));
            Assert.assertEquals("abc", properties.getProperty("TUBE_MASTER_TOKEN"));
            Assert.assertEquals("lqh", properties.getProperty("TUBE_MASTER_USER"));
            Assert.assertEquals("1", properties.getProperty("TUBE_BROKER_ID"));
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

    @Test
    public void testAddTopic() throws Exception {
        AddTopicReq addTopicReq = new AddTopicReq();
        addTopicReq.setType(TubeConst.OP_MODIFY);
        addTopicReq.setMethod("admin_add_new_topic_record");
        // 必填项
        addTopicReq.setTopicName("test1");
        // 配置文件的注入
        addTopicReq.setBrokerId("1");
        addTopicReq.setCreateUser("lqh");
        // 选填项
//        addTopicReq.setAcceptPublish(acceptPublish);
//        addTopicReq.setAcceptSubscribe(acceptSubscribe);
//        addTopicReq.setDeletePolicy(deletePolicy);
//        addTopicReq.setDeleteWhen(deleteWhen);
//        addTopicReq.setUnflushInterval(unflushInterval);
//        addTopicReq.setNumTopicStores(numTopicStores);
//        addTopicReq.setUnflushThreshold(unflushThreshold);
//        addTopicReq.setMemCacheFlushIntvl(memCacheFlushIntvl);
//        addTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
//        addTopicReq.setMaxMsgSizeInMB(maxMsgSizeInMB);
//        addTopicReq.setUnflushDataHold(unflushDataHold);
//        addTopicReq.setNumPartitions(numPartitions);

        cliTopic.baseRequestMaster(addTopicReq);
    }

    @Test
    public void testDeleteTopic() throws Exception {
        DeleteTopicReq deleteTopicReq = new DeleteTopicReq();
        // BaseReq必填
        deleteTopicReq.setType(TubeConst.OP_MODIFY);
        deleteTopicReq.setMethod("admin_delete_topic_info");
        // 必填项
        deleteTopicReq.setTopicName("test1");
        // 配置文件的注入
        deleteTopicReq.setBrokerId("1");
        deleteTopicReq.setModifyUser("lqh");
        // 选填项

        cliTopic.baseRequestMaster(deleteTopicReq);

    }


    @Test
    public void testRemoveTopic() throws Exception {
        DeleteTopicReq deleteTopicReq = new DeleteTopicReq();
        // BaseReq必填
        deleteTopicReq.setType(TubeConst.OP_MODIFY);
        deleteTopicReq.setMethod("admin_delete_topic_info");
        // 必填项
        deleteTopicReq.setTopicName("test1");
        // 配置文件的注入
        deleteTopicReq.setBrokerId("1");
        deleteTopicReq.setModifyUser("lqh");
        // 选填项

        cliTopic.baseRequestMaster(deleteTopicReq);

    }


    @Test
    public void testQueryTopic() throws Exception {
        QueryTopicReq queryTopicReq = new QueryTopicReq();
        // BaseReq必填
        queryTopicReq.setType("op_query");
        queryTopicReq.setMethod("admin_query_topic_info");
        // 必填项
        queryTopicReq.setTopicName("test1");
        // 配置文件的注入
        queryTopicReq.setBrokerId("1");
        queryTopicReq.setModifyUser("lqh");
        // 选填项
//        queryTopicReq.setTopicStatusId(topicStatusId);
//        queryTopicReq.setDeletePolicy(deletePolicy);
//        queryTopicReq.setDeleteWhen(deleteWhen);
//        queryTopicReq.setUnflushInterval(unflushInterval);
//        queryTopicReq.setNumTopicStores(numTopicStores);
//        queryTopicReq.setUnflushInterval(unflushInterval);
//        queryTopicReq.setUnflushThreshold(unflushThreshold);
//        queryTopicReq.setMemCacheMsgSizeInMB(memCacheMsgSizeInMb);
//        queryTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
//        queryTopicReq.setMemCacheFlushIntvl(memCacheFlushIntvl);
//        queryTopicReq.setNumPartitions(numPartitions);
//        queryTopicReq.setUnflushDataHold(unflushDataHold);
        // 选填项

        cliTopic.baseRequestMaster(queryTopicReq);

    }

    @Test
    public void testModifyTopic() throws Exception {
        ModifyTopicReq modifyTopicReq = new ModifyTopicReq();
        // BaseReq必填
        modifyTopicReq.setType(TubeConst.OP_MODIFY);
        modifyTopicReq.setMethod("admin_modify_topic_info");
        // 必填项
        modifyTopicReq.setTopicName("test1");
        // 配置文件的注入
        modifyTopicReq.setBrokerId("1");
        modifyTopicReq.setModifyUser("lqh");
        // 选填项
//        modifyTopicReq.setAcceptPublish(acceptPublish);
//        modifyTopicReq.setAcceptSubscribe(acceptSubscribe);
//        modifyTopicReq.setDeletePolicy(deletePolicy);
//        modifyTopicReq.setDeleteWhen(deleteWhen);
//        modifyTopicReq.setUnflushInterval(unflushInterval);
//        modifyTopicReq.setNumTopicStores(numTopicStores);
//        modifyTopicReq.setUnflushInterval(unflushInterval);
//        modifyTopicReq.setUnflushThreshold(unflushThreshold);
//        modifyTopicReq.setMemCacheMsgSizeInMB(memCacheMsgSizeInMb);
//        modifyTopicReq.setMemCacheMsgCntInK(memCacheMsgCntInK);
//        modifyTopicReq.setNumPartitions(numPartitions);
//        modifyTopicReq.setUnflushDataHold(unflushDataHold);
        cliTopic.baseRequestMaster(modifyTopicReq);

    }




}
