package com.xiaojukeji.know.streaming.km;

import com.xiaojukeji.know.streaming.km.common.bean.dto.cluster.ClusterPhyAddDTO;
import com.xiaojukeji.know.streaming.km.common.bean.entity.config.JmxConfig;
import com.xiaojukeji.know.streaming.km.common.converter.ClusterConverter;
import com.xiaojukeji.know.streaming.km.common.enums.version.VersionEnum;
import com.xiaojukeji.know.streaming.km.core.service.cluster.ClusterPhyService;
import com.xiaojukeji.know.streaming.km.rest.KnowStreaming;
import com.xiaojukeji.know.streaming.test.KMTestEnvService;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ActiveProfiles("test")
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = KnowStreaming.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, value = "server.port=19091")
public class DemoAppTest extends KMTestEnvService {

  @Resource
  private ClusterPhyService clusterPhyService;

  @Test
  void startDemo() throws Exception {
    Properties properties = new Properties();
    JmxConfig jmxConfig = new JmxConfig();
    jmxConfig.setJmxPort(jmxPort());
    jmxConfig.setOpenSSL(false);

    ClusterPhyAddDTO dto = new ClusterPhyAddDTO();
    dto.setName("kafka-2.5.1");
    dto.setDescription("The default single node Test Cluster");
    dto.setKafkaVersion(VersionEnum.V_2_5_1.getVersion());
    dto.setJmxProperties(jmxConfig);
    dto.setClientProperties(properties);
    dto.setZookeeper(zookeeperUrl());
    dto.setBootstrapServers(bootstrapServers());
    assertEquals(1, clusterPhyService.addClusterPhy(ClusterConverter.convert2ClusterPhyPO(dto), "admin"));

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    while (true) {
      ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", new Date().toString(), "Hello, World!");
      producer.send(record);
      Thread.sleep(1000);
    }
  }
}