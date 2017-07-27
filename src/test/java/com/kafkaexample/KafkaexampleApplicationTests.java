package com.kafkaexample;

import com.kafkaexample.service.Receiver;
import com.kafkaexample.service.Sender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaexampleApplicationTests {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaexampleApplicationTests.class);
	private static final String HELLOWORLD_TOPIC = "helloworld.t";
	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, HELLOWORLD_TOPIC);

	@Autowired
	private Sender sender;

	@Autowired
	private Receiver receiver;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.setProperty("kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
		System.setProperty("kafka.topic.helloworld", "helloworld.t");
	}


	@Before
	public void setUp() {
		kafkaListenerEndpointRegistry.getListenerContainers().forEach(mc ->{
			try {
				ContainerTestUtils.waitForAssignment(mc, embeddedKafka.getPartitionsPerTopic());
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		});

	}

	@Test
	public void testReceive() throws InterruptedException {
		sender.send(HELLOWORLD_TOPIC, "Hello Spring Kafka");
		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
	}

}
