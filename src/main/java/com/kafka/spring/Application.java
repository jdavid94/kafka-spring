package com.kafka.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import org.springframework.kafka.core.KafkaTemplate;
import java.util.List;


@SpringBootApplication
public class Application implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public static final Logger log = LoggerFactory.getLogger(Application.class);

	@KafkaListener(topics = "devs4j-topic", containerFactory = "listenerContainerFactory" , groupId = "devs4j-group"
	, properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Start reading messages");
		for	(ConsumerRecord<String, String> message:messages) {
			log.info("Partion = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(),
					message.value());
		}
		log.info("Batch Completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		/*CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample Message"); // .get() for Sync
		// .get(100, TimeUnit.MILLISECONDS) If not TimeoutException.
		future.whenComplete((result, ex) -> {
			log.info("Message sent", result.getRecordMetadata().offset());
		});*/
		for (int i=0; i<100; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample Message %d", i));
		}
	}
}
