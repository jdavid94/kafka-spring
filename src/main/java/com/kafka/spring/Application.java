package com.kafka.spring;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;


@SpringBootApplication
public class Application  {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private MeterRegistry meterRegistry;

/*	@Autowired
	private KafkaListenerEndpointRegistry registry;*/

	public static final Logger log = LoggerFactory.getLogger(Application.class);

	@KafkaListener(id = "devs4jId", autoStartup = "true", topics = "devs4j-topic", containerFactory = "listenerContainerFactory" , groupId = "devs4j-group"
	, properties = {"max.poll.interval.ms:4000", "max.poll.records:50"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Message received {} ", messages.size());
		log.info("Start reading messages");
		for	(ConsumerRecord<String, String> message:messages) {
			/*log.info("Partion = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(),
					message.value());*/
		}
		log.info("Batch Completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		for (int i=0; i<200; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample Message %d", i));
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetric() {
		List<Meter> metrics = meterRegistry.getMeters();
		for (Meter meter:metrics) {
			log.info("Meter = {} ", meter.getId().getName());
		}
		double count =
				meterRegistry.get("kafka.producer.record.send.total")
						.functionCounter().count();
		log.info("Count {} ",count);
	}

	/*@Override
	public void run(String... args) throws Exception {
		*//*CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample Message"); // .get() for Sync
		// .get(100, TimeUnit.MILLISECONDS) If not TimeoutException.
		future.whenComplete((result, ex) -> {
			log.info("Message sent", result.getRecordMetadata().offset());
		});*//*
		for (int i=0; i<100; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample Message %d", i));
			*//*log.info("Waiting to start");
			Thread.sleep(5000);
			log.info("Starting consuming messages");
			registry.getListenerContainer("devs4jId").start();
			Thread.sleep(5000);
			registry.getListenerContainer("devs4jId").stop();*//*
		}
	}*/
}
