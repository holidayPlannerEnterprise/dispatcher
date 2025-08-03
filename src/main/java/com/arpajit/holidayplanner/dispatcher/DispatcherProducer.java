package com.arpajit.holidayplanner.dispatcher;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import com.arpajit.holidayplanner.dispatcher.dto.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DispatcherProducer {
    private static final Logger logger = LoggerFactory.getLogger(DispatcherProducer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DispatcherDataServComm dataService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void dropToController(byte[] correlationId, KafkaMessage messageDTO) throws Exception {
        // Preparing Controller topic message
        String message = objectMapper.writeValueAsString(messageDTO);
        ProducerRecord<String, String> replyRecord = new ProducerRecord<>("holidayplanner-controller", message);
        replyRecord.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId));
        dataService.updateAudit(messageDTO.getTraceId(),
                                messageDTO.getStatus(),
                                messageDTO.getStatusResp());
        logger.info("Sending reply to Dispatcher: {}", message);
        kafkaTemplate.send(replyRecord);
    }
}
