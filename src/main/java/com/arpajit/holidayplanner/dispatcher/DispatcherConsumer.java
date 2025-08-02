package com.arpajit.holidayplanner.dispatcher;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.arpajit.holidayplanner.dto.KafkaMessage;

@Component
public class DispatcherConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DispatcherConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DispatcherDataServComm dataService;

    @Autowired
    private DispatcherProducer dispatcherProducer;

    @KafkaListener(topics = "holidayplanner-dispatcher", groupId = "holidayplanner-controller")
    public void consumedPayload(String message,
                                @Header(KafkaHeaders.CORRELATION_ID) byte[] correlationId,
                                Acknowledgment act)
                                throws Exception {
        logger.info("Received Kafka message:{}", message);
        act.acknowledge();

        // Parsing Message
        KafkaMessage messageDTO = objectMapper.readValue(message, KafkaMessage.class);
        messageDTO.setStatus("DISPATCHER_RECEIVED");
        dataService.updateAudit(messageDTO.getTraceId(),
                                messageDTO.getStatus(),
                                messageDTO.getStatusResp());

        // Routing based on request type
        logger.info("Requested for {}", messageDTO.getRequestType());
        switch (messageDTO.getRequestType()) {
            case "GET_ALL_HOLIDAY_DETAILS":
                logger.info("Triggering GET_ALL_HOLIDAY_DETAILS service");
                messageDTO.setStatus("CONTROLLER_SENT");
                dispatcherProducer.dropToController(correlationId, messageDTO);
                break;
            default:
                logger.warn("{} does not match any request type", messageDTO.getRequestType());
                messageDTO.setStatus("FAILED_AT_DISPATCHER");
                messageDTO.setStatusResp("Request type: "+messageDTO.getRequestType()+" not matched in Dispatcher");
                dataService.updateAudit(messageDTO.getTraceId(),
                                        messageDTO.getStatus(),
                                        messageDTO.getStatusResp());
                break;
        }
    }
}
