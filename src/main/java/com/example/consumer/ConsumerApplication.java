package com.example.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.integration.http.dsl.Http;
import org.springframework.integration.http.dsl.HttpMessageHandlerSpec;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@SpringBootApplication
public class ConsumerApplication {


    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }


    @Bean
    IntegrationFlow incomingHttpMessagesFlow() {
        var frequencyOfPoll = Duration.ofSeconds(1);
        var parameterizedTypeReference = new ParameterizedTypeReference<List<Event>>() {};
        var httpGateway = Http
                .outboundGateway("http://localhost:9091/messages")
                .httpMethod(HttpMethod.GET)
                .expectedResponseType(parameterizedTypeReference);

        var httpOutbound = Http
                .outboundChannelAdapter((Function<Message<Event>, URI>) eventMessage -> URI.create("http://localhost:9091/ack/" + eventMessage.getPayload().id()))
                .httpMethod(HttpMethod.GET);
        
        return IntegrationFlow
                .from(() -> MessageBuilder.withPayload((Object) System.currentTimeMillis()).build(),
                        poller -> poller.poller(pm -> PollerFactory.fixedDelay(frequencyOfPoll)))
                .handle(httpGateway)
                .split()
                .handle((GenericHandler<Event>) (payload, headers) -> {
                    // do something with each delivered message
                    System.out.println("got a new message [" + payload + "]");
                    return payload;
                })
                .handle(httpOutbound)
                .get();
    }


    record Event(String id, String description) {
    }
}


//  simple HTTP controller that simulates a constant drip of new messages needing to be drained by a consumer.
// just a stub for the APi to which you're going to actually connect.
@Controller
@ResponseBody
class HttpEndpoints {

    private final Map<String, ServerEvent> serverEvents = new ConcurrentHashMap<>();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Runnable runnable = () -> {

        var now = Instant.now();
        var events = Math.random() > .5 ?
                List.of(new ServerEvent(UUID.randomUUID().toString(), "First message at " + now),
                        new ServerEvent(UUID.randomUUID().toString(), "Second message at " + now)) :
                List.of(new ServerEvent(UUID.randomUUID().toString(), "First message at " + now),
                        new ServerEvent(UUID.randomUUID().toString(), "Second message at " + now),
                        new ServerEvent(UUID.randomUUID().toString(), "Third message at " + now));

        for (var e : events)
            serverEvents.put(e.id(), e);

        log.info("just added [{}] to the results", events);
    };

    HttpEndpoints() {
        var executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this.runnable, 0, 10, TimeUnit.SECONDS);
    }

    @GetMapping("/ack/{id}")
    AckResponse ack(@PathVariable String id) {
        Assert.notNull(id, "the id must be non-null");
        var removed = this.serverEvents.remove(id);
        this.log.info("removing message {} from the unhandled messages.", id);
        return new AckResponse(id, null == removed);
    }

    @GetMapping("/messages")
    Collection<ServerEvent> getMessages() {
        var currentMessages = new ArrayList<>(this.serverEvents.values()); // delivers (and redelivers) those that haven't been acknowledged
        this.log.info("there are {} outstanding messages",
                currentMessages.size());
        return currentMessages;
    }

    record ServerEvent(String id, String description) {
    }


    record AckResponse(String id, boolean acknowledged) {
    }
}

 