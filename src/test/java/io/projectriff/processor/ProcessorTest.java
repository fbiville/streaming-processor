package io.projectriff.processor;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.testing.GrpcServerRule;
import io.projectriff.invoker.rpc.OutputFrame;
import io.projectriff.invoker.rpc.OutputSignal;
import io.projectriff.invoker.rpc.ReactorRiffGrpc;
import io.projectriff.processor.serialization.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessorTest {

    @Rule
    public final GrpcServerRule liiklusRule = new GrpcServerRule().directExecutor();
    @Rule
    public final GrpcServerRule riffRule = new GrpcServerRule().directExecutor();

    private ReactorRiffGrpc.RiffImplBase riffStub;

    private ReactorLiiklusServiceGrpc.LiiklusServiceImplBase liiklusStub;

    private Processor processor;

    @Before
    public void prepare() {
        OutputFrame outputFrame = OutputFrame.newBuilder()
                .setPayload(ByteString.copyFromUtf8("output"))
                .setContentType("text/plain")
                .setResultIndex(0)
                .build();

        riffStub = mock(ReactorRiffGrpc.RiffImplBase.class);
        when(riffStub.invoke(ArgumentMatchers.any()))
                .thenAnswer((Answer<Flux<OutputSignal>>) invocation -> {
                    for (Object argument : invocation.getArguments()) {
                        ((Flux<?>) argument).subscribe(System.out::println);
                    }
                    return Flux.just(
                            OutputSignal.newBuilder()
                                    .setData(
                                            outputFrame
                                    )
                                    .build()
                    );
                });
        riffRule.getServiceRegistry().addService(riffStub);


        liiklusStub = mock(ReactorLiiklusServiceGrpc.LiiklusServiceImplBase.class);
        when(liiklusStub.subscribe(ArgumentMatchers.any()))
                .thenAnswer((Answer<Flux<SubscribeReply>>) invocation -> Flux.just(
                        SubscribeReply.newBuilder()
                                .setAssignment(Assignment.newBuilder()
                                        .setPartition(42)
                                        .build())
                                .build()));

        when(liiklusStub.receive(ArgumentMatchers.any()))
                .thenAnswer((Answer<Flux<ReceiveReply>>) invocation -> Flux.interval(Duration.ofMillis(5000)).map(i ->
                        ReceiveReply.newBuilder()
                                .setRecord(
                                        ReceiveReply.Record.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("some-key"))
                                                .setOffset(0)
                                                .setValue(Message.newBuilder()
                                                        .setPayload(ByteString.copyFromUtf8("super payload" + i))
                                                        .setContentType("text/plain")
                                                        .build().toByteString())
                                                .build()
                                )
                                .build()));

        when(liiklusStub.ack(ArgumentMatchers.any()))
                .thenAnswer((Answer<Mono<Empty>>) invocation -> Mono.just(Empty.getDefaultInstance()));

        when(liiklusStub.publish(ArgumentMatchers.any()))
                .thenAnswer((Answer<Mono<PublishReply>>) invocation -> {
                    Object[] arguments = invocation.getArguments();
                    PublishRequest expectedRequest = PublishRequest.newBuilder()
                            .setTopic("topic2")
                            .setValue(Message.newBuilder()
                                    .setPayload(outputFrame.getPayload())
                                    .setContentType(outputFrame.getContentType())
                                    .putAllHeaders(outputFrame.getHeadersMap())
                                    .build().toByteString())
                            .build();

                    assertThat(arguments).hasSize(1);
                    Object actualRequest = ((Mono<?>) arguments[0]).block();
                    assertThat(actualRequest).isEqualTo(expectedRequest);
                    return Mono.just(
                            PublishReply.newBuilder()
                                    .setPartition(42)
                                    .setOffset(99)
                                    .build()
                    );
                });

        liiklusRule.getServiceRegistry().addService(liiklusStub);

        Processor.LiiklusTopics liiklusTopics = new Processor.LiiklusTopics();
        liiklusTopics.setInputs(FullyQualifiedTopic.parseMultiple("example.com/topic"));
        liiklusTopics.setOutputs(FullyQualifiedTopic.parseMultiple("example.com/topic2"));
        liiklusTopics.setStubPerAddress(Collections.singletonMap("example.com", ReactorLiiklusServiceGrpc.newReactorStub(liiklusRule.getChannel())));

        processor = new Processor(
                liiklusTopics,
                Collections.singletonList("text/plain"),
                "some-group",
                ReactorRiffGrpc.newReactorStub(riffRule.getChannel())
        );
    }

    @Test
    public void run() {
        processor.run();
    }

    @After
    public void clean_up() {
        liiklusRule.getServer().shutdownNow();
        riffRule.getServer().shutdownNow();
    }
}