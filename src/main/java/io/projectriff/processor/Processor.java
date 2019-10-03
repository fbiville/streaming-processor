package io.projectriff.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.rpc.InputFrame;
import io.projectriff.invoker.rpc.InputSignal;
import io.projectriff.invoker.rpc.OutputFrame;
import io.projectriff.invoker.rpc.OutputSignal;
import io.projectriff.invoker.rpc.ReactorRiffGrpc;
import io.projectriff.invoker.rpc.StartFrame;
import io.projectriff.processor.serialization.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * Main driver class for the streaming processor.
 *
 * <p>Continually pumps data from one or several input streams (see {@code riff-serialization.proto} for this so-called "at rest" format),
 * arranges messages in invocation windows and invokes the riff function over RPC by multiplexing messages from several
 * streams into one RPC channel (see {@code riff-rpc.proto} for the wire format).
 * On the way back, performs the opposite operations: de-muxes results and serializes them back to the corresponding
 * output streams.</p>
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
public class Processor {

    public static class LiiklusTopics {

        private List<FullyQualifiedTopic> inputs;
        private List<FullyQualifiedTopic> outputs;
        private Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> stubPerAddress;

        public LiiklusTopics(String inputs, String outputs) {
            this.inputs = FullyQualifiedTopic.parseMultiple(inputs);
            this.outputs = FullyQualifiedTopic.parseMultiple(outputs);
            Set<FullyQualifiedTopic> topics = new HashSet<>(this.inputs);
            topics.addAll(this.outputs);
            this.stubPerAddress = indexByAddress(topics);
        }

        LiiklusTopics() {
        }

        public int getOutputCount() {
            return this.outputs.size();
        }

        void setInputs(List<FullyQualifiedTopic> inputs) {
            this.inputs = inputs;
        }

        void setOutputs(List<FullyQualifiedTopic> outputs) {
            this.outputs = outputs;
        }

        void setStubPerAddress(Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> stubPerAddress) {
            this.stubPerAddress = stubPerAddress;
        }

        private static Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> indexByAddress(Collection<FullyQualifiedTopic> fullyQualifiedTopics) {
            return fullyQualifiedTopics.stream()
                    .map(FullyQualifiedTopic::getGatewayAddress)
                    .distinct()
                    .collect(Collectors.toMap(
                            address -> address,
                            address -> ReactorLiiklusServiceGrpc.newReactorStub(
                                    NettyChannelBuilder.forTarget(address)
                                            .usePlaintext()
                                            .build())
                            )
                    );
        }

    }

    /**
     * ENV VAR key holding the coordinates of the input streams, as a comma separated list of {@code gatewayAddress:port/streamName}.
     *
     * @see FullyQualifiedTopic
     */
    public static final String INPUTS = "INPUTS";

    /**
     * ENV VAR key holding the coordinates of the output streams, as a comma separated list of {@code gatewayAddress:port/streamName}.
     *
     * @see FullyQualifiedTopic
     */
    public static final String OUTPUTS = "OUTPUTS";

    /**
     * ENV VAR key holding the address of the function RPC, as a {@code host:port} string.
     */
    public static final String FUNCTION = "FUNCTION";

    /**
     * ENV VAR key holding the serialized list of content-types expected on the output streams.
     *
     * @see StreamOutputContentTypes
     */
    public static final String OUTPUT_CONTENT_TYPES = "OUTPUT_CONTENT_TYPES";

    /**
     * ENV VAR key holding the consumer group string this process should use.
     */
    public static final String GROUP = "GROUP";

    /**
     * The number of retries when testing http connection to the function.
     */
    private static final int NUM_RETRIES = 20;

    private final LiiklusTopics topics;

    private final List<String> outputContentTypes;

    /**
     * The consumer group string this process will use to identify itself when reading from the input streams.
     */
    private final String group;

    /**
     * The RPC stub used to communicate with the function process.
     *
     * @see "riff-rpc.proto for the wire format and service definition"
     */
    private final ReactorRiffGrpc.ReactorRiffStub riffStub;

    public static void main(String[] args) throws Exception {

        checkEnvironmentVariables();

        Hooks.onOperatorDebug();

        String functionAddress = System.getenv(FUNCTION);

        assertHttpConnectivity(functionAddress);

        Channel fnChannel = NettyChannelBuilder.forTarget(functionAddress)
                .usePlaintext()
                .build();

        LiiklusTopics liiklusTopics = new LiiklusTopics(System.getenv(INPUTS), System.getenv(OUTPUTS));
        Processor processor = new Processor(
                liiklusTopics,
                parseContentTypes(System.getenv(OUTPUT_CONTENT_TYPES), liiklusTopics.getOutputCount()),
                System.getenv(GROUP),
                ReactorRiffGrpc.newReactorStub(fnChannel));

        processor.run();

    }

    private static void checkEnvironmentVariables() {
        List<String> envVars = Arrays.asList(INPUTS, OUTPUTS, OUTPUT_CONTENT_TYPES, FUNCTION, GROUP);
        if (envVars.stream()
                .anyMatch(v -> (System.getenv(v) == null || System.getenv(v).trim().length() == 0))) {
            System.err.format("Missing one of the following environment variables: %s%n", envVars);
            envVars.forEach(v -> System.err.format("  %s = %s%n", v, System.getenv(v)));
            System.exit(1);
        }
    }

    private static void assertHttpConnectivity(String functionAddress) throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("http://" + functionAddress);
        for (int i = 1; i <= NUM_RETRIES; i++) {
            try (Socket s = new Socket(uri.getHost(), uri.getPort())) {
            } catch (ConnectException t) {
                if (i == NUM_RETRIES) {
                    throw t;
                }
                Thread.sleep(i * 100);
            }
        }
    }

    public Processor(LiiklusTopics topics,
                     List<String> outputContentTypes,
                     String group,
                     ReactorRiffGrpc.ReactorRiffStub riffStub) {

        this.topics = topics;
        this.outputContentTypes = outputContentTypes;
        this.riffStub = riffStub;
        this.group = group;
    }


    public void run() {
        Flux.fromIterable(this.topics.inputs)
                .flatMap(fullyQualifiedTopic -> {
                    ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub inputLiiklus = this.topics.stubPerAddress.get(fullyQualifiedTopic.getGatewayAddress());
                    return inputLiiklus.subscribe(subscribeRequestForInput(fullyQualifiedTopic.getTopic()))
                            .filter(SubscribeReply::hasAssignment)
                            .map(SubscribeReply::getAssignment)
                            .flatMap(
                                    assignment -> inputLiiklus
                                            .receive(receiveRequestForAssignment(assignment))
                                            .delayUntil(receiveReply -> ack(fullyQualifiedTopic, inputLiiklus, receiveReply, assignment))
                            )
                            .map(receiveReply -> toRiffSignal(receiveReply, fullyQualifiedTopic));
                })
                .compose(this::riffWindowing)
                .map(this::invoke)
                .concatMap(identity())
                .concatMap(m -> {
                    OutputFrame next = m.getData();
                    FullyQualifiedTopic output = this.topics.outputs.get(next.getResultIndex());
                    ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub outputLiiklus = this.topics.stubPerAddress.get(output.getGatewayAddress());
                    return outputLiiklus.publish(createPublishRequest(next, output.getTopic()));
                }).blockLast();
    }

    private Mono<Empty> ack(FullyQualifiedTopic topic, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub stub, ReceiveReply receiveReply, Assignment assignment) {
        System.out.format("ACKing %s for group %s: offset=%d, part=%d%n", topic.getTopic(), this.group, receiveReply.getRecord().getOffset(), assignment.getPartition());
        return stub.ack(AckRequest.newBuilder()
                .setGroup(this.group)
                .setOffset(receiveReply.getRecord().getOffset())
                .setPartition(assignment.getPartition())
                .setTopic(topic.getTopic())
                .build());
    }

    private Flux<OutputSignal> invoke(Flux<InputFrame> in) {
        InputSignal start = InputSignal.newBuilder()
                .setStart(StartFrame.newBuilder()
                        .addAllExpectedContentTypes(this.outputContentTypes)
                        .build())
                .build();

        return riffStub.invoke(Flux.concat(
                Flux.just(start), //
                in.map(frame -> InputSignal.newBuilder().setData(frame).build())));
    }

    /**
     * This converts an RPC representation of an {@link OutputFrame} to an at-rest {@link Message}, and creates a publish request for it.
     */
    private PublishRequest createPublishRequest(OutputFrame next, String topic) {
        Message msg = Message.newBuilder()
                .setPayload(next.getPayload())
                .setContentType(next.getContentType())
                .putAllHeaders(next.getHeadersMap())
                .build();

        return PublishRequest.newBuilder()
                .setValue(msg.toByteString())
                .setTopic(topic)
                .build();
    }

    private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
        return ReceiveRequest.newBuilder().setAssignment(assignment).build();
    }

    private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
        return linear.window(Duration.ofSeconds(60));
    }

    /**
     * This converts a liiklus received message (representing an at-rest riff {@link Message}) into an RPC {@link InputFrame}.
     */
    private InputFrame toRiffSignal(ReceiveReply receiveReply, FullyQualifiedTopic fullyQualifiedTopic) {
        int inputIndex = this.topics.inputs.indexOf(fullyQualifiedTopic);
        if (inputIndex == -1) {
            throw new RuntimeException("Unknown topic: " + fullyQualifiedTopic);
        }
        ByteString bytes = receiveReply.getRecord().getValue();
        try {
            Message message = Message.parseFrom(bytes);
            return InputFrame.newBuilder()
                    .setPayload(message.getPayload())
                    .setContentType(message.getContentType())
                    .setArgIndex(inputIndex)
                    .build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

    }

    private SubscribeRequest subscribeRequestForInput(String topic) {
        return SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(group)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
                .build();
    }

    private static List<String> parseContentTypes(String json, int outputCount) {
        try {
            List<String> contentTypes = new ObjectMapper().readValue(json, StreamOutputContentTypes.class).getContentTypes();
            int actualSize = contentTypes.size();
            if (actualSize != outputCount) {
                throw new RuntimeException(
                        String.format("Expected %d output stream content type(s), got %d.%n\tSee %s", outputCount, actualSize, json)
                );
            }
            return contentTypes;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}