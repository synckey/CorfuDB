package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;
import org.corfudb.util.CFUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.*;
import static org.corfudb.runtime.view.Address.*;

@Slf4j
@AllArgsConstructor
/**
 * This class is responsible for managing a state transfer on the current node.
 */
public class StateTransferManager {

    public enum SegmentState {
        NOT_TRANSFERRED,
        TRANSFERRED,
        RESTORED,
        FAILED
    }

    @EqualsAndHashCode
    @Getter
    @ToString
    public static class CurrentTransferSegment {
        private final long startAddress;
        private final long endAddress;
        private final CompletableFuture<CurrentTransferSegmentStatus> status;
        private final Optional<CommittedTransferData> committedTransferData;

        public CurrentTransferSegment(long startAddress, long endAddress,
                                      CompletableFuture<CurrentTransferSegmentStatus> status) {
            this.startAddress = startAddress;
            this.endAddress = endAddress;
            this.status = status;
            this.committedTransferData = Optional.empty();
        }

        public CurrentTransferSegment(long startAddress, long endAddress,
                                      CompletableFuture<CurrentTransferSegmentStatus> status,
                                      Optional<CommittedTransferData> committedTransferData) {
            this.startAddress = startAddress;
            this.endAddress = endAddress;
            this.status = status;
            this.committedTransferData = committedTransferData;
        }


    }

    @Getter
    @ToString
    @EqualsAndHashCode
    public static class CurrentTransferSegmentStatus {
        private final SegmentState segmentState;
        private final long totalTransferred;
        private final Optional<StreamProcessFailure> causeOfFailure;

        public CurrentTransferSegmentStatus(SegmentState segmentState,
                                            long totalTransferred) {
            this.segmentState = segmentState;
            this.totalTransferred = totalTransferred;
            this.causeOfFailure = Optional.empty();
        }

        public CurrentTransferSegmentStatus(SegmentState segmentState,
                                            long totalTransferred,
                                            Optional<StreamProcessFailure> causeOfFailure) {
            this.segmentState = segmentState;
            this.totalTransferred = totalTransferred;
            this.causeOfFailure = causeOfFailure;
        }
    }

    @AllArgsConstructor
    @Getter
    public static class CommittedTransferData {
        private final long committedOffset;
        private final List<String> sourceServers;
    }

    @Getter
    @NonNull
    private final StreamLog streamLog;

    List<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(Collectors.toList());
    }

    public ImmutableList<CurrentTransferSegment> handleTransfer(List<CurrentTransferSegment> stateList,
                                                                StateTransferBatchProcessorData batchProcessorData) {

        List<CurrentTransferSegment> finalList = stateList.stream().map(segment ->
                {
                    CompletableFuture<CurrentTransferSegmentStatus> newStatus = segment
                            .getStatus()
                            .thenCompose(status -> {
                                // if not transferred -> transfer
                                if (status.getSegmentState().equals(NOT_TRANSFERRED)) {
                                    List<Long> unknownAddressesInRange =
                                            getUnknownAddressesInRange(segment.getStartAddress(), segment.getEndAddress());
                                    if (unknownAddressesInRange.isEmpty()) {
                                        // no addresses to transfer - all done
                                        return CompletableFuture.completedFuture(
                                                new CurrentTransferSegmentStatus(TRANSFERRED, segment.getEndAddress())
                                        );
                                    } else {
                                        long numAddressesToTransfer = unknownAddressesInRange.size();
                                        Optional<CommittedTransferData> committedTransferData = segment.getCommittedTransferData();
                                        StateTransferConfig config = StateTransferConfig.builder()
                                                .unknownAddresses(unknownAddressesInRange)
                                                .committedTransferData(committedTransferData)
                                                .batchProcessorData(batchProcessorData).build();

                                        return stateTransfer(config).thenApply(result ->
                                                createStatusBasedOnTransferResult(result, numAddressesToTransfer));

                                    }
                                } else {
                                    return CompletableFuture.completedFuture(status);
                                }
                            });
                    return new CurrentTransferSegment(segment.getStartAddress(), segment.getEndAddress(), newStatus);
                }


        ).collect(Collectors.toList());

        return ImmutableList.copyOf(finalList);

    }

    CompletableFuture<Result<Long, StreamProcessFailure>> stateTransfer(
            StateTransferConfig stateTransferConfig) {
        return configureStateTransferPipeline(stateTransferConfig).get();
    }

    CurrentTransferSegmentStatus createStatusBasedOnTransferResult(Result<Long, StreamProcessFailure> result,
                                                                   long totalNeeded) {
        Result<Long, StreamProcessFailure> checkedResult =
                result.flatMap(totalTransferred -> {
                    if (totalTransferred != totalNeeded) {
                        return Result.error(new StreamProcessFailure
                                (new IllegalStateException
                                        ("Needed " + totalNeeded +
                                                " but transferred " + totalTransferred)));
                    } else {
                        return Result.ok(totalTransferred);
                    }
                });

        if (checkedResult.isError()) {
            return new CurrentTransferSegmentStatus(FAILED, 0L, Optional.of(checkedResult.getError()));
        } else {
            return new CurrentTransferSegmentStatus(TRANSFERRED, checkedResult.get(), Optional.empty());
        }
    }

    Supplier<CompletableFuture<Result<Long, StreamProcessFailure>>>
    configureStateTransferPipeline(StateTransferConfig stateTransferConfig) {

        Optional<CommittedTransferData> possibleCommittedTransferData =
                stateTransferConfig.getCommittedTransferData();

        List<Long> unknownAddresses =
                stateTransferConfig.getUnknownAddresses();

        StateTransferBatchProcessorData batchProcessorData =
                stateTransferConfig.getBatchProcessorData();

        int batchSize = stateTransferConfig.getBatchSize();

        PolicyStreamProcessorData policyStreamProcessorData =
                stateTransferConfig.getPolicyStreamProcessorData();

        // There exists a range of addresses that can be transferred with a committed batch processor.
        if (possibleCommittedTransferData.isPresent()) {
            CommittedTransferData committedTransferData = possibleCommittedTransferData.get();
            long committedOffset = committedTransferData.getCommittedOffset();
            List<String> sourceServers = committedTransferData.getSourceServers();

            int indexOfTheLastCommittedAddress =
                    Collections.binarySearch(unknownAddresses, committedOffset);
            int indexOfTheFirstNonCommittedAddress = indexOfTheLastCommittedAddress + 1;
            int indexOfTheLastAddress = unknownAddresses.size() - 1;

            List<Long> committedAddresses =
                    IntStream.range(0, indexOfTheLastCommittedAddress + 1)
                            .boxed().map(unknownAddresses::get).collect(Collectors.toList());

            StaticPolicyData committedStaticPolicyData =
                    new StaticPolicyData(committedAddresses, Optional.of(sourceServers), batchSize);

            PolicyStreamProcessor committedStreamProcessor = createStreamProcessor(batchProcessorData,
                    policyStreamProcessorData, COMMITTED);

            if (indexOfTheFirstNonCommittedAddress <= indexOfTheLastAddress) {
                List<Long> nonCommittedAddresses =
                        IntStream.range(indexOfTheFirstNonCommittedAddress, indexOfTheLastAddress)
                                .boxed().map(unknownAddresses::get).collect(Collectors.toList());

                StaticPolicyData protocolStaticPolicyData =
                        new StaticPolicyData(nonCommittedAddresses, Optional.empty(), batchSize);

                PolicyStreamProcessor protocolStreamProcessor = createStreamProcessor(batchProcessorData,
                        policyStreamProcessorData, PROTOCOL);

                return () -> coalesceResults(ImmutableList.of(protocolStreamProcessor
                                .processStream(protocolStaticPolicyData),
                        committedStreamProcessor.processStream(committedStaticPolicyData)));
            } else {
                return () -> committedStreamProcessor.processStream(committedStaticPolicyData);
            }

        } else {
            PolicyStreamProcessor protocolStreamProcessor = createStreamProcessor(batchProcessorData,
                    policyStreamProcessorData, PROTOCOL);

            StaticPolicyData protocolStaticPolicyData =
                    new StaticPolicyData(unknownAddresses, Optional.empty(), batchSize);


            return () -> protocolStreamProcessor
                    .processStream(protocolStaticPolicyData);
        }
    }

    PolicyStreamProcessor createStreamProcessor(StateTransferBatchProcessorData batchProcessorData,
                                                PolicyStreamProcessorData policyStreamProcessorData,
                                                BatchProcessorType type) {
        if (type.equals(PROTOCOL)) {
            StateTransferBatchProcessor protocolBatchProcessor =
                    BatchProcessorFactory.createBatchProcessor(batchProcessorData, type);
            return PolicyStreamProcessor
                    .builder()
                    .policyData(policyStreamProcessorData)
                    .batchProcessor(protocolBatchProcessor).build();

        } else if (type.equals(COMMITTED)) {

            StateTransferBatchProcessor committedBatchProcessor =
                    BatchProcessorFactory.createBatchProcessor(batchProcessorData, type);
            return PolicyStreamProcessor
                    .builder()
                    .policyData(policyStreamProcessorData)
                    .batchProcessor(committedBatchProcessor).build();

        } else {
            throw new IllegalStateException("Other types are not defined.");
        }
    }

    CompletableFuture<Result<Long, StreamProcessFailure>> coalesceResults
            (List<CompletableFuture<Result<Long, StreamProcessFailure>>> allResults) {
        CompletableFuture<List<Result<Long, StreamProcessFailure>>> futureOfListResults =
                CFUtils.sequence(allResults);

        CompletableFuture<Optional<Result<Long, StreamProcessFailure>>> possibleSingleResult = futureOfListResults
                .thenApply(multipleResults ->
                        multipleResults
                                .stream()
                                .reduce(this::mergeTransferResults));

        return possibleSingleResult.thenApply(result -> result.orElseGet(() ->
                new Result<>(NON_ADDRESS,
                        new StreamProcessFailure(
                                new IllegalStateException("Coalesced transfer batch result is empty.")))));

    }

    Result<Long, StreamProcessFailure> mergeTransferResults(Result<Long, StreamProcessFailure> firstResult,
                                                            Result<Long, StreamProcessFailure> secondResult) {
        return firstResult.flatMap(firstTotal ->
                secondResult.map(secondTotal ->
                        firstTotal + secondTotal));
    }
}