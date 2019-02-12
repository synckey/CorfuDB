package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

import lombok.NonNull;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;

/**
 * Poll Report generated by the detectors that poll to detect failed or healed nodes.
 * This is consumed and analyzed by the Management Server to detect changes in the cluster and
 * take appropriate action.
 * Created by zlokhandwala on 3/21/17.
 */
@Data
@Builder
public class PollReport {

    @Default
    private long pollEpoch = Layout.INVALID_EPOCH;
    /**
     * Contains list of servers successfully connected with current node.
     * It doesn't contain nodes answered with WrongEpochException.
     */
    @Default
    private final ImmutableSet<String> connectedNodes = ImmutableSet.of();
    /**
     * Contains list of servers disconnected from this node. If the node A can't ping node B then node B will be added
     * to failedNodes list.
     */
    @Default
    private final ImmutableSet<String> failedNodes = ImmutableSet.of();
    /**
     * A map of nodes answered with {@link WrongEpochException}.
     */
    @Default
    private final ImmutableMap<String, Long> wrongEpochs = ImmutableMap.of();
    /**
     * Indicates if current layout slot is unfilled
     */
    @Default
    private final boolean currentLayoutSlotUnFilled = false;
    /**
     * Current cluster state, collected by a failure detector
     */
    @NonNull
    private final ClusterState clusterState;

    /**
     * Returns all connected nodes to the current node
     * @return
     */
    public ImmutableSet<String> getAllConnectedNodes() {
        return Sets.union(connectedNodes, wrongEpochs.keySet()).immutableCopy();
    }
}