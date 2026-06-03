/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftUtil;

import java.util.Optional;

// TODO: write test for this type
public final class ChangeVoterHandlerState {
    private Optional<AddVoterHandlerState> addVoterHandlerState = Optional.empty();
    private Optional<RemoveVoterHandlerState> removeVoterHandlerState = Optional.empty();
    private Optional<UpdateVoterHandlerState> updateVoterHandlerState = Optional.empty();

    private final KafkaRaftMetrics kafkaRaftMetrics;

    public ChangeVoterHandlerState(KafkaRaftMetrics kafkaRaftMetrics) {
        this.kafkaRaftMetrics = kafkaRaftMetrics;
    }

    public Optional<AddVoterHandlerState> addVoterHandlerState() {
        return addVoterHandlerState;
    }

    public void resetAddVoterHandlerState(
        Errors error,
        String message,
        Optional<AddVoterHandlerState> state
    ) {
        // TODO: move the completion to the hander state
        addVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.addVoterResponse(error, message))
        );
        addVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    public Optional<RemoveVoterHandlerState> removeVoterHandlerState() {
        return removeVoterHandlerState;
    }

    public void resetRemoveVoterHandlerState(
        Errors error,
        String message,
        Optional<RemoveVoterHandlerState> state
    ) {
        // TODO: move the completion to the hander state
        removeVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.removeVoterResponse(error, message))
        );
        removeVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    public Optional<UpdateVoterHandlerState> updateVoterHandlerState() {
        return updateVoterHandlerState;
    }

    public void resetUpdateVoterHandlerState(
        Errors error,
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        Optional<UpdateVoterHandlerState> state
    ) {
        // TODO: move the completion to the hander state
        updateVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(
                    RaftUtil.updateVoterResponse(
                        error,
                        handlerState.requestListenerName(),
                        leaderAndEpoch,
                        leaderEndpoints
                    )
                )
        );
        updateVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    private void updateUncommittedVoterChangeMetric() {
        kafkaRaftMetrics.updateUncommittedVoterChange(
            addVoterHandlerState.isPresent() ||
            removeVoterHandlerState.isPresent() ||
            updateVoterHandlerState.isPresent()
        );
    }

    // TODO: add update voter handler to this
    public long maybeExpirePendingOperation(
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        long currentTimeMs
    ) {
        // First abort any expired operations
        long timeUntilAddVoterExpiration = addVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilAddVoterExpiration == 0) {
            resetAddVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        long timeUntilRemoveVoterExpiration = removeVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilRemoveVoterExpiration == 0) {
            resetRemoveVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        long timeUntilUpdateVoterExpiration = updateVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilUpdateVoterExpiration == 0) {
            resetUpdateVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                leaderAndEpoch,
                leaderEndpoints,
                Optional.empty()
            );
        }

        // Reread the timeouts and return the smaller of them
        return Math.min(
            addVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE),
            Math.min(
                removeVoterHandlerState()
                    .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                    .orElse(Long.MAX_VALUE),
                updateVoterHandlerState()
                    .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                    .orElse(Long.MAX_VALUE)
            )
        );
    }

    public void maybeResetPendingVoterHandlerState(Errors error) {
        resetAddVoterHandlerState(error, null, Optional.empty());
        resetRemoveVoterHandlerState(error, null, Optional.empty());
        // TODO: request the update voter state
    }

    public boolean isOperationPending(
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        long currentTimeMs
    ) {
        maybeExpirePendingOperation(leaderAndEpoch, leaderEndpoints, currentTimeMs);
        return addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent() || updateVoterHandlerState.isPresent();
    }

    @Override
    public String toString() {
        return String.format(
            "ChangeVoterHandlerState(addVoterHandlerState=%s, removeVoterHandlerState=%s, updateVoterHandlerState=%s)",
            addVoterHandlerState,
            removeVoterHandlerState,
            updateVoterHandlerState
        );
    }
}
