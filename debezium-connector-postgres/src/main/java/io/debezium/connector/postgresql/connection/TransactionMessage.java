/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

/**
 * Replication message instance representing transaction demarcation events.
 *
 * @author Jiri Pechanec
 *
 */
public class TransactionMessage implements ReplicationMessage {

    private final long transactionId;
    private final Instant commitTime;
    private final Operation operation;
    private final String originName;

    public TransactionMessage(Operation operation, long transactionId, Instant commitTime) {
        this.operation = operation;
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.originName = null;
    }

    public TransactionMessage(Operation operation, long transactionId, Instant commitTime, String originName) {
        this.operation = operation;
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.originName = originName;
    }

    @Override
    public boolean isLastEventForLsn() {
        return operation == Operation.COMMIT;
    }

    @Override
    public boolean hasTypeMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong getTransactionId() {
        return OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    public String getOriginName() {
        return originName;
    }

    @Override
    public String toString() {
        return "TransactionMessage{" +
                "transactionId=" + transactionId +
                ", commitTime=" + commitTime +
                ", operation=" + operation +
                ", originName='" + originName + '\'' +
                '}';
    }
}
