/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;

public class Transaction {
    private final long transactionId;

    private final List<Pair<Lsn, ReplicationMessage>> events;
    private final ReplicationMessage startEvent;
    private final Lsn startLsn;

    public Transaction(ReplicationMessage startEvent, Lsn startLsn) {
        this.startEvent = startEvent;
        this.transactionId = startEvent.getTransactionId().getAsLong();
        this.events = new ArrayList<>();
        this.startLsn = startLsn;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void addEvent(Lsn lsn, ReplicationMessage event) {
        events.add(new Pair<>(lsn, event));
    }

    public List<Pair<Lsn, ReplicationMessage>> getEvents() {
        return events;
    }

    public ReplicationMessage getStartEvent() {
        return startEvent;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                '}';
    }

    public static class Pair<K, V> {
        private final K key;
        private final V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
