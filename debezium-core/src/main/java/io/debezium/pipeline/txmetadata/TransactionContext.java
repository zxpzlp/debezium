/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.schema.DataCollectionId;

/**
 * The context holds internal state necessary for book-keeping of events in active transaction.
 * The main data tracked are
 * <ul>
 * <li>active transaction id</li>
 * <li>the total event number seen from the transaction</li>
 * <li>the number of events per table/collection seen in the transaction</li>
 * </ul>
 *
 * The state of this context is stored in offsets and is recovered upon restart.
 *
 * @author Jiri Pechanec
 */
@NotThreadSafe
public class TransactionContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionContext.class);

    private static final String OFFSET_TRANSACTION_ID = TransactionMonitor.DEBEZIUM_TRANSACTION_KEY + "_" + TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY;
    private static final String OFFSET_TABLE_COUNT_PREFIX = TransactionMonitor.DEBEZIUM_TRANSACTION_KEY + "_"
            + TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY + "_";
    private static final int OFFSET_TABLE_COUNT_PREFIX_LENGTH = OFFSET_TABLE_COUNT_PREFIX.length();
    public static final String DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY = "";
    public static final String CISCO_OFFSET_VERSION = "CISCO_OFFSET_VERSION";

    private String transactionId = null;
    private final Map<String, PerObjectTransactionContext> perObjectTransactionContexts = new HashMap<>();
    private PerObjectTransactionContext defaultPerObjectTransactionContext = new PerObjectTransactionContext();
    private Map<String, Object> localOffset = new HashMap<>();

    private void reset() {
        transactionId = null;
        defaultPerObjectTransactionContext.reset();
        perObjectTransactionContexts.clear();
        perObjectTransactionContexts.put(DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY, defaultPerObjectTransactionContext);
        localOffset.clear();
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        offset.putAll(localOffset);
        return offset;
    }

    @SuppressWarnings("unchecked")
    public static TransactionContext load(Map<String, ?> offsets) {
        final Map<String, Object> o = (Map<String, Object>) offsets;
        final TransactionContext context = new TransactionContext();
        context.transactionId = (String) o.get(OFFSET_TRANSACTION_ID);

        if (offsets.get(CISCO_OFFSET_VERSION) == null) {
            context.defaultPerObjectTransactionContext = PerObjectTransactionContext.load(offsets);
            context.getPerObjectTransactionContexts().put(DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY, context.defaultPerObjectTransactionContext);
            LOGGER.debug("Loaded original offset {}", context);
        }
        else {
            LOGGER.debug("Loading transaction context from {}", offsets);
            for (final Entry<String, Object> offset : o.entrySet()) {
                if (offset.getKey().startsWith(OFFSET_TABLE_COUNT_PREFIX)) {
                    final String value = offset.getKey().substring(OFFSET_TABLE_COUNT_PREFIX_LENGTH);
                    String[] splitted = value.split(PerObjectTransactionContext.IDENTITY_ID_SEPARATOR);
                    final String dataCollectionId = splitted[1];
                    final String identityId = splitted[0];
                    final Long count = (Long) offset.getValue();

                    PerObjectTransactionContext theContext = context.getPerObjectTransactionContexts().computeIfAbsent(identityId, k -> {
                        PerObjectTransactionContext perObjectTransactionContext = new PerObjectTransactionContext();
                        perObjectTransactionContext.beginTransaction(context.transactionId);
                        return perObjectTransactionContext;
                    });

                    theContext.perTableEventCount.put(dataCollectionId, count);
                    theContext.totalEventCount = theContext.perTableEventCount.values().stream().mapToLong(x -> x).sum();
                }
            }
            if (context.getPerObjectTransactionContexts().get(DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY) == null) {
                context.defaultPerObjectTransactionContext.transactionId = context.transactionId;
                context.getPerObjectTransactionContexts().put(DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY, context.defaultPerObjectTransactionContext);
            }
            else {
                context.defaultPerObjectTransactionContext = context.getPerObjectTransactionContexts().get(DEFAULT_PEROBJECTTRANSACTIONCONTEXT_KEY);
            }
            LOGGER.debug("Loaded new offset {}", context);
        }

        return context;
    }

    public boolean isTransactionInProgress() {
        return transactionId != null;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public long getTotalEventCount() {
        long totalEventCount = 0;
        for (PerObjectTransactionContext perObjectTransactionContext : perObjectTransactionContexts.values()) {
            totalEventCount += perObjectTransactionContext.getTotalEventCount();
        }
        return totalEventCount;
    }

    public void beginTransaction(String txId) {
        reset();
        transactionId = txId;
        defaultPerObjectTransactionContext.beginTransaction(txId);
        localOffset.put(OFFSET_TRANSACTION_ID, transactionId);
        localOffset.put(CISCO_OFFSET_VERSION, 1);
    }

    public void endTransaction() {
        reset();
    }

    public long event(DataCollectionId source, String objectId) {
        PerObjectTransactionContext perObjectTransactionContext = perObjectTransactionContexts.computeIfAbsent(objectId, k -> {
            PerObjectTransactionContext transactionContext = new PerObjectTransactionContext();
            transactionContext.beginTransaction(transactionId);
            return transactionContext;
        });
        long count = perObjectTransactionContext.event(source, objectId);
        localOffset.putAll(perObjectTransactionContext.getLocalOffset());
        return count;
    }

    public Map<String, Long> getPerTableEventCount() {
        return defaultPerObjectTransactionContext.getPerTableEventCount();
    }

    public Map<String, PerObjectTransactionContext> getPerObjectTransactionContexts() {
        return perObjectTransactionContexts;
    }

    @Override
    public String toString() {
        return "TransactionContext [currentTransactionId=" + transactionId + ", perTableEventCount="
                + perObjectTransactionContexts + "]";
    }
}
