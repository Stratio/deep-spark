/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.cassandra.cql;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.querybuilder.CassandraUpdateQueryBuilder;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.commons.handler.DeepRecordWriter;
import com.stratio.deep.commons.utils.Utils;

/**
 * Handles the distributed write to cassandra in batch.
 */
public final class DeepCqlRecordWriter extends DeepRecordWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DeepCqlRecordWriter.class);
    private final static int MAX_PARALLEL_QUERIES = 4;
    private final static int WORK_QUEUE_SIZE = 8;

    private final ListeningExecutorService taskExecutorService;
    private final ConcurrentHashMap<String, ListenableFuture<?>> pendingTasks;
    private WriteTask currentTask;
    private boolean hasCurrentTask = false;

    private final ICassandraDeepJobConfig writeConfig;
    private final CassandraUpdateQueryBuilder queryBuilder;

    private Session sessionWithHost;

    /**
     * Cassandra record writer constructor.
     *
     * @param writeConfig write configuration
     * @param queryBuilder query builder
     */
    public DeepCqlRecordWriter(ICassandraDeepJobConfig writeConfig, CassandraUpdateQueryBuilder queryBuilder) {
        this.taskExecutorService = MoreExecutors.listeningDecorator(Utils.newBlockingFixedThreadPoolExecutor
                (MAX_PARALLEL_QUERIES, WORK_QUEUE_SIZE));
        this.pendingTasks = new ConcurrentHashMap<>();
        this.writeConfig = writeConfig;
        this.queryBuilder = queryBuilder;
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            sessionWithHost = CassandraClientProvider.trySessionForLocation(localhost.getHostAddress(),
                    (CassandraDeepJobConfig) writeConfig, false).left;
            sessionWithHost.init();
        } catch (UnknownHostException e) {
            throw new DeepInstantiationException("Cannot resolve local hostname", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing all writer tasks");

        if (hasCurrentTask) {
            executeTaskAsync();
        }

        waitForCompletion();
        taskExecutorService.shutdown();
    }

    /**
     * Adds the provided row to a batch. If the batch size reaches the threshold configured in
     * IDeepJobConfig.getBatchSize the batch will be sent to the data store.
     *
     * @param keys   the Cells object containing the row keys.
     * @param values the Cells object containing all the other row columns.
     */
    public void write(Cells keys, Cells values) {

        if (!hasCurrentTask) {
            String localCql = queryBuilder.prepareQuery(keys, values);
            currentTask = new WriteTask(localCql);
            hasCurrentTask = true;
        }

        // add primary key columns to the bind variables
        List<Object> allValues = new ArrayList<>(values.getCellValues());
        allValues.addAll(keys.getCellValues());
        currentTask.add(allValues);

        if (isBatchSizeReached()) {
            executeTaskAsync();
        }
    }

    /**
     * Validates if batch size threshold has been reached.
     */
    private boolean isBatchSizeReached() {
        return currentTask.size() >= writeConfig.getBatchSize();
    }

    /**
     * Submits the task for future execution. Task is added to pending tasks
     * and removed when the execution is done.
     */
    private void executeTaskAsync() {
        final String taskId = currentTask.getId();

        ListenableFuture<?> future = taskExecutorService.submit(currentTask);
        pendingTasks.put(taskId, future);

        future.addListener(new Runnable() {
            @Override
            public void run() {
                pendingTasks.remove(taskId);
            }
        }, MoreExecutors.sameThreadExecutor());

        hasCurrentTask = false;
    }

    /**
     * Waits until all pending tasks completed.
     */
    private void waitForCompletion() {
        for (ListenableFuture<?> future : pendingTasks.values()) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("[" + this + "] Error waiting for writes to complete: " + e.getMessage());
            }
        }
    }

    /**
     * A task that executes async writes to cassandra.
     */
    class WriteTask implements Runnable {
        private final UUID id = UUID.randomUUID();

        private final String cql;
        private final List<List<Object>> records = new ArrayList<>();

        public WriteTask(String cql) {
            this.cql = cql;
        }

        public void add(List<Object> values) {
            this.records.add(values);
        }

        public int size() {
            return this.records.size();
        }

        public String getId() {
            return id.toString();
        }

        /**
         * Executes cql batch statements in Cassandra
         */
        @Override
        public void run() {
            LOG.debug("[" + this + "] Executing batch write to cassandra");
            try {
                final PreparedStatement preparedStatement = sessionWithHost.prepare(cql);
                final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                for (final List<Object> record : records) {
                    batchStatement.add(preparedStatement.bind(record.toArray(new Object[record.size()])));
                }
                sessionWithHost.execute(batchStatement);
            } catch (Exception e) {
                LOG.error("[" + this + "] Exception occurred while trying to execute batch in cassandra: " +
                        e.getMessage());
            }
        }
    }
}
