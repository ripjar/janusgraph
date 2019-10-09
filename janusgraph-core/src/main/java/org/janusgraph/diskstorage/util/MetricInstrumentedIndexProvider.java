// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.util.stats.MetricManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

public class MetricInstrumentedIndexProvider implements IndexProvider {
    private final MetricManager metricManager = MetricManager.INSTANCE;
    private IndexProvider indexProvider;
    private String prefix;
    private static final String M_MUTATE = "mutate";
    private static final String M_RESTORE = "restore";
    private static final String M_QUERY = "query";
    private static final String M_RAW_QUERY = "rawQuery";
    private static final String M_TOTALS = "totals";
    private static final String M_CALLS = "calls";
    private static final String M_TIME = "time";
    private static final String M_EXCEPTIONS = "exceptions";

    public MetricInstrumentedIndexProvider(final IndexProvider indexProvider, String prefix) {
        this.indexProvider = indexProvider;
        this.prefix = prefix;
    }

    @Override
    public void register(final String store, final String key, final KeyInformation information, final BaseTransaction tx) throws BackendException {
        indexProvider.register(store, key, information, tx);
    }

    @Override
    public void mutate(final Map<String, Map<String, IndexMutation>> mutations, final KeyInformation.IndexRetriever information,
                       final BaseTransaction tx) throws BackendException {
        runWithMetrics(prefix, M_MUTATE, () -> indexProvider.mutate(mutations, information, tx));
    }

    @Override
    public void restore(
        final Map<String, Map<String, List<IndexEntry>>> documents, final KeyInformation.IndexRetriever information,
        final BaseTransaction tx) throws BackendException {
        runWithMetrics(prefix, M_RESTORE, () -> indexProvider.restore(documents, information, tx));
    }

    @Override
    public Stream<String> query(final IndexQuery query, final KeyInformation.IndexRetriever information,
                                final BaseTransaction tx) throws BackendException {
        return runWithMetrics(prefix, M_QUERY, () -> indexProvider.query(query, information, tx));
    }

    @Override
    public Stream<RawQuery.Result<String>> query(final RawQuery query, final KeyInformation.IndexRetriever information,
                                                 final BaseTransaction tx) throws BackendException {
        return runWithMetrics(prefix, M_RAW_QUERY, () -> indexProvider.query(query, information, tx));
    }

    @Override
    public Long totals(final RawQuery query, final KeyInformation.IndexRetriever information, final BaseTransaction tx) throws BackendException {
        return runWithMetrics(prefix, M_TOTALS, () -> indexProvider.totals(query, information, tx));
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return indexProvider.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        indexProvider.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        indexProvider.clearStorage();
    }

    @Override
    public boolean exists() throws BackendException {
        return indexProvider.exists();
    }

    @Override
    public boolean supports(final KeyInformation information, final JanusGraphPredicate janusgraphPredicate) {
        return indexProvider.supports(information, janusgraphPredicate);
    }

    @Override
    public boolean supports(final KeyInformation information) {
        return indexProvider.supports(information);
    }

    @Override
    public String mapKey2Field(final String key, final KeyInformation information) {
        return indexProvider.mapKey2Field(key, information);
    }

    @Override
    public IndexFeatures getFeatures() {
        return indexProvider.getFeatures();
    }

    void runWithMetrics(String prefix, String name, StorageRunnable impl) throws BackendException {
        if (null == prefix) {
            impl.run();
        }

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(impl);

        metricManager.getCounter(prefix, null, name, M_CALLS).inc();
        final Timer.Context tc = metricManager.getTimer(prefix, null, name, M_TIME).time();
        try {
            impl.run();
        } catch (RuntimeException e) {
            metricManager.getCounter(prefix, null, name, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    <T> T runWithMetrics(String prefix, String name, StorageCallable<T> impl) throws BackendException {
        if (null == prefix) {
            return impl.call();
        }

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(impl);

        metricManager.getCounter(prefix, null, name, M_CALLS).inc();
        final Timer.Context tc = metricManager.getTimer(prefix, null, name, M_TIME).time();
        try {
            return impl.call();
        } catch (RuntimeException e) {
            metricManager.getCounter(prefix, null, name, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }
}
