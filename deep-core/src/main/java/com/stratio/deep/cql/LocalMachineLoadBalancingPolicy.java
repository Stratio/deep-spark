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

package com.stratio.deep.cql;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.AbstractIterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RoundRobinPolicy;

/**
 * Load balancing policy that, unlike round robin, sticks to one host.
 */
public class LocalMachineLoadBalancingPolicy extends RoundRobinPolicy {
    private InetAddress host;

	/**
	 * Public constructor.
	 */
    public LocalMachineLoadBalancingPolicy(InetAddress host) {
        this.host = host;
    }

    private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
    private final AtomicInteger index = new AtomicInteger();

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {

        for (Host h : hosts) {
            if (h.getAddress().equals(host)) {
                this.liveHosts.add(h);
                this.index.set(0);
                break;
            }
        }
    }

    /**
     * Return the HostDistance for the provided host.
     * <p/>
     * This policy consider all nodes as local. This is generally the right
     * thing to do in a single datacenter deployment. If you use multiple
     * datacenter, see {@link com.datastax.driver.core.policies.DCAwareRoundRobinPolicy} instead.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        return HostDistance.LOCAL;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * The returned plan will try each known host of the cluster. Upon each
     * call to this method, the {@code i}th host of the plans returned will cycle
     * over all the hosts of the cluster in a round-robin fashion.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this
     *                       query.
     * @param statement      the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to
     * try first for querying, which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {

        // We clone liveHosts because we want a version of the list that
        // cannot change concurrently of the query plan iterator (this
        // would be racy). We use clone() as it don't involve a copy of the
        // underlying array (and thus we rely on liveHosts being a CopyOnWriteArrayList).
        @SuppressWarnings("unchecked")
        final List<Host> hosts = (List<Host>) liveHosts.clone();
        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000) {
            index.set(0);
        }

        return new AbstractIterator<Host>() {

            private int idx = startIdx;
            private int remaining = hosts.size();

            @Override
            protected Host computeNext() {
                if (remaining <= 0) {
                    return endOfData();
                }

                remaining--;
                int c = idx++ % hosts.size();
                if (c < 0) {
                    c += hosts.size();
                }
                return hosts.get(c);
            }
        };
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void onUp(Host host) {
        liveHosts.addIfAbsent(host);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void onDown(Host host) {
        liveHosts.remove(host);
    }

	/**
	 * Adds the given new host only if the list of known hosts is empty.
	 *
	 * @param host the host to add.
	 */
    @Override
    public void onAdd(Host host) {
        if (liveHosts.isEmpty()) {
            onUp(host);
        }
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void onRemove(Host host) {
        onDown(host);
    }
}
