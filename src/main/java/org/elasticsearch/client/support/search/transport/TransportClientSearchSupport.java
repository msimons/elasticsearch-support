/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.support.search.transport;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.search.support.BasicRequest;
import org.elasticsearch.client.support.TransportClientSupport;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * TransportClient support class
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TransportClientSearchSupport extends TransportClientSupport implements TransportClientSearch {

    private final static ESLogger logger = Loggers.getLogger(TransportClientSearchSupport.class);

    // singleton
    protected TransportClient client;
    // the settings
    protected Settings settings;
    // the default index
    private String index;

    public TransportClientSearchSupport setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public TransportClientSearchSupport newClient() {
        super.newClient();
        return this;
    }

    @Override
    public TransportClientSearchSupport newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    /**
     * Create settings
     *
     * @param uri
     * @return the settings
     */
    protected Settings initialSettings(URI uri) {
        int n = Runtime.getRuntime().availableProcessors();
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false) // sniff would join us into any cluster ... bug?
                .put("transport.netty.worker_count", n * 4)
                .put("transport.netty.connections_per_node.low", 0)
                .put("transport.netty.connections_per_node.med", 0)
                .put("transport.netty.connections_per_node.high", n * 4)
                .put("threadpool.index.type", "fixed")
                .put("threadpool.index.size", 1)
                .put("threadpool.bulk.type", "fixed")
                .put("threadpool.bulk.size", 1)
                .put("threadpool.get.type", "fixed")
                .put("threadpool.get.size", n * 4)
                .put("threadpool.search.type", "fixed")
                .put("threadpool.search.size", n * 4)
                .put("threadpool.percolate.type", "fixed")
                .put("threadpool.percolate.size", 1)
                .put("threadpool.management.type", "fixed")
                .put("threadpool.management.size", 1)
                .put("threadpool.flush.type", "fixed")
                .put("threadpool.flush.size", 1)
                .put("threadpool.merge.type", "fixed")
                .put("threadpool.merge.size", 1)
                .put("threadpool.refresh.type", "fixed")
                .put("threadpool.refresh.size", 1)
                .put("threadpool.cache.type", "fixed")
                .put("threadpool.cache.size", 1)
                .put("threadpool.snapshot.type", "fixed")
                .put("threadpool.snapshot.size", 1)
                .build();
    }

    @Override
    public BasicRequest newSearchRequest() {
        return new BasicRequest()
                .newSearchRequest(client.prepareSearch().setPreference("_primary_first"));
    }

    @Override
    public BasicRequest newGetRequest() {
        return new BasicRequest()
                .newGetRequest(client.prepareGet());
    }

}