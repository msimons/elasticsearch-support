/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.GcMonitor;
import org.xbib.elasticsearch.helper.network.NetworkUtils;
import org.xbib.elasticsearch.plugin.helper.HelperPlugin;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

abstract class BaseTransportClient extends BaseClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseTransportClient.class.getName());

    protected TransportClient client;

    protected GcMonitor gcmon;

    protected boolean ignoreBulkErrors;

    private boolean isShutdown;

    @Override
    protected void createClient(Settings settings) {
        if (client != null) {
            logger.warn("client is open, closing...");
            client.close();
            client.threadPool().shutdown();
            logger.warn("client is closed");
            client = null;
        }
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            Settings effectiveSettings = Settings.builder().put(settings)
                    .put("client.transport.ignore_cluster_name", settings.getAsBoolean("client.transport.ignore_cluster_name", false))
                    .put("client.transport.ping_timeout", settings.getAsTime("client.transport.ping_timeout", TimeValue.timeValueSeconds(15)))
                    .put("client.transport.nodes_sampler_interval", settings.getAsTime("client.transport.nodes_sampler_interval", TimeValue.timeValueSeconds(15)))
                    .build();
            logger.info("creating transport client on {} with effective settings {}",
                    version, effectiveSettings.getAsMap());
            this.client = TransportClient.builder()
                    .addPlugin(HelperPlugin.class)
                    .settings(effectiveSettings)
                    .build();
            this.gcmon = new GcMonitor(settings);
            this.ignoreBulkErrors = settings.getAsBoolean("ignoreBulkErrors", true);
        }
    }

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    public synchronized void shutdown() {
        if (client != null) {
            logger.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            logger.debug("shutdown complete");
        }
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    protected boolean connect(Settings settings, Collection<InetSocketTransportAddress> addresses, boolean autodiscover) {
        logger.info("trying to connect to {}", addresses);
        int retries = settings.getAsInt("client.connect.retries", 3);
        try {
            do {
                for (InetSocketTransportAddress address : addresses) {
                    client.addTransportAddress(address);
                }
                List<DiscoveryNode> nodes = client.connectedNodes();
                if (nodes != null && !nodes.isEmpty()) {
                    logger.info("connected to {}", nodes);
                    if (autodiscover) {
                        logger.info("trying to auto-discover all cluster nodes...");
                        ClusterStateRequestBuilder clusterStateRequestBuilder =
                                new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE);
                        ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                        DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                        for (DiscoveryNode node : discoveryNodes) {
                            logger.info("connecting to auto-discovered node {}", node);
                            try {
                                client.addTransportAddress(node.address());
                            } catch (Exception e) {
                                logger.warn("can't connect to node " + node, e);
                            }
                        }
                        logger.info("after auto-discovery connected to {}", client.connectedNodes());
                    }
                    return true;
                }
                logger.info("no nodes found, waiting to try again ...");
                Thread.sleep(5000L);
            } while (--retries > 0);
        } catch (InterruptedException e) {
            logger.warn("interrupted");
            Thread.currentThread().interrupt();
        }
        return false;
    }

    protected void createClient(Map<String, String> settings) throws IOException {
        createClient(Settings.builder().put(settings).build());
    }

    protected Settings findSettings() {
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put("host", "localhost");
        try {
            String hostname = NetworkUtils.getLocalAddress().getHostName();
            logger.debug("the hostname is {}", hostname);
            settingsBuilder.put("host", hostname)
                    .put("port", 9300);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return settingsBuilder.build();
    }

    protected Collection<InetSocketTransportAddress> findAddresses(Settings settings) throws IOException {
        String[] hostnames = settings.getAsArray("host", new String[]{"localhost"});
        int port = settings.getAsInt("port", 9300);
        Collection<InetSocketTransportAddress> addresses = new ArrayList<>();
        for (String hostname : hostnames) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                try {
                    port = Integer.parseInt(splitHost[1]);
                } catch (Exception e) {
                    // ignore
                }
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
        }
        return addresses;
    }


}
