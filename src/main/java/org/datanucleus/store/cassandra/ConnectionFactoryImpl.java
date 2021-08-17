/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.cassandra;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * Connection factory for Cassandra datastores. Accepts a URL of the form
 * <pre>
 * cassandra:[host1:port[,host2[,host3]]]
 * </pre>
 * 
 * Defaults to a server of "127.0.0.1" if no host/port specified. 
 * Defaults to a single Session per PMF/EMF, but can be overridden using "datanucleus.cassandra.sessionPerManager".
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    public static final String CASSANDRA_CONNECTION_PER_MANAGER = "datanucleus.cassandra.sessionPerManager";

    public static final String CASSANDRA_COMPRESSION = "datanucleus.cassandra.compression";

    public static final String CASSANDRA_METRICS = "datanucleus.cassandra.metrics";

    public static final String CASSANDRA_SSL = "datanucleus.cassandra.ssl";

    public static final String CASSANDRA_SOCKET_READ_TIMEOUT_MILLIS = "datanucleus.cassandra.socket.readTimeoutMillis";

    public static final String CASSANDRA_SOCKET_CONNECT_TIMEOUT_MILLIS = "datanucleus.cassandra.socket.connectTimeoutMillis";

    public static final String CASSANDRA_LOAD_BALANCING_POLICY = "datanucleus.cassandra.loadBalancingPolicy";

    public static final String CASSANDRA_LOAD_BALANCING_POLICY_TOKEN_AWARE_LOCAL_DC = "datanucleus.cassandra.loadBalancingPolicy.tokenAwareLocalDC";

    boolean sessionPerManager = false;

    CqlSessionBuilder sessionBuilder = null;

    /** CqlSession used when we have a single CqlSession per PMF/EMF. */
    CqlSession session = null;

    /**
     * Constructor for a factory.
     * @param storeMgr StoreManager
     * @param resourceType Resource type (not of relevance since we use a single factory)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "cassandra:[server]"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }
        String remains = url.trim().substring(9).trim(); // Strip "cassandra"
        if (remains.length() > 0)
        {
            remains = remains.substring(1); // Strip ":"
        }

        // Extract host(s)/port
        StringTokenizer tokeniser = new StringTokenizer(remains, ",");
        List<String> hosts = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        while (tokeniser.hasMoreTokens())
        {
            String token = tokeniser.nextToken().trim();
            if (token.indexOf(':') == 0)
            {
                token = token.substring(1).trim();
            }

            String hostStr = null;
            String portStr = null;
            int portNumber = 9042;
            if (!StringUtils.isWhitespace(token))
            {
                int nextSemiColon = token.indexOf(':');
                if (nextSemiColon > 0)
                {
                    hostStr = token.substring(0, nextSemiColon);
                    portStr = token.substring(nextSemiColon + 1);
                    if (portStr != null)
                    {
                        try
                        {
                            portNumber = Integer.valueOf(portStr);
                        }
                        catch (NumberFormatException nfe)
                        {
                            NucleusLogger.CONNECTION.warn("Unable to convert '" + portStr + "' to port number for Cassandra, so ignoring");
                        }
                    }
                }
                else
                {
                    hostStr = token.trim();
                }
            }
            hosts.add(hostStr);
            ports.add(portNumber);
        }

        sessionBuilder = CqlSession.builder();
        if (!hosts.isEmpty())
        {
            NucleusLogger.CONNECTION.debug("Starting Cassandra CqlSessionBuilder for hosts " + StringUtils.collectionToString(hosts));
            Iterator<String> hostIter = hosts.iterator();
            Iterator<Integer> portIter = ports.iterator();
            while (hostIter.hasNext())
            {
                String host = hostIter.next();
                Integer port = portIter.next();
                InetSocketAddress addr = new InetSocketAddress(host, port);
                sessionBuilder.addContactPoint(addr);
            }
        }
        else
        {
            // Fallback to Cassandra default (localhost:9042) - no need to add a contact point
            NucleusLogger.CONNECTION.debug("Starting Cassandra CqlSessionBuilder with default contact point(s)");
        }

        // Add any login credentials
        String user = storeMgr.getConnectionUserName();
        String passwd = storeMgr.getConnectionPassword();
        if (!StringUtils.isWhitespace(user))
        {
            sessionBuilder.withAuthCredentials(user, passwd);
        }

        // TODO Support any other config options

        /*
         * String compression = storeMgr.getStringProperty(CASSANDRA_COMPRESSION); if
         * (!StringUtils.isWhitespace(compression)) {
         * builder.withCompression(Compression.valueOf(compression.toUpperCase())); }
         */

        /*
         * Boolean useSSL = storeMgr.getBooleanObjectProperty(CASSANDRA_SSL); // Defaults to false if (useSSL
         * != null && useSSL) { builder.withSSL(); }
         */

        /*
         * Boolean useMetrics = storeMgr.getBooleanObjectProperty(CASSANDRA_METRICS); // Defaults to true if
         * (useMetrics != null && !useMetrics) { builder.withoutMetrics(); }
         */

        Boolean sessionPerManagerProperty = storeMgr.getBooleanObjectProperty(CASSANDRA_CONNECTION_PER_MANAGER);
        if (sessionPerManagerProperty != null && sessionPerManagerProperty)
        {
            sessionPerManager = true;
        }

        // Specify any socket options
//        SocketOptions socketOpts = null;
//        int readTimeout = storeMgr.getIntProperty(CASSANDRA_SOCKET_READ_TIMEOUT_MILLIS);
//        if (readTimeout != 0)
//        {
//            socketOpts = new SocketOptions();
//            socketOpts.setReadTimeoutMillis(readTimeout);
//        }
//        int connectTimeout = storeMgr.getIntProperty(CASSANDRA_SOCKET_CONNECT_TIMEOUT_MILLIS);
//        if (connectTimeout != 0)
//        {
//            if (socketOpts == null)
//            {
//                socketOpts = new SocketOptions();
//            }
//            socketOpts.setConnectTimeoutMillis(connectTimeout);
//        }
//        if (socketOpts != null)
//        {
//            builder.withSocketOptions(socketOpts);
//        }

        // Load balancing policy
//        String loadBalancingPolicy = storeMgr.getStringProperty(CASSANDRA_LOAD_BALANCING_POLICY);
//        if (!StringUtils.isWhitespace(loadBalancingPolicy))
//        {
//            if (loadBalancingPolicy.equalsIgnoreCase("round-robin"))
//            {
//                builder.withLoadBalancingPolicy(new RoundRobinPolicy());
//            }
//            else if (loadBalancingPolicy.equalsIgnoreCase("token-aware"))
//            {
//                String localDC = storeMgr.getStringProperty(CASSANDRA_LOAD_BALANCING_POLICY_TOKEN_AWARE_LOCAL_DC);
//                if (!StringUtils.isWhitespace(localDC))
//                {
//                    builder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(localDC).build());
//                }
//            }
//            else
//            {
//                NucleusLogger.CONNECTION.error("Supplied Cassandra LoadBalancingPolicy of " + loadBalancingPolicy + " is not supported. " +
//                    "Provide a GitHub pull request to contribute support");
//            }
//        }
    }

    public void close()
    {
        if (session != null)
        {
            NucleusLogger.CONNECTION.debug("Closed Cassandra CqlSession");
            session.close();
        }

        super.close();
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction associated
     * to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options Any options for then creating the connection (currently ignored)
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return new ManagedConnectionImpl();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        XAResource xaRes = null;

        public Object getConnection()
        {
            if (conn == null)
            {
                obtainNewConnection();
            }
            return conn;
        }

        protected void obtainNewConnection()
        {
            if (conn == null)
            {
                // Create new connection
                if (sessionPerManager)
                {
                    conn = sessionBuilder.build();
                    NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - obtained CqlSession");
                }
                else
                {
                    if (session == null)
                    {
                        session = sessionBuilder.build();
                    }
                    NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - using connection");
                    conn = session;
                }
            }
        }

        public void release()
        {
            if (commitOnRelease)
            {
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - released connection");
            }
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }

            // Notify anything using this connection to use it now
            for (int i = 0; i < listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPreClose();
            }

            NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - closed connection");

            // Removes the connection from pooling
            for (int i = 0; i < listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPostClose();
            }

            if (sessionPerManager)
            {
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - close CqlSession");
                ((CqlSession)conn).close();
            }
            xaRes = null;

            super.close();
        }

        public XAResource getXAResource()
        {
            if (xaRes == null)
            {
                if (conn == null)
                {
                    obtainNewConnection();
                }
                xaRes = new EmulatedXAResource(this, (CqlSession)conn);
            }
            return xaRes;
        }

        /*
         * (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#closeAfterTransactionEnd()
         */
        @Override
        public boolean closeAfterTransactionEnd()
        {
            if (storeMgr.getBooleanProperty(PropertyNames.PROPERTY_CONNECTION_SINGLE_CONNECTION))
            {
                // Hang on to connection
                return false;
            }
            return super.closeAfterTransactionEnd();
        }
    }

    /**
     * Emulate the two phase protocol for non XA.
     */
    static class EmulatedXAResource extends AbstractEmulatedXAResource
    {
        EmulatedXAResource(ManagedConnectionImpl mconn, CqlSession session)
        {
            super(mconn);
        }
        // Cassandra has no commit/rollback as such so just use superclass logging
    }
}