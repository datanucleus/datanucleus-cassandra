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

import java.util.ArrayList;
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
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Connection factory for Cassandra datastores.
 * Accepts a URL of the form <pre>cassandra:[host1:port[,host2[,host3]]]</pre>
 * Defaults to a server of "127.0.0.1" if no host/port specified
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

    Cluster cluster;

    boolean sessionPerManager = false;

    Session session = null;

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
        List<String> hosts = new ArrayList();
        String port = null;
        while (tokeniser.hasMoreTokens())
        {
            String token = tokeniser.nextToken().trim();
            if (token.indexOf(':') == 0)
            {
                token = token.substring(1).trim();
            }

            String hostStr = null;
            if (!StringUtils.isWhitespace(token))
            {
                int nextSemiColon = token.indexOf(':');
                if (nextSemiColon > 0)
                {
                    port = token.substring(nextSemiColon+1);
                    hostStr = token.substring(0, nextSemiColon);
                }
                else
                {
                    hostStr = token.trim();
                }
            }
            hosts.add(hostStr);
        }

        Builder builder = Cluster.builder();
        if (hosts.size() > 0)
        {
            NucleusLogger.CONNECTION.debug("Starting Cassandra Cluster for hosts " + StringUtils.collectionToString(hosts));
            for (String host : hosts)
            {
                builder.addContactPoint(host);
            }
        }
        else
        {
            // Fallback to localhost
            NucleusLogger.CONNECTION.debug("Starting Cassandra Cluster for host 127.0.0.1");
            builder.addContactPoint("127.0.0.1");
        }
        if (port != null)
        {
            int portNumber = 0;
            try
            {
                portNumber = Integer.valueOf(port);
            }
            catch (NumberFormatException nfe)
            {
                NucleusLogger.CONNECTION.warn("Unable to convert '" + port + "' to port number for Cassandra, so ignoring");
            }
            if (portNumber > 0)
            {
                builder.withPort(portNumber);
            }
        }

        // Add any login credentials
        String user = storeMgr.getConnectionUserName();
        String passwd = storeMgr.getConnectionPassword();
        if (!StringUtils.isWhitespace(user))
        {
            builder.withCredentials(user, passwd);
        }

        // TODO Support any other config options

        String compression = storeMgr.getStringProperty(CASSANDRA_COMPRESSION);
        if (!StringUtils.isWhitespace(compression))
        {
            builder.withCompression(Compression.valueOf(compression.toUpperCase()));
        }

        Boolean useSSL = storeMgr.getBooleanObjectProperty(CASSANDRA_SSL); // Defaults to false
        if (useSSL != null && useSSL)
        {
            builder.withSSL();
        }

        Boolean useMetrics = storeMgr.getBooleanObjectProperty(CASSANDRA_METRICS); // Defaults to true
        if (useMetrics != null && !useMetrics)
        {
            builder.withoutMetrics();
        }

        Boolean sessionPerManagerProperty = storeMgr.getBooleanObjectProperty(CASSANDRA_CONNECTION_PER_MANAGER);
        if (sessionPerManagerProperty != null && sessionPerManagerProperty)
        {
            sessionPerManager = true;
        }

        // Specify any socket options
        SocketOptions socketOpts = null;
        int readTimeout = storeMgr.getIntProperty(CASSANDRA_SOCKET_READ_TIMEOUT_MILLIS);
        if (readTimeout != 0)
        {
            socketOpts = new SocketOptions();
            socketOpts.setReadTimeoutMillis(readTimeout);
        }
        int connectTimeout = storeMgr.getIntProperty(CASSANDRA_SOCKET_CONNECT_TIMEOUT_MILLIS);
        if (connectTimeout != 0)
        {
            if (socketOpts == null)
            {
                socketOpts = new SocketOptions();
            }
            socketOpts.setConnectTimeoutMillis(connectTimeout);
        }
        if (socketOpts != null)
        {
            builder.withSocketOptions(socketOpts);
        }

        // Get the Cluster
        cluster = builder.build();
    }

    public void close()
    {
        if (session != null)
        {
            NucleusLogger.CONNECTION.debug("Shutting down Cassandra Session");
            session.shutdown();
        }
        NucleusLogger.CONNECTION.debug("Shutting down Cassandra Cluster");
        cluster.shutdown();

        super.close();
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction associated to the ExecutionContext
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
                    conn = cluster.connect();
                    NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - obtained Session");
                }
                else
                {
                    if (session == null)
                    {
                        session = cluster.connect();
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
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
            }

            NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - closed connection");

            // Removes the connection from pooling
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
            }

            if (sessionPerManager)
            {
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " - shutdown Session");
                ((Session)conn).shutdown();
            }
            conn = null;
            xaRes = null;
        }

        public XAResource getXAResource()
        {
            if (xaRes == null)
            {
                if (conn == null)
                {
                    obtainNewConnection();
                }
                xaRes = new EmulatedXAResource(this, (Session)conn);
            }
            return xaRes;
        }

        /* (non-Javadoc)
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
        Session session;

        EmulatedXAResource(ManagedConnectionImpl mconn, Session session)
        {
            super(mconn);
            this.session = session;
        }
        // Cassandra has no commit/rollback as such so just use superclass logging
    }
}