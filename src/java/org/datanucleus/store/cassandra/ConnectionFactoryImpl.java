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

import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
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
 * Accepts a URL of the form 
 * <pre>cassandra:[{host:port}]</pre>
 * Defaults to a server of "127.0.0.1" if nothing specified
 * TODO Update this URL to include all config that Cassandra allows
 * TODO Should we use one Session per EMF/PMF ? or one per EM/PM ? since Cassandra doesn't do real txns then not obvious. Are they thread-safe? Currently does one Session per PM/EM
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    public static final String CASSANDRA_COMPRESSION = "datanucleus.cassandra.compression";
    public static final String CASSANDRA_METRICS = "datanucleus.cassandra.metrics";
    public static final String CASSANDRA_SSL = "datanucleus.cassandra.ssl";
    public static final String CASSANDRA_SOCKET_READ_TIMEOUT_MILLIS = "datanucleus.cassandra.socket.readTimeoutMillis";
    public static final String CASSANDRA_SOCKET_CONNECT_TIMEOUT_MILLIS = "datanucleus.cassandra.socket.connectTimeoutMillis";

    Cluster cluster;

    /**
     * @param storeMgr
     * @param resourceType
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "cassandra:[server]"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property 'datanucleus.ConnectionURL' (or alias)");
        }

        String remains = url.substring(9).trim();
        if (remains.indexOf(':') == 0)
        {
            remains = remains.substring(1).trim();
        }
        String host = "127.0.0.1";
        String port = null;
        if (!StringUtils.isWhitespace(remains))
        {
            int nextSemiColon = remains.indexOf(':');
            if (nextSemiColon > 0)
            {
                port = remains.substring(nextSemiColon+1);
                host = remains.substring(0, nextSemiColon);
            }
            else
            {
                host = remains.trim();
            }
        }

        Builder builder = Cluster.builder();
        builder.addContactPoint(host);
        // TODO Support multiple contact points?
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
        cluster.shutdown();

        super.close();
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param transactionOptions Any options for then creating the connection (currently ignored)
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map transactionOptions)
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
                conn = cluster.connect();
            }
        }

        public void release()
        {
            if (commitOnRelease)
            {
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
                // TODO Commit the Session ?
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");
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

            NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
            // TODO End the Session?
            NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");

            // Removes the connection from pooling
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
            }

            ((Session)conn).shutdown();
            conn = null;
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
    }

    /**
     * Emulate the two phase protocol for non XA.
     */
    static class EmulatedXAResource implements XAResource
    {
        ManagedConnectionImpl mconn;
        Session session;

        EmulatedXAResource(ManagedConnectionImpl mconn, Session session)
        {
            this.mconn = mconn;
            this.session = session;
        }

        public void commit(Xid xid, boolean onePhase) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is committing for transaction "+xid.toString()+" with onePhase="+onePhase);
            // TODO Commit of Session (not that we can)
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " committed connection for transaction "+xid.toString()+" with onePhase="+onePhase);
        }

        public void rollback(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is rolling back for transaction "+xid.toString());
            // TODO Rollback of Session (not that we can)
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " rolled back connection for transaction "+xid.toString());
        }

        public void start(Xid xid, int flags) throws XAException
        {
        }
        public int prepare(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is preparing for transaction "+xid.toString());
            return 0;
        }
        public void forget(Xid xid) throws XAException
        {
        }
        public void end(Xid xid, int flags) throws XAException
        {
        }
        public Xid[] recover(int flags) throws XAException
        {
            throw new XAException("Unsupported operation");
        }
        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }
        public boolean setTransactionTimeout(int timeout) throws XAException
        {
            return false;
        }
        public boolean isSameRM(XAResource xares) throws XAException
        {
            return (this == xares);
        }
    }
}