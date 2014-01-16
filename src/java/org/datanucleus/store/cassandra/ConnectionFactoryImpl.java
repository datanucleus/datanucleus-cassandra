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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

/**
 * Connection factory for Cassandra datastores.
 * Accepts a URL of the form 
 * <pre>cassandra:[{server1}][/{dbName}][,{server2}[,{server3}]]</pre>
 * Defaults to a server of "localhost"/"127.0.0.1" if nothing specified
 * TODO Update this URL to include all config that Cassandra allows
 * TODO Should we use one Session per EMF/PMF ? or one per EM/PM ? sine Cassandra doesn't do real txns then not obvious. Are they thread-safe?
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
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
            remains = remains.substring(1);
        }
        // TODO Parse the connection URL and add contact point(s)

        Builder builder = Cluster.builder();
        builder.addContactPoint("127.0.0.1");
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
     * @param txnOptionsIgnored Any options for then creating the connection
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