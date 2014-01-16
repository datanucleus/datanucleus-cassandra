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

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

/**
 * Connection factory for Cassandra datastores.
 * Accepts a URL of the form 
 * <pre>cassandra:[{server1}][/{dbName}][,{server2}[,{server3}]]</pre>
 * Defaults to a server of "localhost"/"127.0.0.1" if nothing specified
 * TODO Update this URL to include all config that Cassandra allows
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.connection.ConnectionFactory#createManagedConnection(org.datanucleus.ExecutionContext, java.util.Map)
     */
    @Override
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map transactionOptions)
    {
        // TODO Implement this
        throw new UnsupportedOperationException("Dont currently support connecting to Cassandra");
    }
}