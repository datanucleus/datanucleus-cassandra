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

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.cassandra.query;

import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.Session;

/**
 * CQL query for Cassandra.
 */
public class CQLQuery extends AbstractJavaQuery
{
    String cql;

    /**
     * Constructs a new query instance that uses the given execution context.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public CQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (CQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public CQLQuery(StoreManager storeMgr, ExecutionContext ec, CQLQuery q)
    {
        super(storeMgr, ec);
        this.cql = q.cql;
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public CQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec);
        this.cql = query;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractJavaQuery#getSingleStringQuery()
     */
    @Override
    public String getSingleStringQuery()
    {
        return cql;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#compileInternal(java.util.Map)
     */
    @Override
    protected void compileInternal(Map parameterValues)
    {
        // TODO Auto-generated method stub
        
    }

    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        try
        {
            Session session = (Session) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }

            /*ResultSet rs =*/ session.execute(cql);
            List results = null; // TODO Convert into some sort of result class

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }
}