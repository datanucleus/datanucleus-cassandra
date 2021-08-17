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
    barisergun75@gmail.com
 ***********************************************************************/
package org.datanucleus.store.cassandra.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.types.TypeManager;

/**
 * CQL query for Cassandra. Allows the user to execute a CQL query and return the results in the form "List&lt;Object[]&gt;".
 */
public class CQLQuery extends AbstractJavaQuery
{
    private static final long serialVersionUID = 2808968696540162104L;

    /**
     * The compilation of the query for this datastore. Not applicable if totally in-memory.
     */
    protected transient CassandraQueryCompilation datastoreCompilation;

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
        this.cql = q != null ? q.cql : null;
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

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractJavaQuery#getSingleStringQuery()
     */
    @Override
    public String getSingleStringQuery()
    {
        return cql;
    }

    @Override
    public void compileGeneric(Map parameterValues)
    {
    }

    @Override
    protected void compileInternal(Map parameterValues)
    {

    }

    @Override
    protected Object performExecute(Map parameters)
    {
        if (type == QueryType.SELECT)
        {
            ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
            try
            {
                List results = new ArrayList();
                CqlSession session = (CqlSession) mconn.getConnection();

                long startTime = System.currentTimeMillis();
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("021046", "CQL", getSingleStringQuery(), null));
                }

                SimpleStatement stmt = SimpleStatement.newInstance(cql);
                int fetchSize = this.getFetchPlan().getFetchSize();
                if (0 < fetchSize)
                {
                    stmt.setPageSize(fetchSize);
                }
                ResultSet rs = session.execute(stmt);

                Class resultCls = this.getResultClass();
                ResultClassInfo rci = (resultCls != null) ? CassandraUtils.getResultClassInfoFromColumnDefinitions(resultCls, rs.getColumnDefinitions()) : null;

                CQLQueryResult queryResult = new CQLQueryResult(this, rs);
                queryResult.initialise();

                TypeManager typeMgr = storeMgr.getNucleusContext().getTypeManager();
                Iterator<Object> iter = queryResult.iterator();
                while (iter.hasNext())
                {
                    Row row = (Row) iter.next();
                    if (rci != null)
                    {
                        Object[] rowResult = CassandraUtils.getObjectArrayFromRow(typeMgr, row, rs.getColumnDefinitions(), rci.getFieldsMatchingColumnIndexes(), rci.getFields().length);
                        Object rcResult = QueryUtils.createResultObjectUsingDefaultConstructorAndSetters(resultClass, rci.getFieldNames(), rci.getFields(), rowResult);
                        results.add(rcResult);
                    }
                    else
                    {
                        Object[] rowResult = CassandraUtils.getObjectArrayFromRow(typeMgr, row, rs.getColumnDefinitions(), new ArrayList<Integer>(), rs.getColumnDefinitions().size());
                        results.add(rowResult);// get raw result as Object[]
                    }
                }
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("021074", "CQL", "" + (System.currentTimeMillis() - startTime)));
                }
                queryResult.close();

                return results;
            }
            finally
            {
                mconn.release();
            }
        }
        else if (type == QueryType.BULK_DELETE || type == QueryType.BULK_UPDATE)
        {
            // TODO
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else
        {
            // TODO
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
