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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * CQL query for Cassandra.
 * Allows the user to execute a CQL query and return the results in the form "List&lt;Object[]&gt;".
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
        List results = new ArrayList();
        try
        {
            Session session = (Session) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }

            // TODO Return as QueryResult that wraps ResultSet so we can fetch lazily
            ResultSet rs = session.execute(cql);
            List<Definition> defs = rs.getColumnDefinitions().asList();
            Iterator<Row> iter = rs.iterator();
            while (iter.hasNext())
            {
                Row row = iter.next();
                Object[] resultRow = new Object[defs.size()];
                int i=0;
                for (Definition def : defs)
                {
                    DataType colType = def.getType();
                    if (colType == DataType.varchar())
                    {
                        resultRow[i] = row.getString(i);
                    }
                    else if (colType == DataType.bigint())
                    {
                        resultRow[i] = row.getLong(i);
                    }
                    else if (colType == DataType.decimal())
                    {
                        resultRow[i] = row.getDecimal(i);
                    }
                    else if (colType == DataType.cfloat())
                    {
                        resultRow[i] = row.getFloat(i);
                    }
                    else if (colType == DataType.cdouble())
                    {
                        resultRow[i] = row.getDouble(i);
                    }
                    else if (colType == DataType.cboolean())
                    {
                        resultRow[i] = row.getBool(i);
                    }
                    else if (colType == DataType.timestamp())
                    {
                        resultRow[i] = row.getDate(i);
                    }
                    else if (colType == DataType.varint())
                    {
                        resultRow[i] = row.getInt(i);
                    }
                    else
                    {
                        NucleusLogger.QUERY.warn("Column " + i + " of results is of unsupported type (" + colType + ") : returning null");
                        resultRow[i] = null;
                    }
                    i++;
                }
                results.add(resultRow);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }
}