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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * JDOQL query for Cassandra.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{
    /**
     * Constructs a new query instance that uses the given execution context.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
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

            List candidates = null;
            if (candidateCollection == null)
            {
                candidates = getCandidatesForQuery(session);
            }
            else
            {
                candidates = new ArrayList(candidateCollection);
            }
            // TODO Evaluate as much as possible in the datastore using QueryToCQLMapper

            // Evaluate result/filter/grouping/having/ordering in-memory
            JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation,
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

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

    /**
     * Convenience method that returns all candidate objects for this query.
     * This is performed using a "SELECT * FROM schema.table" for the candidate, and optionally its subclasses.
     * @param session The session
     * @return The candidate objects
     */
    protected List getCandidatesForQuery(Session session)
    {
        // TODO Create lazy-loading QueryResult object to contain these and return that
        List candidateObjs = new ArrayList();

        CassandraStoreManager storeMgr = (CassandraStoreManager)this.storeMgr;
        List<AbstractClassMetaData> cmds =
            MetaDataUtils.getMetaDataForCandidates(getCandidateClass(), isSubclasses(), ec);
        for (AbstractClassMetaData cmd : cmds)
        {
            // TODO Remove this and when class is registered, use listener to manage it
            storeMgr.manageClasses(clr, cmd.getFullClassName());

            // Obtain candidate objects for this class
            StringBuilder stmtBuilder = new StringBuilder("SELECT * FROM ");

            Table table = (Table) storeMgr.getStoreDataForClass(cmd.getFullClassName()).getProperties().get("tableObject");
            stmtBuilder.append(table.getSchemaName()).append('.').append(table.getIdentifier());
            // TODO Add discriminator restriction if table is being shared (when we support table sharing)

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                String multitenancyValue = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID);
                stmtBuilder.append(" WHERE ").append(table.getMultitenancyColumn().getIdentifier()).append("='").append(multitenancyValue).append("'");
            }

            // Execute the SELECT
            CassandraUtils.logCqlStatement(stmtBuilder.toString(), null, NucleusLogger.DATASTORE_NATIVE);
            ResultSet rs = session.execute(stmtBuilder.toString());

            // Extract the candidates from the ResultSet
            Iterator<Row> iter = rs.iterator();
            while (iter.hasNext())
            {
                Row row = iter.next();
                candidateObjs.add(CassandraUtils.getPojoForRowForCandidate(row, cmd, ec, getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers(), getIgnoreCache()));
            }
        }

        return candidateObjs;
    }
}