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
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * JPQL query for Cassandra.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, Object q)
    {
        super(storeMgr, ec, (JPQLQuery)q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    protected AbstractClassMetaData getCandidateClassMetaData()
    {
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
        if (candidateClass.isInterface())
        {
            // Query of interface
            String[] impls = ec.getMetaDataManager().getClassesImplementingInterface(candidateClass.getName(), clr);
            if (impls.length == 1 && cmd.isImplementationOfPersistentDefinition())
            {
                // Only the generated implementation, so just use its metadata
            }
            else
            {
                // Use metadata for the persistent interface
                cmd = ec.getMetaDataManager().getMetaDataForInterface(candidateClass, clr);
                if (cmd == null)
                {
                    throw new NucleusUserException("Attempting to query an interface yet it is not declared 'persistent'." +
                        " Define the interface in metadata as being persistent to perform this operation, and make sure" +
                        " any implementations use the same identity and identity member(s)");
                }
            }
        }

        return cmd;
    }

    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        try
        {
            Session session = (Session) mconn.getConnection();

            long startTime = 0;
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                startTime = System.currentTimeMillis();
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JPQL", getSingleStringQuery(), null));
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

            // Map any result restrictions onto the worksheet results
            JavaQueryEvaluator resultMapper = new JPQLEvaluator(this, candidates, compilation, 
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == BULK_UPDATE)
            {
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
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
            // Obtain candidate objects for this class
            StringBuilder stmtBuilder = new StringBuilder("SELECT * FROM ");
            stmtBuilder.append(storeMgr.getSchemaNameForClass(cmd)).append('.').append(storeMgr.getNamingFactory().getTableName(cmd));
            // TODO Add discriminator restriction if table is being shared (when we support table sharing)

            // Execute the SELECT
            NucleusLogger.QUERY.debug("Obtaining query candidates of type " + cmd.getFullClassName() + " using : " + stmtBuilder.toString());
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