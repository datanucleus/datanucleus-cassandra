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
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.QueryManager;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * JPQL query for Cassandra.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    /** The compilation of the query for this datastore. Not applicable if totally in-memory. */
    protected transient CassandraQueryCompilation datastoreCompilation;

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

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        if (evaluateInMemory())
        {
            // Don't need datastore compilation here since evaluating in-memory
            return compilation != null;
        }
        else
        {
            // Need both to be present to say "compiled"
            if (compilation == null || datastoreCompilation == null)
            {
                return false;
            }
            if (!datastoreCompilation.isPrecompilable())
            {
                NucleusLogger.GENERAL.info("Query compiled but not precompilable so ditching datastore compilation");
                datastoreCompilation = null;
                return false;
            }
            return true;
        }
    }

    /**
     * Convenience method to return whether the query should be evaluated in-memory.
     * @return Use in-memory evaluation?
     */
    protected boolean evaluateInMemory()
    {
        if (candidateCollection != null)
        {
            if (compilation != null && compilation.getSubqueryAliases() != null)
            {
                // TODO In-memory evaluation of subqueries isn't fully implemented yet, so remove this when it is
                NucleusLogger.QUERY.warn("In-memory evaluator doesn't currently handle subqueries completely so evaluating in datastore");
                return false;
            }

            Object val = getExtension(EXTENSION_EVALUATE_IN_MEMORY);
            if (val == null)
            {
                return true;
            }
            return Boolean.valueOf((String)val);
        }
        return super.evaluateInMemory();
    }

    /**
     * Method to compile the JDOQL query.
     * Uses the superclass to compile the generic query populating the "compilation", and then generates
     * the datastore-specific "datastoreCompilation".
     * @param parameterValues Map of param values keyed by param name (if available at compile time)
     */
    protected synchronized void compileInternal(Map parameterValues)
    {
        if (isCompiled())
        {
            return;
        }

        // Compile the generic query expressions
        super.compileInternal(parameterValues);

        boolean inMemory = evaluateInMemory();
        if (candidateCollection != null && inMemory)
        {
            // Querying a candidate collection in-memory, so just return now (don't need datastore compilation)
            // TODO Maybe apply the result class checks ?
            return;
        }

        if (candidateClass == null)
        {
            throw new NucleusUserException(Localiser.msg("021009", candidateClassName));
        }

        // Make sure any persistence info is loaded
        ec.hasPersistenceInformationForClass(candidateClass);

        QueryManager qm = getQueryManager();
        String datastoreKey = getStoreManager().getQueryCacheKey();
        String cacheKey = getQueryCacheKey();
        if (useCaching())
        {
            // Allowing caching so try to find compiled (datastore) query
            datastoreCompilation = (CassandraQueryCompilation)qm.getDatastoreQueryCompilation(datastoreKey, getLanguage(), cacheKey);
            if (datastoreCompilation != null)
            {
                // Cached compilation exists for this datastore so reuse it
                setResultDistinct(compilation.getResultDistinct());
                return;
            }
        }

        datastoreCompilation = new CassandraQueryCompilation();
        synchronized (datastoreCompilation)
        {
            if (inMemory)
            {
                // Generate statement to just retrieve all candidate objects for later processing
            }
            else
            {
                // Try to generate statement to perform the full query in the datastore
                compileQueryFull(parameterValues);
            }
        }

        if (cacheKey != null)
        {
            if (datastoreCompilation.isPrecompilable())
            {
                qm.addDatastoreQueryCompilation(datastoreKey, getLanguage(), cacheKey, datastoreCompilation);
            }
        }
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
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "JPQL", getSingleStringQuery(), null));
            }

            boolean filterInMemory = (filter != null);
            Boolean orderInMemory = (ordering != null);
            Boolean rangeInMemory = (range != null);
            List candidates = null;
            if (candidateCollection != null)
            {
                candidates = new ArrayList(candidateCollection);
            }
            else if (evaluateInMemory())
            {
                candidates = getCandidatesForQuery(session);
            }
            else
            {
                if (filter != null && datastoreCompilation.isFilterComplete())
                {
                    candidates = new ArrayList();
                    Set<String> classNamesQueryable = datastoreCompilation.getClassNames();
                    for (String className : classNamesQueryable)
                    {
                        String cql = datastoreCompilation.getCQLForClass(className);
                        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(className, clr);

                        // Execute the SELECT
                        CassandraUtils.logCqlStatement(cql, null, NucleusLogger.DATASTORE_NATIVE);
                        ResultSet rs = session.execute(cql);

                        // Extract the candidates from the ResultSet
                        Iterator<Row> iter = rs.iterator();
                        while (iter.hasNext())
                        {
                            Row row = iter.next();
                            candidates.add(CassandraUtils.getPojoForRowForCandidate(row, cmd, ec, getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers(), getIgnoreCache()));
                        }
                    }
                    filterInMemory = false;
                }
                else
                {
                    // Filter not evaluatable in datastore so get candidates
                    candidates = getCandidatesForQuery(session);
                }
            }

            Collection results = candidates;
            if (filterInMemory || result != null || resultClass != null || rangeInMemory || orderInMemory)
            {
                if (candidates instanceof QueryResult)
                {
                    // Make sure the cursor(s) are all loaded
                    ((QueryResult)candidates).disconnect();
                }

                JavaQueryEvaluator resultMapper = new JPQLEvaluator(this, candidates, compilation, parameters, ec.getClassLoaderResolver());
                results = resultMapper.execute(filterInMemory, orderInMemory, result != null, resultClass != null, rangeInMemory);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", "JDOQL", "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == BULK_UPDATE)
            {
                // TODO Support BULK UPDATE
                throw new NucleusException("Bulk Update is not yet supported");
            }

            if (results instanceof QueryResult)
            {
                final QueryResult qr1 = (QueryResult)results;
                final ManagedConnection mconn1 = mconn;
                ManagedConnectionResourceListener listener = new ManagedConnectionResourceListener()
                {
                    public void transactionFlushed(){}
                    public void transactionPreClose()
                    {
                        // Tx : disconnect query from ManagedConnection (read in unread rows etc)
                        qr1.disconnect();
                    }
                    public void managedConnectionPreClose()
                    {
                        if (!ec.getTransaction().isActive())
                        {
                            // Non-Tx : disconnect query from ManagedConnection (read in unread rows etc)
                            qr1.disconnect();
                        }
                    }
                    public void managedConnectionPostClose(){}
                    public void resourcePostClose()
                    {
                        mconn1.removeListener(this);
                    }
                };
                mconn.addListener(listener);
                if (qr1 instanceof AbstractQueryResult)
                {
                    ((AbstractQueryResult)qr1).addConnectionListener(listener);
                }
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
            if (cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE || cmd.isEmbeddedOnly())
            {
                continue;
            }
            else if (cmd instanceof ClassMetaData && ((ClassMetaData)cmd).isAbstract())
            {
                continue;
            }

            // TODO Remove this and when class is registered, use listener to manage it
            storeMgr.manageClasses(clr, cmd.getFullClassName());

            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            if (table == null)
            {
                continue;
            }

            // Obtain candidate objects for this class
            StringBuilder stmtBuilder = new StringBuilder("SELECT * FROM ");
            stmtBuilder.append(table.getSchemaName()).append('.').append(table.getName());
            // TODO Add discriminator restriction if table is being shared (when we support table sharing)

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                String multitenancyValue = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID);
                stmtBuilder.append(" WHERE ").append(table.getMultitenancyColumn().getName()).append("='").append(multitenancyValue).append("'");
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


    /**
     * Method to compile the query for the datastore attempting to evaluate the whole query in the datastore
     * if possible. Sets the components of the "datastoreCompilation".
     * @param parameters Input parameters (if known)
     * @param candidateCmd Metadata for the candidate class
     */
    private void compileQueryFull(Map parameters)
    {
        long startTime = 0;
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
            NucleusLogger.QUERY.debug(Localiser.msg("021083", getLanguage(), toString()));
        }

        List<AbstractClassMetaData> cmds = MetaDataUtils.getMetaDataForCandidates(getCandidateClass(), isSubclasses(), ec);
        for (AbstractClassMetaData cmd : cmds)
        {
            if (cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE || cmd.isEmbeddedOnly())
            {
                continue;
            }
            else if (cmd instanceof ClassMetaData && ((ClassMetaData)cmd).isAbstract())
            {
                continue;
            }

            // TODO Remove this and when class is registered, use listener to manage it
            storeMgr.manageClasses(clr, cmd.getFullClassName());

            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            if (table == null)
            {
                continue;
            }

            QueryToCQLMapper mapper = new QueryToCQLMapper(compilation, parameters, cmd, ec, this, table);
            mapper.compile();
            datastoreCompilation.setFilterComplete(mapper.isFilterComplete());
            datastoreCompilation.setResultComplete(mapper.isResultComplete());
            datastoreCompilation.setOrderComplete(mapper.isOrderComplete());
            datastoreCompilation.setCQLForClass(cmd.getFullClassName(), mapper.getCQL());
            datastoreCompilation.setPrecompilable(mapper.isPrecompilable());
        }

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("021084", getLanguage(), System.currentTimeMillis()-startTime));
        }
    }
}