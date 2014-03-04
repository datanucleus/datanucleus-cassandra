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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.NamingCase;
import org.datanucleus.store.schema.table.CompleteClassTable;

import com.datastax.driver.core.Session;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    SessionStatementProvider stmtProvider = new SessionStatementProvider();

    protected final Set<String> reservedWords = new HashSet<String>();

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public CassandraStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusCtx, Map<String, Object> props)
    {
        super("cassandra", clr, nucleusCtx, props);

        reservedWords.add("ADD");
        reservedWords.add("ALLOW");
        reservedWords.add("ALTER");
        reservedWords.add("AND");
        reservedWords.add("ANY");
        reservedWords.add("APPLY");
        reservedWords.add("ASC");
        reservedWords.add("AUTHORIZE");
        reservedWords.add("BATCH");
        reservedWords.add("BEGIN");
        reservedWords.add("BY");
        reservedWords.add("COLUMNFAMILY");
        reservedWords.add("CREATE");
        reservedWords.add("DELETE");
        reservedWords.add("DESC");
        reservedWords.add("DROP");
        reservedWords.add("FROM");
        reservedWords.add("GRANT");
        reservedWords.add("IN");
        reservedWords.add("INDEX");
        reservedWords.add("INET");
        reservedWords.add("INSERT");
        reservedWords.add("INTO");
        reservedWords.add("KEYSPACE");
        reservedWords.add("KEYSPACES");
        reservedWords.add("LIMIT");
        reservedWords.add("MODIFY");
        reservedWords.add("NORECURSIVE");
        reservedWords.add("OF");
        reservedWords.add("ON");
        reservedWords.add("ONE");
        reservedWords.add("ORDER");
        reservedWords.add("PASSWORD");
        reservedWords.add("PRIMARY");
        reservedWords.add("QUORUM");
        reservedWords.add("RENAME");
        reservedWords.add("REVOKE");
        reservedWords.add("SCHEMA");
        reservedWords.add("SELECT");
        reservedWords.add("SET");
        reservedWords.add("TABLE");
        reservedWords.add("TO");
        reservedWords.add("TOKEN");
        reservedWords.add("THREE");
        reservedWords.add("TRUNCATE");
        reservedWords.add("TWO");
        reservedWords.add("UNLOGGED");
        reservedWords.add("UPDATE");
        reservedWords.add("USE");
        reservedWords.add("USING");
        reservedWords.add("WHERE");
        reservedWords.add("WITH");
        getNamingFactory().setReservedKeywords(reservedWords);

        schemaHandler = new CassandraSchemaHandler(this);
        persistenceHandler = new CassandraPersistenceHandler(this);

        // TODO Support quoted names
        getNamingFactory().setNamingCase(NamingCase.LOWER_CASE);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        return set;
    }

    public SessionStatementProvider getStatementProvider()
    {
        return stmtProvider;
    }

    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();
            addClasses(classNames, clr, session);
        }
        finally
        {
            mconn.release();
        }
    }

    public void addClasses(String[] classNames, ClassLoaderResolver clr, Session session)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE)
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, new ColumnAttributerImpl(this, cmd, clr));
                        sd = newStoreData(cmd, clr);
                        sd.addProperty("tableObject", table);
                        registerStoreData(sd);
                    }

                    // Create schema for class
                    Set<String> clsNameSet = new HashSet<String>();
                    clsNameSet.add(cmd.getFullClassName());
                    schemaHandler.createSchemaForClasses(clsNameSet, null, session);
                }
            }
        }
    }

    public void createSchema(String schemaName, Properties props)
    {
        schemaHandler.createSchema(schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.createSchemaForClasses(classNames, props, null);
    }

    public void deleteSchema(String schemaName, Properties props)
    {
        schemaHandler.deleteSchema(schemaName, props, null);
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.deleteSchemaForClasses(classNames, props, null);
    }

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.validateSchema(classNames, props, null);
    }
}