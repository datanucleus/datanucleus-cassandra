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
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.Session;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public CassandraStoreManager(ClassLoaderResolver clr, NucleusContext nucleusContext, Map<String, Object> props)
    {
        super("cassandra", clr, nucleusContext, props);

        // Handler for persistence process
        persistenceHandler = new CassandraPersistenceHandler(this);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("ORM");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
    }

    public void addClasses(String[] classNames, ClassLoaderResolver clr)
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
        String[] filteredClassNames = 
            getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

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
                        registerStoreData(newStoreData(cmd, clr));
                    }

                    // Create schema for class
                    createSchemaForClass(cmd, session);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#createSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void createSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    createSchemaForClass(cmd, session);
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    protected void createSchemaForClass(AbstractClassMetaData cmd, Session session)
    {
        // TODO Does the keyspace exist? if not then create it "CREATE KEYSPACE schemaName WITH replication ..."
        if (autoCreateTables)
        {
            // TODO Create the table(s) required for this class
//            String tableName = getNamingFactory().getTableName(cmd);
            // CREATE TABLE keyspace.tblName (col1 type1, col2 type2, ...)
        }
        if (autoCreateConstraints)
        {
            // TODO Create the constraint(s) required for this class
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    String tableName = getNamingFactory().getTableName(cmd);
                    // TODO Prefix with keyspace (schemaName)
                    String stmt = "DROP TABLE " + tableName;
                    NucleusLogger.DATASTORE_SCHEMA.debug("Dropping table : " + stmt);
                    session.execute(stmt);
                }
            }

            // TODO Drop the keyspace?
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props)
    {
        boolean success = true;
        ManagedConnection mconn = getConnection(-1);
        try
        {
//            Session session = (Session)mconn.getConnection();

            // TODO Implement validaton of schema
        }
        finally
        {
            mconn.release();
        }

        if (!success)
        {
            throw new NucleusException("Errors were encountered during validation of MongoDB schema");
        }
    }
}