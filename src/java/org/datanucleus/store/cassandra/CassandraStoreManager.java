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
import org.datanucleus.PropertyNames;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.NamingCase;

import com.datastax.driver.core.Session;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    CassandraSchemaHandler schemaHandler;

    String schemaName = null;

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public CassandraStoreManager(ClassLoaderResolver clr, NucleusContext nucleusContext, Map<String, Object> props)
    {
        super("cassandra", clr, nucleusContext, props);

        // Handler for schema
        schemaHandler = new CassandraSchemaHandler(this);

        // Handler for persistence process
        persistenceHandler = new CassandraPersistenceHandler(this);

        // TODO Support quoted names
        getNamingFactory().setNamingCase(NamingCase.LOWER_CASE);

        schemaName = getStringProperty(PropertyNames.PROPERTY_MAPPING_SCHEMA);

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

    public String getSchemaNameForClass(AbstractClassMetaData cmd)
    {
        if (cmd.getSchema() != null)
        {
            return cmd.getSchema();
        }
        else if (schemaName != null)
        {
           return schemaName;
        }
        return null;
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
                    schemaHandler.createSchemaForClass(cmd, session, clr);
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
        schemaHandler.createSchema(classNames, props);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        schemaHandler.deleteSchema(classNames, props);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props)
    {
        schemaHandler.validateSchema(classNames, props);
    }
}