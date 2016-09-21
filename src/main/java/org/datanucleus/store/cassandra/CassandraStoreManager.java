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

import java.awt.image.BufferedImage;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.NamingCase;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    public static final String PROPERTY_CASSANDRA_ENFORCE_UNIQUENESS_IN_APPLICATION = "datanucleus.cassandra.enforceUniquenessInApplication";

    /** Comma separated USING clause for INSERTS. */
    public static final String EXTENSION_CASSANDRA_INSERT_USING = "cassandra.insert.using"; // TODO Make this "datanucleus."
    /** Comma separated USING clause for UPDATES. */
    public static final String EXTENSION_CASSANDRA_UPDATE_USING = "cassandra.update.using"; // TODO Make this "datanucleus."
    /** Comma separated USING clause for DELETES. */
    public static final String EXTENSION_CASSANDRA_DELETE_USING = "cassandra.delete.using"; // TODO Make this "datanucleus."

    static
    {
        Localiser.registerBundle("org.datanucleus.store.cassandra.Localisation", CassandraStoreManager.class.getClassLoader());
    }

    SessionStatementProvider stmtProvider = new SessionStatementProvider();

    public static final String RESERVED_WORDS = "ADD,ALLOW,ALTER,AND,ANY,APPLY,ASC,AUTHORIZE,BATCH,BEGIN,BY,COLUMNFAMILY,CREATE,DELETE,DESC,DROP," + 
        "FROM,GRANT,IN,INDEX,INET,INSERT,INTO,KEYSPACE,KEYSPACES,LIMIT,MODIFY,NORECURSIVE,OF,ON,ONE,ORDER,PASSWORD,PRIMARY,QUORUM,RENAME,REVOKE," + 
        "SCHEMA,SELECT,SET,TABLE,TO,TOKEN,THREE,TRUNCATE,TWO,UNLOGGED,UPDATE,USE,USING,WHERE,WITH";

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public CassandraStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusCtx, Map<String, Object> props)
    {
        super("cassandra", clr, nucleusCtx, props);

        // Override BufferedReader default converter to use ByteBuffer
        nucleusCtx.getTypeManager().setDefaultTypeConverterForType(BufferedImage.class, "dn.bufferedimage-bytebuffer");

        // Set up naming factory to match Cassandra capabilities
        getNamingFactory().setReservedKeywords(StringUtils.convertCommaSeparatedStringToSet(RESERVED_WORDS));
        String namingCase = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_CASE);
        if (!StringUtils.isWhitespace(namingCase))
        {
            if (namingCase.equalsIgnoreCase("UPPERCASE"))
            {
                getNamingFactory().setNamingCase(NamingCase.UPPER_CASE_QUOTED);
            }
            else if (namingCase.equalsIgnoreCase("lowercase"))
            {
                getNamingFactory().setNamingCase(NamingCase.LOWER_CASE);
            }
            else
            {
                getNamingFactory().setNamingCase(NamingCase.MIXED_CASE_QUOTED);
            }
        }
        else
        {
            // Default to lowercase
            getNamingFactory().setNamingCase(NamingCase.LOWER_CASE);
        }

        schemaHandler = new CassandraSchemaHandler(this);
        persistenceHandler = new CassandraPersistenceHandler(this);

        logConfiguration();
    }

    public Collection<String> getSupportedOptions()
    {
        Set<String> set = new HashSet<>();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_ORM_SERIALISED_PC);
        set.add(StoreManager.OPTION_DATASTORE_TIME_STORES_MILLISECS);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
//        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_UPDATE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
//        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_UPDATE);

        set.add(StoreManager.OPTION_ORM_INHERITANCE_COMPLETE_TABLE);

        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getNativeQueryLanguage()
     */
    @Override
    public String getNativeQueryLanguage()
    {
        return "CQL";
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
            Session session = (Session) mconn.getConnection();
            manageClasses(classNames, clr, session);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, Session session)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData) iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isEmbeddedOnly() && !cmd.isAbstract())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, new SchemaVerifierImpl(this, cmd, clr));
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }

        // Create schema for classes
        schemaHandler.createSchemaForClasses(clsNameSet, null, session);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object, org.datanucleus.ClassLoaderResolver, org.datanucleus.ExecutionContext)
     */
    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        if (id == null)
        {
            // User stupidity
            return null;
        }
        else if (id instanceof SCOID)
        {
            // Object is a SCOID
            return ((SCOID) id).getSCOClass();
        }

        // Find overall root class possible for this id
        String rootClassName = super.getClassNameForObjectID(id, clr, ec);
        if (rootClassName != null)
        {
            // User could have passed in a superclass of the real class, so consult the datastore for the precise table/class
            String[] subclasses = getMetaDataManager().getSubclassesForClass(rootClassName, true);
            if (subclasses == null || subclasses.length == 0)
            {
                // No subclasses so no need to go to the datastore
                return rootClassName;
            }

            // TODO Check the datastore for this id
            return rootClassName;
        }
        return null;
    }

    public void createDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.createDatabase(catalogName, schemaName, props, null);
    }

    public void deleteDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.deleteDatabase(catalogName, schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.createSchemaForClasses(classNames, props, null);
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