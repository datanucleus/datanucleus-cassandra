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
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.query.CQLQuery;
import org.datanucleus.store.cassandra.query.JDOQLQuery;
import org.datanucleus.store.cassandra.query.JPQLQuery;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.NamingCase;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    public static final String PROPERTY_CASSANDRA_ENFORCE_UNIQUENESS_IN_APPLICATION = "datanucleus.cassandra.enforceUniquenessInApplication".toLowerCase();

    /** Comma separated USING clause for INSERTS. */
    public static final String EXTENSION_CASSANDRA_INSERT_USING = "datanucleus.cassandra.insert.using";
    /** Comma separated USING clause for UPDATES. */
    public static final String EXTENSION_CASSANDRA_UPDATE_USING = "datanucleus.cassandra.update.using";
    /** Comma separated USING clause for DELETES. */
    public static final String EXTENSION_CASSANDRA_DELETE_USING = "datanucleus.cassandra.delete.using";

    public static final String QUERY_LANGUAGE_CQL = "CQL";

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
        nucleusCtx.getTypeManager().setDefaultTypeConverterForType(java.awt.image.BufferedImage.class, "dn.bufferedimage-bytebuffer");

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
     * @see org.datanucleus.store.AbstractStoreManager#getSupportedQueryLanguages()
     */
    @Override
    public Collection<String> getSupportedQueryLanguages()
    {
        Collection<String> languages = super.getSupportedQueryLanguages();
        languages.add(QUERY_LANGUAGE_CQL);
        return languages;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#supportsQueryLanguage(java.lang.String)
     */
    @Override
    public boolean supportsQueryLanguage(String language)
    {
        if (language != null && (language.equals(QueryLanguage.JDOQL.name()) || language.equals(QueryLanguage.JPQL.name()) || language.equals(QUERY_LANGUAGE_CQL)))
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getNativeQueryLanguage()
     */
    @Override
    public String getNativeQueryLanguage()
    {
        return QUERY_LANGUAGE_CQL;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec);
        }
        else if (language.equals(QUERY_LANGUAGE_CQL))
        {
            return new CQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        else if (language.equals(QUERY_LANGUAGE_CQL))
        {
            return new CQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        else if (language.equals(QUERY_LANGUAGE_CQL))
        {
            return new CQLQuery(this, ec, (CQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
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

        ManagedConnection mconn = connectionMgr.getConnection(-1);
        try
        {
            CqlSession session = (CqlSession) mconn.getConnection();
            manageClasses(classNames, clr, session);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, CqlSession session)
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