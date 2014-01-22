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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingCase;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
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

        // Handler for persistence process
        persistenceHandler = new CassandraPersistenceHandler(this);

        getNamingFactory().setNamingCase(NamingCase.UPPER_CASE);

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
                    createSchemaForClass(cmd, session, clr);
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
//        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
//        String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
//        boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));
        // TODO Make use of DDL properties - see RDBMSStoreManager.createSchema

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
                    createSchemaForClass(cmd, session, clr);
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    protected void createSchemaForClass(AbstractClassMetaData cmd, Session session, ClassLoaderResolver clr)
    {
        String schemaNameForClass = getSchemaNameForClass(cmd); // Check existence using "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
        String tableName = getNamingFactory().getTableName(cmd);

        boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);

        if (autoCreateTables && !tableExists) // TODO Add ability to add/delete columns to match the current definition
        {
            // Create the table(s) required for this class
            // CREATE TABLE keyspace.tblName (col1 type1, col2 type2, ...)
            StringBuilder stmtBuilder = new StringBuilder("CREATE TABLE "); // Note that we could do "IF NOT EXISTS" but have the existence checker method for validation so use that
            if (schemaNameForClass != null)
            {
                stmtBuilder.append(schemaNameForClass).append('.');
            }
            stmtBuilder.append(tableName);
            stmtBuilder.append(" (");
            boolean firstCol = true;
            AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
            for (int i=0;i<mmds.length;i++)
            {
                String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmds[i], nucleusContext.getTypeManager(), clr);
                if (cassandraType == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmds[i].getFullFieldName() + " of type "+ mmds[i].getTypeName() + " has no supported cassandra type! Ignoring");
                }
                else
                {
                    if (!firstCol)
                    {
                        stmtBuilder.append(',');
                    }
                    stmtBuilder.append(getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN)).append(' ').append(cassandraType);
                }
                if (i == 0)
                {
                    firstCol = false;
                }
            }
            // Add columns for superclasses
            AbstractClassMetaData superCmd = cmd.getSuperAbstractClassMetaData();
            while (superCmd != null)
            {
                mmds = superCmd.getManagedMembers();
                for (int i=0;i<mmds.length;i++)
                {
                    String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmds[i], nucleusContext.getTypeManager(), clr);
                    if (cassandraType == null)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmds[i].getFullFieldName() + " of type "+ mmds[i].getTypeName() + " has no supported cassandra type! Ignoring");
                    }
                    else
                    {
                        if (!firstCol)
                        {
                            stmtBuilder.append(',');
                        }
                        stmtBuilder.append(getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN)).append(' ').append(cassandraType);
                    }
                    if (i == 0)
                    {
                        firstCol = false;
                    }
                }

                superCmd = superCmd.getSuperAbstractClassMetaData();
            }

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                String colName = getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
                String colType = "bigint"; // TODO Set the type based on jdbc-type of the datastore-id metadata : uuid?, varchar?
                stmtBuilder.append(colName).append(" ").append(colType);

                stmtBuilder.append(",PRIMARY KEY (").append(colName).append(")");
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append("PRIMARY KEY (");
                int[] pkPositions = cmd.getPKMemberPositions();
                for (int i=0;i<pkPositions.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(',');
                    }
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                    stmtBuilder.append(getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN));
                }
                stmtBuilder.append(")");
            }

            stmtBuilder.append(')');
            // TODO Add support for "WITH option1=val1 AND option2=val2 ..." by using extensions part of metadata

            NucleusLogger.DATASTORE_SCHEMA.debug("Creating table : " + stmtBuilder.toString());
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug("Created table for class " + cmd.getFullClassName() + " successfully");
            tableExists = true;
        }

        if (autoCreateConstraints)
        {
            if (!tableExists)
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Cannot create constraints for " + cmd.getFullClassName() +
                    " since table doesn't exist : perhaps you should enable autoCreateTables ?");
                return;
            }

            // TODO Cater for superclasses with indexes on their members
            // TODO Check existence of indexes before creating

            // Add class-level indexes
            IndexMetaData[] clsIdxMds = cmd.getIndexMetaData();
            if (clsIdxMds != null)
            {
                for (int i=0;i<clsIdxMds.length;i++)
                {
                    IndexMetaData idxmd = clsIdxMds[i];
                    ColumnMetaData[] colmds = idxmd.getColumnMetaData();
                    if (colmds.length > 1)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.warn("Class " + cmd.getFullClassName() + " has an index defined with more than 1 column. Cassandra doesn't support composite indexes so ignoring");
                    }
                    else
                    {
                        String idxName = (idxmd.getName() != null ? idxmd.getName() : getNamingFactory().getIndexName(cmd, idxmd, i));
                        createIndex(session, idxName, schemaNameForClass, tableName, colmds[0].getName());
                    }
                }
            }

            // Add member-level indexes
            AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
            for (int i=0;i<mmds.length;i++)
            {
                IndexMetaData idxmd = mmds[i].getIndexMetaData();
                if (idxmd != null)
                {
                    String colName = getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN);
                    String idxName = (idxmd.getName() != null ? idxmd.getName() : getNamingFactory().getIndexName(mmds[i], idxmd));
                    createIndex(session, idxName, schemaNameForClass, tableName, colName);
                }
            }
            // TODO Index on version column? or discriminator?, or datastoreId?

            // Cassandra doesn't support unique constraints or FKs at the moment
        }
        NucleusLogger.GENERAL.info(">> createSchemaForClass DONE for " + cmd.getFullClassName());
    }

    protected void createIndex(Session session, String indexName, String schemaName, String tableName, String columnName)
    {
        StringBuilder stmtBuilder = new StringBuilder("CREATE INDEX ");
        stmtBuilder.append(indexName);
        stmtBuilder.append(" ON ");
        if (schemaName != null)
        {
            stmtBuilder.append(schemaName).append('.');
        }
        stmtBuilder.append(tableName);
        stmtBuilder.append(" (").append(columnName).append(")");

        NucleusLogger.DATASTORE_SCHEMA.debug("Creating index : " + stmtBuilder.toString());
        session.execute(stmtBuilder.toString());
        NucleusLogger.DATASTORE_SCHEMA.debug("Created index " + indexName + " for table " + tableName + " successfully");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void deleteSchema(Set<String> classNames, Properties props)
    {
//      String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
//      String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
//      boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));
      // TODO Make use of DDL properties - see RDBMSStoreManager.createSchema

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
                    // Drop any class indexes
                    IndexMetaData[] clsIdxMds = cmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        for (int i=0;i<clsIdxMds.length;i++)
                        {
                            IndexMetaData idxmd = clsIdxMds[i];
                            StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                            String idxName = idxmd.getName();
                            if (idxName == null)
                            {
                                idxName = getNamingFactory().getIndexName(cmd, idxmd, i);
                            }
                            NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                            session.execute(stmtBuilder.toString());
                            NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                        }
                    }
                    // Drop any member-level indexes
                    AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
                    for (int i=0;i<mmds.length;i++)
                    {
                        IndexMetaData idxmd = mmds[i].getIndexMetaData();
                        if (idxmd != null)
                        {
                            StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                            String idxName = idxmd.getName();
                            if (idxName == null)
                            {
                                idxName = getNamingFactory().getIndexName(mmds[i], idxmd);
                            }
                            NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                            session.execute(stmtBuilder.toString());
                            NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                        }
                    }

                    // Drop the table
                    String tableName = getNamingFactory().getTableName(cmd);
                    StringBuilder stmtBuilder = new StringBuilder("DROP TABLE ");
                    String schemaNameForClass = getSchemaNameForClass(cmd);
                    if (schemaNameForClass != null)
                    {
                        stmtBuilder.append(schemaNameForClass).append('.');
                    }
                    stmtBuilder.append(tableName);
                    NucleusLogger.DATASTORE_SCHEMA.debug("Dropping table : " + stmtBuilder.toString());
                    session.execute(stmtBuilder.toString());
                    NucleusLogger.DATASTORE_SCHEMA.debug("Dropped table for class " + cmd.getFullClassName() + " successfully");
                }
            }
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
        ClassLoaderResolver clr = getNucleusContext().getClassLoaderResolver(null);
        ManagedConnection mconn = getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            for (String className : classNames)
            {
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);

                String schemaNameForClass = getSchemaNameForClass(cmd);
                String tableName = getNamingFactory().getTableName(cmd);

                boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);
                if (!tableExists)
                {
                    NucleusLogger.DATASTORE_SCHEMA.error("Table for class " + cmd.getFullClassName() + " doesn't exist : should have name " + tableName + " in schema " + schemaNameForClass);
                    success = false;
                }
                else
                {
                    // Check structure of the table against the required members
                    Map<String, ColumnDetails> colsByName = getColumnDetailsForTable(session, schemaNameForClass, tableName);
                    AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
                    Set<String> colsFound = new HashSet();
                    for (int i=0;i<mmds.length;i++)
                    {
                        String columnName = getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN);
                        ColumnDetails details = colsByName.get(columnName.toLowerCase()); // Stored in lowercase (unless we later on start quoting column names)
                        if (details != null)
                        {
                            String reqdType = CassandraUtils.getCassandraColumnTypeForMember(mmds[i], getNucleusContext().getTypeManager(), clr);
                            if ((reqdType != null && reqdType.equals(details.typeName)) || (reqdType == null && details.typeName == null))
                            {
                                // Type matches
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column " + columnName + " has type=" + details.typeName + " yet member type " + mmds[i].getFullFieldName() +
                                    " ought to be using type=" + reqdType);
                            }

                            colsFound.add(columnName.toLowerCase());
                        }
                        else
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " doesn't have column " + columnName + " for member " + mmds[i].getFullFieldName());
                            success = false;
                        }
                    }
                    // TODO Check datastore id, version, discriminator
                    if (success && colsByName.size() != colsFound.size())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " should have " + colsFound.size() + " columns but has " + colsByName.size() + " columns!");
                        success = false;
                    }

                    // Check class-level indexes
                    IndexMetaData[] clsIdxMds = cmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        for (int i=0;i<clsIdxMds.length;i++)
                        {
                            IndexMetaData idxmd = clsIdxMds[i];
                            ColumnMetaData[] colmds = idxmd.getColumnMetaData();
                            if (colmds.length == 1)
                            {
                                String colName = colmds[0].getName();
                                ColumnDetails details = colsByName.get(colName.toLowerCase());
                                if (details == null || details.indexName == null)
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column=" + colName + " should have an index but doesn't");
                                }
                            }
                        }
                    }

                    // Add member-level indexes
                    for (int i=0;i<mmds.length;i++)
                    {
                        IndexMetaData idxmd = mmds[i].getIndexMetaData();
                        if (idxmd != null)
                        {
                            String colName = getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN);
                            ColumnDetails details = colsByName.get(colName.toLowerCase());
                            if (details == null || details.indexName == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column=" + colName + " should have an index but doesn't");
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }

        if (!success)
        {
            throw new NucleusException("Errors were encountered during validation of Cassandra schema");
        }
    }

    protected boolean checkTableExistence(Session session, String schemaName, String tableName)
    {
        boolean tableExists = false;
        StringBuilder stmtBuilder = new StringBuilder("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name='");
        stmtBuilder.append(schemaName).append("' AND columnfamily_name='").append(tableName).append("'");
        ResultSet rs = session.execute(stmtBuilder.toString());
        if (!rs.isExhausted())
        {
            tableExists = true;
        }

        return tableExists;
    }

    public Map<String, ColumnDetails> getColumnDetailsForTable(Session session, String schemaName, String tableName)
    {
        StringBuilder stmtBuilder = new StringBuilder("SELECT column_name, index_name, validator FROM system.schema_columns WHERE keyspace_name='");
        stmtBuilder.append(schemaName).append("' AND columnfamily_name='").append(tableName).append("'");
        ResultSet rs = session.execute(stmtBuilder.toString());
        Map<String, ColumnDetails> cols = new HashMap<String, CassandraStoreManager.ColumnDetails>();
        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext())
        {
            Row row = iter.next();
            String typeName = null;
            String validator = row.getString("validator");
            if (validator.indexOf("LongType") >= 0)
            {
                typeName = "long";
            }
            else if (validator.indexOf("Int32Type") >= 0)
            {
                typeName = "int";
            }
            else if (validator.indexOf("DoubleType") >= 0)
            {
                typeName = "double";
            }
            else if (validator.indexOf("FloatType") >= 0)
            {
                typeName = "float";
            }
            else if (validator.indexOf("BooleanType") >= 0)
            {
                typeName = "boolean";
            }
            else if (validator.indexOf("UTF8") >= 0)
            {
                typeName = "varchar";
            }
            // TODO Include other types
            String colName = row.getString("column_name");
            ColumnDetails col = new ColumnDetails(colName, row.getString("index_name"), typeName);
            cols.put(colName, col);
        }
        return cols;
    }

    public class ColumnDetails
    {
        String name;
        String indexName;
        String typeName;
        public ColumnDetails(String name, String idxName, String typeName)
        {
            this.name = name;
            this.indexName = idxName;
            this.typeName = typeName;
        }
    }
}