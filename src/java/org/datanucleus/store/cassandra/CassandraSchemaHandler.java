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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Handler for schema management with Cassandra.
 */
public class CassandraSchemaHandler
{
    CassandraStoreManager storeMgr;

    public CassandraSchemaHandler(CassandraStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
    }

    /**
     * Method to create a schema (keyspace) in Cassandra.
     * Accepts properties with names "replication", "durable_writes" (case sensitive).
     * @param schemaName Name of the schema
     * @param props Any properties defining the new keyspace
     */
    public void createSchema(String schemaName, Properties props)
    {
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ");
            stmtBuilder.append(schemaName).append(" WITH ");
            String replicationProp = (props != null ? (String)props.get("replication") : "{'class': 'SimpleStrategy', 'replication_factor' : 3}");
            stmtBuilder.append("replication = ").append(replicationProp);
            if (props != null && props.containsKey("durable_writes"))
            {
                Boolean durable = Boolean.valueOf((String)props.get("durable_writes"));
                if (!durable)
                {
                    stmtBuilder.append(" AND durable_writes=false");
                }
            }

            NucleusLogger.DATASTORE_SCHEMA.debug(stmtBuilder.toString());
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug("Schema " + schemaName + " created successfully");
        }
        finally
        {
            mconn.release();
        }
    }

    public void createSchema(Set<String> classNames, Properties props)
    {
//        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
//        String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
//        boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));
        // TODO Make use of DDL properties - see RDBMSStoreManager.createSchema

        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
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
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        String schemaNameForClass = storeMgr.getSchemaNameForClass(cmd); // Check existence using "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
        String tableName = namingFactory.getTableName(cmd);

        boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);

        if (storeMgr.isAutoCreateTables() && !tableExists)
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
            int[] memberPositions = cmd.getAllMemberPositions();
            for (int i=0;i<memberPositions.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
                if (cassandraType == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
                }
                else
                {
                    if (!firstCol)
                    {
                        stmtBuilder.append(',');
                    }
                    // TODO Cater for embedded fields
                    stmtBuilder.append(namingFactory.getColumnName(mmd, ColumnType.COLUMN)).append(' ').append(cassandraType);
                }
                if (i == 0)
                {
                    firstCol = false;
                }
            }

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                String colName = namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
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
                    stmtBuilder.append(namingFactory.getColumnName(pkMmd, ColumnType.COLUMN));
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
        else if (tableExists && storeMgr.isAutoCreateColumns())
        {
            // TODO Add ability to add/delete columns to match the current definition
            // ALTER TABLE schema.table ADD {colname} {typeName}
            // ALTER TABLE schema.table DROP {colName} - Note that this really ought to have a persistence property, and make sure there are no classes sharing the table that need it
        }

        if (storeMgr.isAutoCreateConstraints())
        {
            if (!tableExists)
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Cannot create constraints for " + cmd.getFullClassName() +
                    " since table doesn't exist : perhaps you should enable autoCreateTables ?");
                return;
            }

            // TODO Check existence of indexes before creating

            // Add class-level indexes TODO What about superclass indexMetaData?
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
                        String idxName = (idxmd.getName() != null ? idxmd.getName() : namingFactory.getIndexName(cmd, idxmd, i));
                        createIndex(session, idxName, schemaNameForClass, tableName, colmds[0].getName());
                    }
                }
            }

            // Add member-level indexes
            int[] memberPositions = cmd.getAllMemberPositions();
            for (int i=0;i<memberPositions.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                IndexMetaData idxmd = mmd.getIndexMetaData();
                if (idxmd != null)
                {
                    String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                    String idxName = (idxmd.getName() != null ? idxmd.getName() : namingFactory.getIndexName(mmd, idxmd));
                    createIndex(session, idxName, schemaNameForClass, tableName, colName);
                }
            }

            // TODO Index on version column? or discriminator?, or datastoreId?
        }
    }

    /**
     * Method to drop a schema (keyspace) in Cassandra.
     * @param schemaName Name of the schema (keyspace).
     */
    public void deleteSchema(String schemaName)
    {
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("DROP KEYSPACE IF EXISTS ");
            stmtBuilder.append(schemaName);

            NucleusLogger.DATASTORE_SCHEMA.debug(stmtBuilder.toString());
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug("Schema " + schemaName + " dropped successfully");
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteSchema(Set<String> classNames, Properties props)
    {
//      String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
//      String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
//      boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));
      // TODO Make use of DDL properties - see RDBMSStoreManager.createSchema

        // TODO Add deletion of any "incrementtable" if used

        NamingFactory namingFactory = storeMgr.getNamingFactory();
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    String schemaNameForClass = storeMgr.getSchemaNameForClass(cmd); // Check existence using "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
                    String tableName = namingFactory.getTableName(cmd);
                    boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);
                    if (tableExists)
                    {
                        // Drop any class indexes TODO What about superclass indexMetaData?
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
                                    idxName = namingFactory.getIndexName(cmd, idxmd, i);
                                }
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                                session.execute(stmtBuilder.toString());
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                            }
                        }
                        // Drop any member-level indexes
                        int[] memberPositions = cmd.getAllMemberPositions();
                        for (int i=0;i<memberPositions.length;i++)
                        {
                            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                            IndexMetaData idxmd = mmd.getIndexMetaData();
                            if (idxmd != null)
                            {
                                StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                String idxName = idxmd.getName();
                                if (idxName == null)
                                {
                                    idxName = namingFactory.getIndexName(mmd, idxmd);
                                }
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                                session.execute(stmtBuilder.toString());
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                            }
                        }

                        // Drop the table
                        StringBuilder stmtBuilder = new StringBuilder("DROP TABLE ");
                        if (schemaNameForClass != null)
                        {
                            stmtBuilder.append(schemaNameForClass).append('.');
                        }
                        stmtBuilder.append(tableName);
                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropping table : " + stmtBuilder.toString());
                        session.execute(stmtBuilder.toString());
                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropped table for class " + cmd.getFullClassName() + " successfully");
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug("Class " + cmd.getFullClassName() + " table=" + tableName + " didnt exist so can't be dropped");
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void validateSchema(Set<String> classNames, Properties props)
    {
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        boolean success = true;
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            for (String className : classNames)
            {
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);

                String schemaNameForClass = storeMgr.getSchemaNameForClass(cmd);
                String tableName = namingFactory.getTableName(cmd);

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
                    Set<String> colsFound = new HashSet();
                    int[] memberPositions = cmd.getAllMemberPositions();
                    for (int i=0;i<memberPositions.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                        String columnName = namingFactory.getColumnName(mmd, ColumnType.COLUMN); // TODO Cater for embedded fields
                        ColumnDetails details = colsByName.get(columnName.toLowerCase()); // Stored in lowercase (unless we later on start quoting column names)
                        if (details != null)
                        {
                            String reqdType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
                            if ((reqdType != null && reqdType.equals(details.typeName)) || (reqdType == null && details.typeName == null))
                            {
                                // Type matches
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column " + columnName + " has type=" + details.typeName + " yet member type " + mmd.getFullFieldName() +
                                    " ought to be using type=" + reqdType);
                            }

                            colsFound.add(columnName.toLowerCase());
                        }
                        else
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " doesn't have column " + columnName + " for member " + mmd.getFullFieldName());
                            success = false;
                        }
                    }
                    // TODO Check datastore id, version, discriminator
                    if (success && colsByName.size() != colsFound.size())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " should have " + colsFound.size() + " columns but has " + colsByName.size() + " columns!");
                        success = false;
                    }

                    // Check class-level indexes TODO What about superclass indexMetaData?
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
                    for (int i=0;i<memberPositions.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                        IndexMetaData idxmd = mmd.getIndexMetaData();
                        if (idxmd != null)
                        {
                            String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
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

    public static boolean checkTableExistence(Session session, String schemaName, String tableName)
    {
        StringBuilder stmtBuilder = new StringBuilder("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug("Checking existence of table " + tableName + " using : " + stmtBuilder.toString());
        PreparedStatement stmt = session.prepare(stmtBuilder.toString());
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase(), tableName.toLowerCase()));
        if (!rs.isExhausted())
        {
            return true;
        }
        return false;
    }

    public Map<String, ColumnDetails> getColumnDetailsForTable(Session session, String schemaName, String tableName)
    {
        StringBuilder stmtBuilder = new StringBuilder("SELECT column_name, index_name, validator FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug("Checking structure of table " + tableName + " using : " + stmtBuilder.toString());
        PreparedStatement stmt = session.prepare(stmtBuilder.toString());
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase(), tableName.toLowerCase()));
        Map<String, ColumnDetails> cols = new HashMap<String, ColumnDetails>();
        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext())
        {
            Row row = iter.next();
            String typeName = null;
            String validator = row.getString("validator");
            if (validator.indexOf("LongType") >= 0)
            {
                typeName = "bigint";
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
