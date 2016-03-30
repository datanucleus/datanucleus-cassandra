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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SchemaVerifier;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Implementation of a schema verifier for Cassandra. This class provides a way for the Cassandra plugin to
 * override any "default" handling that core provides to better fit in with the types that are persistable in
 * Cassandra. It also allows us to specify the Cassandra "type name" on the Columns (for later use in schema
 * generation).
 */
public class SchemaVerifierImpl implements SchemaVerifier
{
    StoreManager storeMgr;

    AbstractClassMetaData cmd;

    ClassLoaderResolver clr;

    public SchemaVerifierImpl(StoreManager storeMgr, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        this.storeMgr = storeMgr;
        this.cmd = cmd;
        this.clr = clr;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.schema.table.SchemaVerifier#
     * verifyTypeConverterForMember(org.datanucleus.metadata.AbstractMemberMetaData,
     * org.datanucleus.store.types.converters.TypeConverter)
     */
    @Override
    public TypeConverter verifyTypeConverterForMember(AbstractMemberMetaData mmd, TypeConverter conv)
    {
        if (conv != null && !(conv instanceof MultiColumnConverter))
        {
            Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, mmd.getType());
            if (datastoreType != null)
            {
                if (java.util.Date.class.isAssignableFrom(datastoreType) && datastoreType != java.util.Date.class)
                {
                    // Swap for a converter that has java.util.Date in the datastore
                    // (since that's what Cassandra returns)
                    TypeConverter newConv = storeMgr.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), java.util.Date.class);
                    if (newConv == null)
                    {
                        NucleusLogger.DATASTORE.info("Member " + mmd.getFullFieldName() + " required to convert to datastore type of " + datastoreType.getName() +
                            " but no supported converter for this datastore!");
                    }
                    return newConv;
                }
                else if (java.util.UUID.class.isAssignableFrom(mmd.getType()))
                {
                    // UUID : by default this will get a TypeConverter<UUID,String> assigned, so unset unless
                    // explicitly requested
                    ColumnMetaData[] colmds = mmd.getColumnMetaData();
                    boolean jdbcTypeSpecified = false;
                    if (colmds != null && colmds.length == 1 && !StringUtils.isWhitespace(colmds[0].getJdbcTypeName()))
                    {
                        jdbcTypeSpecified = true;
                    }
                    if (!jdbcTypeSpecified)
                    {
                        // We don't want a TypeConverter for UUID except when requested, so unset the default
                        // TypeConverter
                        return null;
                    }
                }
            }
        }
        return conv;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.datanucleus.store.schema.table.SchemaVerifier#attributeMember(org.datanucleus.store.schema.table
     * .MemberColumnMapping)
     */
    @Override
    public void attributeMember(MemberColumnMapping mapping)
    {
        if (mapping.getColumn(0).getColumnType() == ColumnType.DATASTOREID_COLUMN)
        {
            String type = "bigint"; // Default to bigint unless specified
            IdentityMetaData idmd = cmd.getIdentityMetaData();
            if (idmd != null && idmd.getColumnMetaData() != null && idmd.getColumnMetaData().getJdbcType() != null)
            {
                JdbcType jdbcType = idmd.getColumnMetaData().getJdbcType();
                if (MetaDataUtils.isJdbcTypeString(jdbcType))
                {
                    type = "varchar";
                }
                else if (jdbcType == JdbcType.INTEGER)
                {
                    type = "int";
                }
            }
            mapping.getColumn(0).setTypeName(type);
        }
        else if (mapping.getColumn(0).getColumnType() == ColumnType.VERSION_COLUMN)
        {
            String cassandraType = cmd.getVersionMetaDataForClass().getVersionStrategy() == VersionStrategy.DATE_TIME ? "timestamp" : "bigint";
            mapping.getColumn(0).setTypeName(cassandraType);
        }
        else if (mapping.getColumn(0).getColumnType() == ColumnType.DISCRIMINATOR_COLUMN)
        {
            mapping.getColumn(0).setTypeName("varchar");
        }
        else if (mapping.getColumn(0).getColumnType() == ColumnType.MULTITENANCY_COLUMN)
        {
            mapping.getColumn(0).setTypeName("varchar");
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.datanucleus.store.schema.table.SchemaVerifier#attributeColumn(org.datanucleus.store.schema.table
     * .MemberColumnMapping, org.datanucleus.metadata.AbstractMemberMetaData)
     */
    @Override
    public void attributeMember(MemberColumnMapping mapping, AbstractMemberMetaData mmd)
    {
        verifyMemberColumnMapping(mmd, mapping, storeMgr.getNucleusContext().getTypeManager(), clr);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.datanucleus.store.schema.table.SchemaVerifier#attributeEmbeddedColumn(org.datanucleus.store.schema
     * .table.MemberColumnMapping, java.util.List)
     */
    @Override
    public void attributeEmbeddedMember(MemberColumnMapping mapping, List<AbstractMemberMetaData> mmds)
    {
        AbstractMemberMetaData mmd = mmds.get(mmds.size() - 1);
        verifyMemberColumnMapping(mmd, mapping, storeMgr.getNucleusContext().getTypeManager(), clr);
    }

    /**
     * Method to verify the member-column mapping and assign the Cassandra type to all Columns that it contains.
     * @param mmd Metadata for the member
     * @param mapping Member-column mapping
     * @param typeMgr Type manager
     * @param clr ClassLoader resolver
     */
    public static void verifyMemberColumnMapping(AbstractMemberMetaData mmd, MemberColumnMapping mapping, TypeManager typeMgr, ClassLoaderResolver clr)
    {
        String type = null;

        if (mapping.getTypeConverter() != null)
        {
            // TypeConverter defined, so just lookup the Cassandra type
            if (mapping.getNumberOfColumns() > 1)
            {
                Class[] datastoreJavaTypes = ((MultiColumnConverter) mapping.getTypeConverter()).getDatastoreColumnTypes();
                for (int i = 0; i < datastoreJavaTypes.length; i++)
                {
                    type = CassandraUtils.getCassandraTypeForDatastoreType(datastoreJavaTypes[i].getName());
                    mapping.getColumn(i).setTypeName(type);
                }
            }
            else
            {
                Class datastoreJavaType = TypeConverterHelper.getDatastoreTypeForTypeConverter(mapping.getTypeConverter(), mmd.getType());
                type = CassandraUtils.getCassandraTypeForDatastoreType(datastoreJavaType.getName());
                mapping.getColumn(0).setTypeName(type);
            }
        }
        else
        {
            // No TypeConverter so work out the cassandra type most appropriate
            RelationType relType = mmd.getRelationType(clr);
            boolean optional = false;
            if (Optional.class.isAssignableFrom(mmd.getType()))
            {
                if (relType != RelationType.NONE)
                {
                    relType = RelationType.ONE_TO_ONE_UNI; // Optional<PC>
                }
                optional = true;
            }

            if (relType == RelationType.NONE)
            {
                if (mmd.isSerialized())
                {
                    // Could check if type is Serializable but user may have
                    // Object field that stores Serializable objects
                    type = "blob";
                }
                else if (!optional && mmd.hasContainer())
                {
                    if (mmd.hasCollection())
                    {
                        Class elementType = clr.classForName(mmd.getCollection().getElementType());
                        String cqlElementType = null;
                        if (mmd.getElementMetaData() != null && mmd.getElementMetaData().hasExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME))
                        {
                            TypeConverter elemTypeConv = typeMgr.getTypeConverterForName(mmd.getElementMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME));
                            Class datastoreJavaType = TypeConverterHelper.getDatastoreTypeForTypeConverter(elemTypeConv, clr.classForName(mmd.getCollection().getElementType()));
                            cqlElementType = CassandraUtils.getCassandraTypeForDatastoreType(datastoreJavaType.getName());
                        }
                        else
                        {
                            cqlElementType = mmd.getCollection().isSerializedElement() ? "blob" : CassandraUtils.getCassandraTypeForNonPersistableType(elementType, false, typeMgr, null);
                        }

                        if (List.class.isAssignableFrom(mmd.getType()) || Queue.class.isAssignableFrom(mmd.getType()))
                        {
                            type = "list<" + cqlElementType + ">";
                        }
                        else if (Set.class.isAssignableFrom(mmd.getType()))
                        {
                            type = "set<" + cqlElementType + ">";
                        }
                        else
                        {
                            if (mmd.getOrderMetaData() != null)
                            {
                                type = "list<" + cqlElementType + ">";
                            }
                            else
                            {
                                type = "set<" + cqlElementType + ">";
                            }
                        }
                    }
                    else if (mmd.hasMap())
                    {
                        // Map<NonPC, NonPC>
                        Class keyType = clr.classForName(mmd.getMap().getKeyType());
                        Class valType = clr.classForName(mmd.getMap().getValueType());
                        String cqlKeyType = null;
                        if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().hasExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME))
                        {
                            TypeConverter keyTypeConv = typeMgr.getTypeConverterForName(mmd.getKeyMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME));
                            Class datastoreJavaType = TypeConverterHelper.getDatastoreTypeForTypeConverter(keyTypeConv, clr.classForName(mmd.getMap().getKeyType()));
                            cqlKeyType = CassandraUtils.getCassandraTypeForDatastoreType(datastoreJavaType.getName());
                        }
                        else
                        {
                            cqlKeyType = mmd.getMap().isSerializedKey() ? "blob" : CassandraUtils.getCassandraTypeForNonPersistableType(keyType, false, typeMgr, null);
                        }

                        String cqlValType = null;
                        if (mmd.getValueMetaData() != null && mmd.getValueMetaData().hasExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME))
                        {
                            TypeConverter valTypeConv = typeMgr.getTypeConverterForName(mmd.getValueMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME));
                            Class datastoreJavaType = TypeConverterHelper.getDatastoreTypeForTypeConverter(valTypeConv, clr.classForName(mmd.getMap().getValueType()));
                            cqlValType = CassandraUtils.getCassandraTypeForDatastoreType(datastoreJavaType.getName());
                        }
                        else
                        {
                            cqlValType = mmd.getMap().isSerializedValue() ? "blob" : CassandraUtils.getCassandraTypeForNonPersistableType(valType, false, typeMgr, null);
                        }
                        type = "map<" + cqlKeyType + "," + cqlValType + ">";
                    }
                    else if (mmd.hasArray())
                    {
                        // NonPC[]
                        Class elementType = clr.classForName(mmd.getArray().getElementType());
                        String cqlElementType = mmd.getArray().isSerializedElement() ? "blob" : CassandraUtils.getCassandraTypeForNonPersistableType(elementType, false, typeMgr, null);
                        type = "list<" + cqlElementType + ">";
                    }
                }
                else
                {
                    Column col = mapping.getColumn(0);
                    if (col.getJdbcType() != null)
                    {
                        // Use jdbc-type where it is specified
                        if (MetaDataUtils.isJdbcTypeString(col.getJdbcType()))
                        {
                            type = "varchar";
                        }
                        else if (col.getJdbcType() == JdbcType.BIGINT)
                        {
                            type = "bigint";
                        }
                        else if (col.getJdbcType() == JdbcType.CHAR)
                        {
                            col.setJdbcType(JdbcType.VARCHAR); // Not available with Cassandra
                            type = "varchar";
                        }
                        else if (col.getJdbcType() == JdbcType.BLOB)
                        {
                            type = "blob";
                        }
                        else if (col.getJdbcType() == JdbcType.INTEGER || col.getJdbcType() == JdbcType.SMALLINT || col.getJdbcType() == JdbcType.TINYINT)
                        {
                            type = "int";
                        }
                        else if (col.getJdbcType() == JdbcType.DECIMAL)
                        {
                            type = "decimal";
                        }
                        else if (col.getJdbcType() == JdbcType.FLOAT)
                        {
                            type = "float";
                        }
                        else if (col.getJdbcType() == JdbcType.DOUBLE)
                        {
                            type = "double";
                        }
                        else if (col.getJdbcType() == JdbcType.DATE || col.getJdbcType() == JdbcType.TIME || col.getJdbcType() == JdbcType.TIMESTAMP)
                        {
                            type = "timestamp";
                        }
                    }
                    if (type == null)
                    {
                        // Fallback to defaults based on the member type
                        String typeName = mmd.getTypeName();
                        if (optional)
                        {
                            typeName = mmd.getCollection().getElementType();
                        }
                        type = CassandraUtils.getCassandraTypeForDatastoreType(typeName);
                        if (type != null)
                        {
                            // Just use the default type
                        }
                        else if (Enum.class.isAssignableFrom(mmd.getType()))
                        {
                            // Default to persisting the Enum.ordinal (can use Enum.name if varchar specified
                            // above)
                            type = "int";
                        }
                        else
                        {
                            // Try String/Long/Int converters (in case not assigned by CompleteClassTable)
                            TypeConverter stringConverter = typeMgr.getTypeConverterForType(mmd.getType(), String.class);
                            if (stringConverter != null)
                            {
                                type = "varchar";
                                mapping.setTypeConverter(stringConverter);
                            }
                            else
                            {
                                TypeConverter longConverter = typeMgr.getTypeConverterForType(mmd.getType(), Long.class);
                                if (longConverter != null)
                                {
                                    type = "bigint";
                                    mapping.setTypeConverter(longConverter);
                                }
                                else
                                {
                                    TypeConverter intConverter = typeMgr.getTypeConverterForType(mmd.getType(), Integer.class);
                                    if (intConverter != null)
                                    {
                                        type = "int";
                                        mapping.setTypeConverter(intConverter);
                                    }
                                    else if (Serializable.class.isAssignableFrom(mmd.getType()))
                                    {
                                        type = "blob";
                                        mapping.setTypeConverter(typeMgr.getTypeConverterForType(Serializable.class, ByteBuffer.class));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if (RelationType.isRelationSingleValued(relType))
            {
                // TODO Support 1-1 storage using "FK" style column(s) for related object
                // 1-1/N-1 relation stored as String (or serialised)
                type = mmd.isSerialized() ? "blob" : "varchar";
            }
            else if (RelationType.isRelationMultiValued(relType))
            {
                // 1-N/M-N relation stored as set/list<String> or set/list<blob> (or serialised whole field)
                if (mmd.hasCollection())
                {
                    if (List.class.isAssignableFrom(mmd.getType()) || Queue.class.isAssignableFrom(mmd.getType()))
                    {
                        type = mmd.getCollection().isSerializedElement() ? "list<blob>" : "list<varchar>";
                    }
                    else if (Set.class.isAssignableFrom(mmd.getType()))
                    {
                        type = mmd.getCollection().isSerializedElement() ? "set<blob>" : "set<varchar>";
                    }
                    else
                    {
                        if (relType == RelationType.MANY_TO_MANY_BI)
                        {
                            type = mmd.getCollection().isSerializedElement() ? "set<blob>" : "set<varchar>";
                        }
                        else if (mmd.getOrderMetaData() != null)
                        {
                            type = mmd.getCollection().isSerializedElement() ? "list<blob>" : "list<varchar>";
                        }
                        else
                        {
                            type = mmd.getCollection().isSerializedElement() ? "set<blob>" : "set<varchar>";
                        }
                    }
                }
                else if (mmd.hasMap())
                {
                    String keyType = null;
                    String valType = null;
                    if (mmd.getMap().keyIsPersistent())
                    {
                        keyType = mmd.getMap().isSerializedKey() ? "blob" : "varchar";
                    }
                    else
                    {
                        keyType = CassandraUtils.getCassandraTypeForDatastoreType(mmd.getMap().getKeyType());
                    }
                    if (mmd.getMap().valueIsPersistent())
                    {
                        valType = mmd.getMap().isSerializedValue() ? "blob" : "varchar";
                    }
                    else
                    {
                        valType = CassandraUtils.getCassandraTypeForDatastoreType(mmd.getMap().getValueType());
                    }
                    type = "map<" + keyType + "," + valType + ">";
                }
                else if (mmd.hasArray())
                {
                    type = mmd.getArray().isSerializedElement() ? "list<blob>" : "list<varchar>";
                }
            }

            if (!StringUtils.isWhitespace(type))
            {
                mapping.getColumn(0).setTypeName(type);
            }
            else
            {
                // TODO Allow for fields declared as Object but with particular persistent implementations
                NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type=" + mmd.getTypeName() + " could not be directly mapped for Cassandra. Using varchar column");
                // Fallback to varchar - maybe BLOB would be better???
                mapping.getColumn(0).setTypeName("varchar");
            }
        }
    }
}