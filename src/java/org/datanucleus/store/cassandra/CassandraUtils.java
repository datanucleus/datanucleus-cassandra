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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;

import com.datastax.driver.core.Row;

/**
 * Utility methods for handling Cassandra datastores.
 */
public class CassandraUtils
{
    static Map<String, String> cassandraTypeByJavaType = new HashMap<String, String>();

    static
    {
        cassandraTypeByJavaType.put(boolean.class.getName(), "boolean");
        cassandraTypeByJavaType.put(byte.class.getName(), "int");
        cassandraTypeByJavaType.put(char.class.getName(), "varchar");
        cassandraTypeByJavaType.put(double.class.getName(), "double");
        cassandraTypeByJavaType.put(float.class.getName(), "float");
        cassandraTypeByJavaType.put(int.class.getName(), "int");
        cassandraTypeByJavaType.put(long.class.getName(), "bigint");
        cassandraTypeByJavaType.put(short.class.getName(), "int");
        cassandraTypeByJavaType.put(Boolean.class.getName(), "boolean");
        cassandraTypeByJavaType.put(Byte.class.getName(), "int");
        cassandraTypeByJavaType.put(Character.class.getName(), "varchar");
        cassandraTypeByJavaType.put(Double.class.getName(), "double");
        cassandraTypeByJavaType.put(Float.class.getName(), "float");
        cassandraTypeByJavaType.put(Integer.class.getName(), "int");
        cassandraTypeByJavaType.put(Long.class.getName(), "bigint");
        cassandraTypeByJavaType.put(Short.class.getName(), "int");

        cassandraTypeByJavaType.put(String.class.getName(), "varchar");
        cassandraTypeByJavaType.put(BigDecimal.class.getName(), "double");
        cassandraTypeByJavaType.put(BigInteger.class.getName(), "bigint");
        cassandraTypeByJavaType.put(Date.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Time.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(java.sql.Date.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Timestamp.class.getName(), "timestamp");
    }

    /**
     * Method to return the Cassandra column type that the specified member will be stored as.
     * @param mmd Metadata for the member
     * @param typeMgr Type manager
     * @return The cassandra column type
     */
    public static String getCassandraColumnTypeForMember(AbstractMemberMetaData mmd, TypeManager typeMgr, ClassLoaderResolver clr)
    {
        // TODO Make use of jdbc-type in mmd.getColumnMetaData()
        Class type = mmd.getType();
        String cTypeName = cassandraTypeByJavaType.get(type.getName());
        if (cTypeName != null)
        {
            return cTypeName;
        }
        else if (mmd.isSerialized() && Serializable.class.isAssignableFrom(type))
        {
            return "blob";
        }

        if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1)
            {
                if (colmds[0].getJdbcType().equalsIgnoreCase("varchar"))
                {
                    return "varchar";
                }
            }
            return "int";
        }

        RelationType relType = mmd.getRelationType(clr);
        // TODO Support embedded relations

        if (RelationType.isRelationSingleValued(relType))
        {
            return "varchar"; // TODO Assumes we persist the String form of the id of the related object
        }
        else if (RelationType.isRelationMultiValued(relType))
        {
            if (mmd.hasCollection())
            {
                if (List.class.isAssignableFrom(mmd.getType()))
                {
                    return "list<varchar>"; // TODO Assumes we persist the String form of the id of the related object
                }
                else if (Set.class.isAssignableFrom(mmd.getType()))
                {
                    return "set<varchar>"; // TODO Assumes we persist the String form of the id of the related object
                }
            }
            else if (mmd.hasMap())
            {
                // TODO Support maps where key/val is persistable
            }
        }
        else
        {
            if (mmd.hasCollection())
            {
                String elementType = mmd.getCollection().getElementType();
                String cqlElementType = cassandraTypeByJavaType.get(elementType);
                if (cqlElementType != null)
                {
                    if (List.class.isAssignableFrom(mmd.getType()))
                    {
                        return "list<" + cqlElementType + ">";
                    }
                    else if (Set.class.isAssignableFrom(mmd.getType()))
                    {
                        return "set<" + cqlElementType + ">";
                    }
                }
            }
            else if (mmd.hasMap())
            {
                String keyType = mmd.getMap().getKeyType();
                String valType = mmd.getMap().getValueType();
                String cqlKeyType = cassandraTypeByJavaType.get(keyType);
                String cqlValType = cassandraTypeByJavaType.get(valType);
                if (cqlKeyType != null && cqlValType != null)
                {
                    return "map<" + cqlKeyType + "," + cqlValType + ">";
                }
            }

            // No direct mapping, so find a converter
            TypeConverter stringConverter = typeMgr.getTypeConverterForType(type, String.class);
            if (stringConverter != null)
            {
                return "varchar";
            }
            TypeConverter longConverter = typeMgr.getTypeConverterForType(type, Long.class);
            if (longConverter != null)
            {
                return "bigint";
            }
            TypeConverter intConverter = typeMgr.getTypeConverterForType(type, Integer.class);
            if (intConverter != null)
            {
                return "int";
            }
            if (Serializable.class.isAssignableFrom(type))
            {
                return "blob";
            }
        }

        // Fallback to varchar - maybe BLOB would be better???
        return "varchar";
    }

    /**
     * Convenience method to convert from a member value to the value to be stored in Cassandra.
     * @param mmd Metadata for the member
     * @param memberValue Value for the member
     * @return The value to be stored
     */
    public static Object getDatastoreValueForMemberValue(AbstractMemberMetaData mmd, Object memberValue)
    {
        // TODO Do the conversion to match above types
        return memberValue;
    }

    public static Object getPojoForRowForCandidate(Row row, AbstractClassMetaData cmd, ExecutionContext ec, int[] fpMembers, boolean ignoreCache)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Determine the class from the discriminator property
//            String discrimColName = ec.getStoreManager().getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            // TODO Get the value for this column
        }

        Object pojo = null;
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            pojo = CassandraUtils.getObjectUsingApplicationIdForRow(row, cmd, ec, ignoreCache, fpMembers);
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            pojo = CassandraUtils.getObjectUsingDatastoreIdForRow(row, cmd, ec, ignoreCache, fpMembers);
        }
        else
        {
            throw new NucleusUserException("Attempt to get candidate for class " + cmd.getFullClassName() + " but uses nondurable-identity and this is not supported by this datastore");
        }
        return pojo;
    }

    public static Object getObjectUsingApplicationIdForRow(final Row row, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        StoreManager storeMgr = ec.getStoreManager();
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            ObjectProvider op = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = op.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = row.getInt(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN));
            }
            op.setVersion(version);
        }

        return pc;
    }

    public static Object getObjectUsingDatastoreIdForRow(final Row row, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        Object idKey = null;
        StoreManager storeMgr = ec.getStoreManager();
        if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
        {
            // TODO Support this?
        }
        else
        {
            idKey = row.getLong(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
        }

        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd);
        OID oid = OIDFactory.getInstance(ec.getNucleusContext(), cmd.getFullClassName(), idKey);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(oid, 
            new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            ObjectProvider op = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = op.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = row.getLong(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN));
            }
            op.setVersion(version);
        }
        return pc;
    }
}