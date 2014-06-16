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
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.Row;

/**
 * Utility methods for handling Cassandra datastores.
 */
public class CassandraUtils
{
    private CassandraUtils() {}

    static Map<String, String> cassandraTypeByJavaType = new HashMap<String, String>();

    static Map<String, Class> datastoreTypeByCassandraType = new HashMap<String, Class>();

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
        cassandraTypeByJavaType.put(BigDecimal.class.getName(), "decimal");
        cassandraTypeByJavaType.put(BigInteger.class.getName(), "bigint");
        cassandraTypeByJavaType.put(Date.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Time.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(java.sql.Date.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Timestamp.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Calendar.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(TimeZone.class.getName(), "varchar");
        cassandraTypeByJavaType.put(Locale.class.getName(), "varchar");

        datastoreTypeByCassandraType.put("timestamp", Date.class);
        datastoreTypeByCassandraType.put("boolean", Boolean.class);
        datastoreTypeByCassandraType.put("int", Integer.class);
        datastoreTypeByCassandraType.put("double", Double.class);
        datastoreTypeByCassandraType.put("float", Float.class);
        datastoreTypeByCassandraType.put("bigint", Long.class);
        datastoreTypeByCassandraType.put("varchar", String.class);
        datastoreTypeByCassandraType.put("blob", ByteBuffer.class);
    }

    public static Class getJavaTypeForCassandraType(String cassandraType)
    {
        return datastoreTypeByCassandraType.get(cassandraType);
    }

    public static String getCassandraTypeForDatastoreType(String javaType)
    {
        return cassandraTypeByJavaType.get(javaType);
    }

    public static Object getMemberValueForColumnWithConverter(Row row, Column column, TypeConverter typeConv)
    {
        if (column.getTypeName().equals("varchar"))
        {
            return typeConv.toMemberType(row.getString(column.getName()));
        }
        else if (column.getTypeName().equals("int"))
        {
            return typeConv.toMemberType(row.getInt(column.getName()));
        }
        else if (column.getTypeName().equals("boolean"))
        {
            return typeConv.toMemberType(row.getBool(column.getName()));
        }
        else if (column.getTypeName().equals("double"))
        {
            return typeConv.toMemberType(row.getDouble(column.getName()));
        }
        else if (column.getTypeName().equals("float"))
        {
            return typeConv.toMemberType(row.getFloat(column.getName()));
        }
        else if (column.getTypeName().equals("bigint"))
        {
            return typeConv.toMemberType(row.getLong(column.getName()));
        }
        else if (column.getTypeName().equals("timestamp"))
        {
            return typeConv.toMemberType(row.getDate(column.getName()));
        }
        else if (column.getTypeName().equals("blob"))
        {
            return typeConv.toMemberType(row.getBytes(column.getName()));
        }
        return null;
    }

    public static Object getJavaValueForDatastoreValue(Object datastoreValue, String cassandraType, Class javaType, ExecutionContext ec)
    {
        if (datastoreValue == null)
        {
            return null;
        }

        if (cassandraType.equals("blob") && datastoreValue instanceof ByteBuffer)
        {
            // Serialised field
            TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
            return serialConv.toMemberType((ByteBuffer) datastoreValue);
        }
        else if (javaType.isEnum())
        {
            if (cassandraType.equals("int"))
            {
                return javaType.getEnumConstants()[(Integer) datastoreValue];
            }

            return Enum.valueOf(javaType, (String) datastoreValue);
        }
        else if (java.sql.Date.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            // TODO There is a TypeConverter for this
            return new java.sql.Date(((Date) datastoreValue).getTime());
        }
        else if (java.sql.Time.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            // TODO There is a TypeConverter for this
            return new java.sql.Time(((Date) datastoreValue).getTime());
        }
        else if (java.sql.Timestamp.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            // TODO There is a TypeConverter for this
            return new java.sql.Timestamp(((Date) datastoreValue).getTime());
        }
        else if (Calendar.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            // TODO There is a TypeConverter for this
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date) datastoreValue);
            return cal;
        }
        else if (Date.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            return datastoreValue;
        }
        else if (datastoreValue instanceof String)
        {
            TypeConverter converter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
            if (converter != null)
            {
                return converter.toMemberType(datastoreValue);
            }
        }
        else if (datastoreValue instanceof Long)
        {
            TypeConverter converter = ec.getTypeManager().getTypeConverterForType(javaType, Long.class);
            if (converter != null)
            {
                return converter.toMemberType(datastoreValue);
            }
        }
        else if (datastoreValue instanceof Integer)
        {
            TypeConverter converter = ec.getTypeManager().getTypeConverterForType(javaType, Integer.class);
            if (converter != null)
            {
                return converter.toMemberType(datastoreValue);
            }
        }

        return datastoreValue;
    }

    /**
     * Convenience method to return the Cassandra type that we would store the provided type as. Note that
     * this does not support container (Collection, Map) types just single value types.
     * @param type The java type
     * @param serialised Whether it should be serialised
     * @param typeMgr The type manager
     * @param jdbcType Any jdbc-type that has been specified to take into account
     * @return The Cassandra type
     */
    public static String getCassandraTypeForNonPersistableType(Class type, boolean serialised, TypeManager typeMgr, String jdbcType)
    {
        String cTypeName = cassandraTypeByJavaType.get(type.getName());
        if (cTypeName != null)
        {
            return cTypeName;
        }
        else if (serialised && Serializable.class.isAssignableFrom(type))
        {
            return "blob";
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            if (jdbcType != null && jdbcType.equalsIgnoreCase("varchar"))
            {
                return "varchar";
            }
            return "int";
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

        // Fallback to varchar
        return "varchar";
    }

    /**
     * Convenience method to convert from a non-persistable value to the value to be stored in Cassandra.
     * @param value Value for the member
     * @param datastoreType Cassandra column type
     * @param serialised Whether the value is to be stored serialised
     * @param typeMgr Type Manager
     * @return The value to be stored
     */
    public static Object getDatastoreValueForNonPersistableValue(Object value, String datastoreType, boolean serialised, TypeManager typeMgr)
    {
        // TODO Support TypeManager autoApply type converter
        if (value == null)
        {
            return value;
        }
        else if (serialised && value instanceof Serializable)
        {
            // Convert to ByteBuffer and use that
            TypeConverter serialConv = typeMgr.getTypeConverterForType(Serializable.class, ByteBuffer.class);
            return serialConv.toDatastoreType(value);
        }
        else if (value.getClass() == Character.class)
        {
            return "" + value;
        }
        else if (value.getClass() == Byte.class)
        {
            return ((Byte) value).intValue();
        }
        else if (value.getClass() == Short.class)
        {
            return ((Short) value).intValue();
        }
        else if (value.getClass() == Float.class)
        {
            if (datastoreType.equals("decimal"))
            {
                return BigDecimal.valueOf((Float) value);
            }
            if (datastoreType.equals("double"))
            {
                return Double.valueOf((Float) value);
            }
            return value;
        }
        else if (value.getClass() == Double.class)
        {
            if (datastoreType.equals("decimal"))
            {
                return BigDecimal.valueOf((Double) value);
            }
            return value;
        }
        else if (ClassUtils.isPrimitiveWrapperType(value.getClass().getName()))
        {
            return value;
        }
        else if (value.getClass() == BigInteger.class)
        {
            // TODO There is a TypeConverter for this
            return ((BigInteger) value).longValue();
        }
        else if (value.getClass() == BigDecimal.class)
        {
            // TODO There is a TypeConverter for this
            return value;
        }
        else if (value instanceof Enum)
        {
            // Persist as ordinal unless user specifies jdbc-type of "varchar"
            if (datastoreType.equals("varchar"))
            {
                return ((Enum) value).name();
            }
            return ((Enum) value).ordinal();
        }
        else if (value instanceof Calendar)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(Calendar.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            // TODO There is a TypeConverter for this
            return ((Calendar) value).getTime();
        }
        else if (value instanceof Date)
        {
            if (datastoreType.equals("varchar"))
            {
                // TODO When we have a lookup map per table of column and TypeConverter, remove this
                Class valueType = Date.class;
                if (value instanceof Time)
                {
                    valueType = Time.class;
                }
                else if (value instanceof java.sql.Date)
                {
                    valueType = java.sql.Date.class;
                }
                else if (value instanceof Timestamp)
                {
                    valueType = Timestamp.class;
                }
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(valueType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            // TODO There is a TypeConverter for this
            return value;
        }
        else if (value instanceof TimeZone)
        {
            TypeConverter stringConverter = typeMgr.getTypeConverterForType(TimeZone.class, String.class);
            if (stringConverter != null)
            {
                return stringConverter.toDatastoreType(value);
            }
        }
        else if (value instanceof Locale)
        {
            TypeConverter stringConverter = typeMgr.getTypeConverterForType(Locale.class, String.class);
            if (stringConverter != null)
            {
                return stringConverter.toDatastoreType(value);
            }
        }

        TypeConverter stringConverter = typeMgr.getTypeConverterForType(value.getClass(), String.class);
        if (stringConverter != null)
        {
            return stringConverter.toDatastoreType(value);
        }
        TypeConverter longConverter = typeMgr.getTypeConverterForType(value.getClass(), Long.class);
        if (longConverter != null)
        {
            return longConverter.toDatastoreType(value);
        }

        // TODO Support any other types
        return value;
    }

    /**
     * Method to take a ResultSet Row and convert it into a persistable object.
     * @param row The results row
     * @param cmd Metadata for the class of which this is an instance (or subclass)
     * @param ec ExecutionContext managing it
     * @param fpMembers FetchPlan members to populate
     * @param ignoreCache Whether to ignore the cache when instantiating this
     * @return The persistable object for this row.
     */
    public static Object getPojoForRowForCandidate(Row row, AbstractClassMetaData cmd, ExecutionContext ec, int[] fpMembers, boolean ignoreCache)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Determine the class from the discriminator property
            // String discrimColName = ec.getStoreManager().getNamingFactory().getColumnName(cmd,
            // ColumnType.DISCRIMINATOR_COLUMN);
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
            throw new NucleusUserException(
                    "Attempt to get candidate for class " + cmd.getFullClassName() + " but uses nondurable-identity and this is not supported by this datastore");
        }
        return pojo;
    }

    private static Object getObjectUsingApplicationIdForRow(final Row row, final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache,
            final int[] fpMembers)
    {
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd, table);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, new FieldValues()
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
                if (table.getVersionColumn().getTypeName().equals("int"))
                {
                    version = row.getInt(table.getVersionColumn().getName());
                }
                else
                {
                    version = row.getLong(table.getVersionColumn().getName());
                }
            }
            op.setVersion(version);
        }

        return pc;
    }

    private static Object getObjectUsingDatastoreIdForRow(final Row row, final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache,
            final int[] fpMembers)
    {
        Object idKey = null;
        StoreManager storeMgr = ec.getStoreManager();
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
        {
            // TODO Support this?
        }
        else
        {
            Column col = table.getDatastoreIdColumn();
            if (col.getTypeName().equals("varchar"))
            {
                idKey = row.getString(col.getName());
            }
            else
            {
                idKey = row.getLong(col.getName());
            }
        }

        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd, table);
        Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, new FieldValues()
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
                if (table.getVersionColumn().getTypeName().equals("int"))
                {
                    version = row.getInt(table.getVersionColumn().getName());
                }
                else
                {
                    version = row.getLong(table.getVersionColumn().getName());
                }
            }
            op.setVersion(version);
        }
        return pc;
    }

    /**
     * Convenience method to log the provided CQL statement, substituting the provided parameters for any "?"
     * in the statement
     * @param stmt The CQL statement
     * @param values Any parameter values
     * @param logger The logger to log to (at DEBUG level).
     */
    public static void logCqlStatement(String stmt, Object[] values, NucleusLogger logger)
    {
        if (values == null || values.length == 0)
        {
            logger.debug(stmt);
            return;
        }

        StringBuilder str = new StringBuilder();
        int paramNo = 0;
        int currentPos = 0;
        boolean moreParams = true;
        while (moreParams)
        {
            int pos = stmt.indexOf('?', currentPos);
            if (pos > 0)
            {
                str.append(stmt.substring(currentPos, pos));
                str.append('<').append("" + values[paramNo]).append('>');
                paramNo++;

                currentPos = pos + 1;
            }
            else
            {
                moreParams = false;
            }
        }
        str.append(stmt.substring(currentPos));

        logger.debug(str.toString());
    }
}