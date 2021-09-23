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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.datanucleus.store.cassandra.query.ResultClassInfo;

/**
 * Utility methods for handling Cassandra datastores.
 */
public class CassandraUtils
{
    private CassandraUtils()
    {
    }

    static Map<String, String> cassandraTypeByJavaType = new HashMap<>();

    static Map<String, Class> datastoreTypeByCassandraType = new HashMap<>();

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
        cassandraTypeByJavaType.put(java.sql.Time.class.getName(), "time");
        cassandraTypeByJavaType.put(java.sql.Date.class.getName(), "date");
        cassandraTypeByJavaType.put(Timestamp.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(java.time.LocalTime.class.getName(), "time");
        cassandraTypeByJavaType.put(java.time.LocalDate.class.getName(), "date");
        cassandraTypeByJavaType.put(java.time.YearMonth.class.getName(), "date");
        cassandraTypeByJavaType.put(java.time.MonthDay.class.getName(), "date");
        cassandraTypeByJavaType.put(java.time.LocalDateTime.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(java.time.Instant.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(Calendar.class.getName(), "timestamp");
        cassandraTypeByJavaType.put(TimeZone.class.getName(), "varchar");
        cassandraTypeByJavaType.put(Locale.class.getName(), "varchar");
        cassandraTypeByJavaType.put(UUID.class.getName(), "uuid");
        cassandraTypeByJavaType.put(ByteBuffer.class.getName(), "blob");

        datastoreTypeByCassandraType.put("timestamp", Date.class);
        datastoreTypeByCassandraType.put("boolean", Boolean.class);
        datastoreTypeByCassandraType.put("int", Integer.class);
        datastoreTypeByCassandraType.put("double", Double.class);
        datastoreTypeByCassandraType.put("float", Float.class);
        datastoreTypeByCassandraType.put("bigint", Long.class);
        datastoreTypeByCassandraType.put("varchar", String.class);
        datastoreTypeByCassandraType.put("blob", ByteBuffer.class);
        datastoreTypeByCassandraType.put("uuid", UUID.class);
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
            return typeConv.toMemberType(row.getBoolean(column.getName()));
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
            return typeConv.toMemberType(Timestamp.from(row.getInstant(column.getName())));
        }
        else if (column.getTypeName().equals("blob"))
        {
            return typeConv.toMemberType(row.getByteBuffer(column.getName()));
        }
        else if (column.getTypeName().equals("uuid"))
        {
            return row.getUuid(column.getName());
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
        else if (java.time.LocalDateTime.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            return java.time.LocalDateTime.ofInstant((Instant)datastoreValue, ZoneId.systemDefault());   
        }
        else if (java.time.LocalDate.class.isAssignableFrom(javaType))
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
        else if (java.time.LocalTime.class.isAssignableFrom(javaType))
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
        else if (java.time.Instant.class.isAssignableFrom(javaType))
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
        else if (java.time.YearMonth.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            return YearMonth.from((LocalDate)datastoreValue);
        }
        else if (java.time.MonthDay.class.isAssignableFrom(javaType))
        {
            if (cassandraType.equals("varchar"))
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(javaType, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(datastoreValue);
                }
            }
            return MonthDay.from((LocalDate)datastoreValue);
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
     * Convenience method to return the Cassandra type that we would store the provided type as. 
     * Note that this does not support container (Collection, Map) types just single value types.
     * @param type The java type
     * @param serialised Whether it should be serialised
     * @param typeMgr The type manager
     * @param jdbcType Any jdbc-type that has been specified to take into account
     * @param mmd The field/property that this is for
     * @param role The role of the field that this value represents (i.e whole field, collection element, map key, etc)
     * @param clr ClassLoader resolver
     * @return The Cassandra type
     */
    public static String getCassandraTypeForNonPersistableType(Class type, boolean serialised, TypeManager typeMgr, String jdbcType, AbstractMemberMetaData mmd, FieldRole role,
            ClassLoaderResolver clr)
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
            if (jdbcType != null)
            {
                if (jdbcType.equalsIgnoreCase("varchar"))
                {
                    return "varchar";
                }
                return "int";
            }
            JdbcType enumJdbcType = MetaDataUtils.getJdbcTypeForEnum(mmd, role, clr);
            if (MetaDataUtils.isJdbcTypeNumeric(enumJdbcType))
            {
                return "int";
            }
            return "varchar";
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
     * @param mmd The field/property that this is for
     * @param role The role of the field that this value represents (i.e whole field, collection element, map key, etc)
     * @return The value to be stored
     */
    public static Object getDatastoreValueForNonPersistableValue(Object value, String datastoreType, boolean serialised, TypeManager typeMgr, AbstractMemberMetaData mmd, FieldRole role)
    {
        // TODO Support TypeManager autoApply type converter
        if (value == null)
        {
            return value;
        }
        else if (serialised && value instanceof Serializable)
        {
            // Convert to ByteBuffer and use that
            TypeConverter serialConv;
            if (value instanceof byte[])
            {
                serialConv = typeMgr.getTypeConverterForType(byte[].class, ByteBuffer.class);
            }
            else
            {
                serialConv = typeMgr.getTypeConverterForType(Serializable.class, ByteBuffer.class);
            }

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
            Object datastoreValue = EnumConversionHelper.getStoredValueFromEnum(mmd, role, (Enum)value);
            if (datastoreValue instanceof Number)
            {
                return ((Number)datastoreValue).intValue();
            }
            return datastoreValue;
        }
        else if (value instanceof java.time.LocalDateTime)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(LocalDateTime.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            return ((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant();
        }
        else if (value instanceof java.time.LocalDate)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(LocalDate.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            return value;
        }
        else if (value instanceof java.time.LocalTime)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(LocalTime.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            return value;
        }
        else if (value instanceof java.time.Instant)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(Instant.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            return value;
        }
        else if (value instanceof java.time.YearMonth)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(YearMonth.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            YearMonth ym = (YearMonth)value;
            return LocalDate.of(ym.getYear(), ym.getMonthValue(), 1);
        }
        else if (value instanceof java.time.MonthDay)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(YearMonth.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            MonthDay md = (MonthDay)value;
            return LocalDate.of(0, md.getMonthValue(), md.getDayOfMonth());
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
            return ((Calendar)value).toInstant();
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
            else if (datastoreType.equals("timestamp"))
            {
                // Cassandra expects Instant
                if (value instanceof Timestamp)
                {
                    return ((Timestamp)value).toInstant();
                }
                else if (value instanceof java.sql.Date)
                {
                    return ((java.sql.Date)value).toInstant();
                }
                else if (value instanceof Date)
                {
                    return ((Date)value).toInstant();
                }
            }
            else if (datastoreType.equals("time"))
            {
                // Cassandra expects LocalTime
                if (value instanceof java.sql.Time)
                {
                    return ((java.sql.Time)value).toLocalTime();
                }
            }
            else if (datastoreType.equals("date"))
            {
                // Cassandra expects LocalDate
                if (value instanceof java.sql.Date)
                {
                    return ((java.sql.Date)value).toLocalDate();
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
        else if (value instanceof UUID)
        {
            if (datastoreType.equals("varchar"))
            {
                TypeConverter stringConverter = typeMgr.getTypeConverterForType(UUID.class, String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toDatastoreType(value);
                }
            }
            return value;
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
    public static Object getPojoForRowForCandidate(Row row, AbstractClassMetaData cmd, ExecutionContext ec, int[] fpMembers,
            boolean ignoreCache)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Determine the class from the discriminator property
            // String discrimColName =
            // ec.getStoreManager().getNamingFactory().getColumnName(cmd,
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

    private static Object getObjectUsingApplicationIdForRow(final Row row, final AbstractClassMetaData cmd, final ExecutionContext ec,
            boolean ignoreCache, final int[] fpMembers)
    {
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd, table);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, new FieldValues()
        {
            public void fetchFields(ObjectProvider sm)
            {
                sm.replaceFields(fpMembers, fm);
            }

            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                sm.replaceNonLoadedFields(fpMembers, fm);
            }

            public FetchPlan getFetchPlanForLoading()
            {
                return null;
            }
        }, type, ignoreCache, false);
        ObjectProvider sm = ec.findObjectProvider(pc);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                if (table.getSurrogateColumn(SurrogateColumnType.VERSION).getTypeName().equals("int"))
                {
                    version = row.getInt(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                else
                {
                    version = row.getLong(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
            }
            sm.setVersion(version);
        }

        // Any fields loaded above will not be wrapped since we did not have StateManager at the point of creating the FetchFieldManager, so wrap them now
        sm.replaceAllLoadedSCOFieldsWithWrappers();

        return pc;
    }

    private static Object getObjectUsingDatastoreIdForRow(final Row row, final AbstractClassMetaData cmd, final ExecutionContext ec,
            boolean ignoreCache, final int[] fpMembers)
    {
        Object idKey = null;
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        if (ec.getStoreManager().isValueGenerationStrategyDatastoreAttributed(cmd, -1))
        {
            // TODO Support this?
        }
        else
        {
            Column col = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID);
            if (col.getTypeName().equals("varchar"))
            {
                idKey = row.getString(col.getName());
            }
            else
            {
                idKey = row.getLong(col.getName());
            }
        }
        Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);

        final FetchFieldManager fm = new FetchFieldManager(ec, row, cmd, table); // TODO Use the constructor taking op, so we can wrap all SCOs
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, new FieldValues()
        {
            // ObjectProvider calls the fetchFields method
            public void fetchFields(ObjectProvider sm)
            {
                sm.replaceFields(fpMembers, fm);
            }

            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                sm.replaceNonLoadedFields(fpMembers, fm);
            }

            public FetchPlan getFetchPlanForLoading()
            {
                return null;
            }
        }, type, ignoreCache, false);
        ObjectProvider sm = ec.findObjectProvider(pc);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                if (table.getSurrogateColumn(SurrogateColumnType.VERSION).getTypeName().equals("int"))
                {
                    version = row.getInt(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                else
                {
                    version = row.getLong(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
            }
            sm.setVersion(version);
        }

        // Any fields loaded above will not be wrapped since we did not have StateManager at the point of creating the FetchFieldManager, so wrap them now
        sm.replaceAllLoadedSCOFieldsWithWrappers();

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

    /**
     * Convenience method to generate a ResultClassInfo which holds members that would be used by
     * QueryUtils.createResultObjectUsingDefaultConstructorAndSetters method
     * @param resultClazz Class type of result class.
     * @param columnDefinitions Cassandra result column definitions.
     * @return ResultClassPojo
     */
    public static ResultClassInfo getResultClassInfoFromColumnDefinitions(final Class resultClazz, final ColumnDefinitions columnDefinitions)
    {
        Field[] resultClassDeclaredFields = resultClazz.getDeclaredFields();
        assert null != columnDefinitions; // TODO Remove this and throw exception
        Map<Integer, Field> resultClassFields = new TreeMap<>();
        Map<Integer, String> resultClassFieldNames = new TreeMap<>();
        List<Integer> fieldsMatchingColumnIndexes = new ArrayList<>();

        for (Field field : resultClassDeclaredFields)
        {
            String fieldNameLower = field.getName().toLowerCase();
            if (columnDefinitions.contains(fieldNameLower))
            {
                int columnIndex = columnDefinitions.firstIndexOf(fieldNameLower);
                resultClassFields.put(columnIndex, field);
                resultClassFieldNames.put(columnIndex, field.getName());
                fieldsMatchingColumnIndexes.add(columnIndex);
            }
            else
            {
                // if field name not matching it maybe the column name.
                Annotation[] annotations = field.getDeclaredAnnotations();
                for (Annotation annotation : annotations)
                {
                    String annotationString = annotation.toString();
                    if (annotationString.contains("Column"))
                    {
                        int startIndex = annotationString.indexOf("name=") + "name=".length();
                        int columnNameLength = annotationString.substring(startIndex).indexOf(",");
                        if (-1 < startIndex)
                        {
                            assert columnNameLength > 0;
                            String columnName = annotationString.substring(startIndex, startIndex + columnNameLength);
                            int columnIndex = columnDefinitions.firstIndexOf(columnName);
                            resultClassFields.put(columnIndex, field);
                            resultClassFieldNames.put(columnIndex, field.getName());
                            fieldsMatchingColumnIndexes.add(columnIndex);
                            break;
                        }
                    }
                }
            }
        }
        return new ResultClassInfo(resultClassFields.values().toArray(new Field[0]), resultClassFieldNames.values().toArray(new String[0]), fieldsMatchingColumnIndexes);
    }

    /**
     * Convenience method to get Object[] from Cassandra Row.
     * @param typeMgr TypeManager
     * @param row Row returned from Cassandra driver
     * @param columnDefinitions Cassandra result column definitions.
     * @param fieldsMatchingColumnIndexes indices of ColumnDefinitions that match to a field of resultClass
     * @param resultRowSize size of Object [] that is returned
     * @return Object[] of results
     */
    public static Object[] getObjectArrayFromRow(TypeManager typeMgr, Row row, ColumnDefinitions columnDefinitions, List<Integer> fieldsMatchingColumnIndexes, int resultRowSize)
    {
        Object[] resultRow = new Object[resultRowSize];
        int resultRowNum = 0;
        for (int i = 0;i < columnDefinitions.size(); i++)
        {
            ColumnDefinition def = columnDefinitions.get(i);

            if (fieldsMatchingColumnIndexes.isEmpty() || fieldsMatchingColumnIndexes.contains(columnDefinitions.firstIndexOf(def.getName())))
            {
                DataType colType = def.getType();
                if (colType == DataTypes.TEXT)
                {
                    resultRow[resultRowNum] = row.getString(i);
                }
                else if (colType == DataTypes.BIGINT)
                {
                    resultRow[resultRowNum] = row.getLong(i);
                }
                else if (colType == DataTypes.DECIMAL)
                {
                    resultRow[resultRowNum] = row.getBigDecimal(i);
                }
                else if (colType == DataTypes.FLOAT)
                {
                    resultRow[resultRowNum] = row.getFloat(i);
                }
                else if (colType == DataTypes.DOUBLE)
                {
                    resultRow[resultRowNum] = row.getDouble(i);
                }
                else if (colType == DataTypes.BOOLEAN)
                {
                    resultRow[resultRowNum] = row.getBoolean(i);
                }
                else if (colType == DataTypes.TIMESTAMP)
                {
                    resultRow[resultRowNum] = Timestamp.from(row.getInstant(i));
                }
                else if (colType == DataTypes.TIME)
                {
                    resultRow[resultRowNum] = Time.valueOf(row.getLocalTime(i));
                }
                else if (colType == DataTypes.DATE)
                {
                    resultRow[resultRowNum] = new Date(java.util.Date.from(row.getLocalDate(i).atStartOfDay(ZoneId.systemDefault()).toInstant()).getTime());
                }
                else if (colType == DataTypes.INT)
                {
                    resultRow[resultRowNum] = row.getInt(i);
                }
                else if (colType == DataTypes.SMALLINT)
                {
                    resultRow[resultRowNum] = row.getShort(i);
                }
                else if (colType == DataTypes.TINYINT)
                {
                    resultRow[resultRowNum] = row.getShort(i);
                }
                else if (colType == DataTypes.BLOB)
                {
                    TypeConverter typeConverter = typeMgr.getTypeConverterForType(byte[].class, ByteBuffer.class);
                    resultRow[resultRowNum] = typeConverter.toMemberType(row.getByteBuffer(i));
                }
                else if (colType == DataTypes.UUID)
                {
                    resultRow[resultRowNum] = row.getUuid(i);
                }
                else
                {
                    NucleusLogger.QUERY.warn("Column " + i + " of results is of unsupported type (" + colType + ") : returning null");
                    resultRow[resultRowNum] = null;
                }
                resultRowNum++;
            }
        }
        return resultRow;
    }
}