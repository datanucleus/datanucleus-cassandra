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
package org.datanucleus.store.cassandra.fieldmanager;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;

import com.datastax.driver.core.Row;

/**
 * FieldManager to use for retrieving values from Cassandra to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected Table table;

    protected Row row;

    public FetchFieldManager(ObjectProvider op, Row row, Table table)
    {
        super(op);
        this.table = table;
        this.row = row;
    }

    public FetchFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd, Table table)
    {
        super(ec, cmd);
        this.table = table;
        this.row = row;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        return row.getBool(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        return row.getString(getColumnMapping(fieldNumber).getColumn(0).getName()).charAt(0);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        return (byte) row.getInt(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        return (short) row.getInt(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        return row.getInt(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        return row.getLong(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        if (col.getJdbcType() == JdbcType.DECIMAL)
        {
            return row.getDecimal(col.getName()).floatValue();
        }
        else if (col.getJdbcType() == JdbcType.DOUBLE)
        {
            return (float) row.getDouble(col.getName());
        }
        return row.getFloat(col.getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        if (col.getJdbcType() == JdbcType.DECIMAL)
        {
            return row.getDecimal(col.getName()).doubleValue();
        }
        return row.getDouble(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        return row.getString(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Null detection
                List<AbstractMemberMetaData> embMmds = new ArrayList<>();
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, row, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Embedded Collection
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded)");
                return null; // Remove this when we support embedded
            }
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        boolean optional = false;
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }
            optional = true;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (row.isNull(mapping.getColumn(0).getName()))
            {
                return optional ? Optional.empty() : null;
            }

            if (mmd.isSerialized())
            {
                // Convert back from ByteBuffer
                TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                ByteBuffer datastoreBuffer = row.getBytes(mapping.getColumn(0).getName());
                Object value = serialConv.toMemberType(datastoreBuffer);

                // Make sure it has an ObjectProvider
                ObjectProvider pcOP = ec.findObjectProvider(value);
                if (pcOP == null || ec.getApiAdapter().getExecutionContext(value) == null)
                {
                    ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, value, false, op, fieldNumber);
                }

                return value;
            }

            // TODO Support 1-1 storage using "FK" style column(s) for related object
            Object value = row.getString(mapping.getColumn(0).getName());
            Object memberValue = getValueForSingleRelationField(mmd, value, clr);
            return optional ? Optional.of(memberValue) : memberValue;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            Object value = null;
            if (mmd.hasCollection())
            {
                Class elementCls = mmd.getCollection().isSerializedElement() ? ByteBuffer.class : String.class;
                if (Set.class.isAssignableFrom(mmd.getType()))
                {
                    value = row.getSet(mapping.getColumn(0).getName(), elementCls);
                }
                else if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                {
                    value = row.getList(mapping.getColumn(0).getName(), elementCls);
                }
                else
                {
                    value = row.getSet(mapping.getColumn(0).getName(), elementCls);
                }
            }
            else if (mmd.hasMap())
            {
                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                if (mmd.getMap().keyIsPersistent())
                {
                    keyCls = mmd.getMap().isSerializedKey() ? ByteBuffer.class : String.class;
                }
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                if (mmd.getMap().valueIsPersistent())
                {
                    valCls = mmd.getMap().isSerializedValue() ? ByteBuffer.class : String.class;
                }
                value = row.getMap(mapping.getColumn(0).getName(), keyCls, valCls);
            }
            else if (mmd.hasArray())
            {
                value = row.getList(mapping.getColumn(0).getName(), String.class);
            }
            return getValueForContainerRelationField(mmd, value, clr);
        }
        else
        {
            if (mapping.getTypeConverter() != null && !mmd.isSerialized())
            {
                // Convert any columns that have a converter defined back to the member type with the converter
                if (mapping.getNumberOfColumns() > 1)
                {
                    // Check for null member
                    boolean allNull = true;
                    for (int i = 0; i < mapping.getNumberOfColumns(); i++)
                    {
                        if (!row.isNull(mapping.getColumn(i).getName()))
                        {
                            allNull = false;
                        }
                    }
                    if (allNull)
                    {
                        return optional ? Optional.empty() : null;
                    }

                    Object valuesArr = null;
                    Class[] colTypes = ((MultiColumnConverter) mapping.getTypeConverter()).getDatastoreColumnTypes();
                    if (colTypes[0] == int.class)
                    {
                        valuesArr = new int[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == long.class)
                    {
                        valuesArr = new long[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == double.class)
                    {
                        valuesArr = new double[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == float.class)
                    {
                        valuesArr = new double[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == String.class)
                    {
                        valuesArr = new String[mapping.getNumberOfColumns()];
                    }
                    else
                    {
                        valuesArr = new Object[mapping.getNumberOfColumns()];
                    }
                    boolean isNull = true;
                    for (int i = 0; i < mapping.getNumberOfColumns(); i++)
                    {
                        Column col = mapping.getColumn(i);
                        if (col.getTypeName().equals("int"))
                        {
                            Array.set(valuesArr, i, row.getInt(col.getName()));
                        }
                        else if (col.getTypeName().equals("bool"))
                        {
                            Array.set(valuesArr, i, row.getBool(col.getName()));
                        }
                        else if (col.getTypeName().equals("timestamp"))
                        {
                            Array.set(valuesArr, i, row.getTimestamp(col.getName()));
                        }
                        else if (col.getTypeName().equals("decimal"))
                        {
                            Array.set(valuesArr, i, row.getDecimal(col.getName()));
                        }
                        else if (col.getTypeName().equals("double"))
                        {
                            Array.set(valuesArr, i, row.getDouble(col.getName()));
                        }
                        else if (col.getTypeName().equals("float"))
                        {
                            Array.set(valuesArr, i, row.getFloat(col.getName()));
                        }
                        else if (col.getTypeName().equals("bigint"))
                        {
                            Array.set(valuesArr, i, row.getLong(col.getName()));
                        }
                        else
                        {
                            Array.set(valuesArr, i, row.getString(col.getName()));
                        }
                        // TODO Support other types
                        if (isNull && Array.get(valuesArr, i) != null)
                        {
                            isNull = false;
                        }
                    }
                    if (isNull)
                    {
                        return null;
                    }
                    return mapping.getTypeConverter().toMemberType(valuesArr);
                }

                if (row.isNull(mapping.getColumn(0).getName()))
                {
                    return optional ? Optional.empty() : null;
                }

                // Obtain value using converter
                Object returnValue = CassandraUtils.getMemberValueForColumnWithConverter(row, mapping.getColumn(0), mapping.getTypeConverter());
                if (op != null)
                {
                    returnValue = SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
                }
                return returnValue;
            }

            if (!optional && mmd.hasCollection())
            {
                if (mmd.isSerialized())
                {
                    // Collection field was serialised, so convert back from ByteBuffer
                    TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                    ByteBuffer datastoreBuffer = row.getBytes(mapping.getColumn(0).getName());
                    if (datastoreBuffer == null)
                    {
                        return null;
                    }
                    Object returnValue = serialConv.toMemberType(datastoreBuffer);
                    returnValue = (op!=null) ? (Collection) SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true) : returnValue;
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                if (!row.isNull(mapping.getColumn(0).getName()))
                {
                    TypeConverter elemConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_COLLECTION_ELEMENT);

                    Class elemCls = clr.classForName(mmd.getCollection().getElementType());
                    String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null, mmd, FieldRole.ROLE_COLLECTION_ELEMENT, clr);
                    Class cassElemCls = CassandraUtils.getJavaTypeForCassandraType(elemCassType);

                    Collection cassColl = null;
                    if (Set.class.isAssignableFrom(mmd.getType()))
                    {
                        cassColl = row.getSet(mapping.getColumn(0).getName(), cassElemCls);
                    }
                    else if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                    {
                        cassColl = row.getList(mapping.getColumn(0).getName(), cassElemCls);
                    }
                    else
                    {
                        cassColl = row.getSet(mapping.getColumn(0).getName(), cassElemCls);
                    }

                    if (cassColl != null)
                    {
                        Iterator cassCollIter = cassColl.iterator();
                        while (cassCollIter.hasNext())
                        {
                            Object cassElem = cassCollIter.next();
                            Object elem = null;
                            if (elemConv != null)
                            {
                                elem = elemConv.toMemberType(cassElem);
                            }
                            else
                            {
                                elem = CassandraUtils.getJavaValueForDatastoreValue(cassElem, elemCassType, elemCls, ec);
                            }
                            coll.add(elem);
                        }
                    }
                }

                if (op != null)
                {
                    // Wrap if SCO
                    coll = (Collection) SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
                }
                return coll;
            }
            else if (!optional && mmd.hasMap())
            {
                if (mmd.isSerialized())
                {
                    // Map field was serialised, so convert back from ByteBuffer
                    TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                    ByteBuffer datastoreBuffer = row.getBytes(mapping.getColumn(0).getName());
                    if (datastoreBuffer == null)
                    {
                        return null;
                    }
                    Object returnValue = serialConv.toMemberType(datastoreBuffer);
                    returnValue = (op!=null) ? SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true) : returnValue;
                }

                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                    map = (Map) instanceType.getDeclaredConstructor().newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                if (!row.isNull(mapping.getColumn(0).getName()))
                {
                    TypeConverter keyConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_KEY);
                    TypeConverter valConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_VALUE);

                    Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                    String keyCassType = CassandraUtils.getCassandraTypeForNonPersistableType(keyCls, false, ec.getTypeManager(), null, mmd, FieldRole.ROLE_MAP_KEY, clr);
                    Class cassKeyCls = CassandraUtils.getJavaTypeForCassandraType(keyCassType);
                    Class valCls = clr.classForName(mmd.getMap().getValueType());
                    String valCassType = CassandraUtils.getCassandraTypeForNonPersistableType(valCls, false, ec.getTypeManager(), null, mmd, FieldRole.ROLE_MAP_VALUE, clr);
                    Class cassValCls = CassandraUtils.getJavaTypeForCassandraType(valCassType);

                    Map cassMap = row.getMap(mapping.getColumn(0).getName(), cassKeyCls, cassValCls);
                    if (cassMap != null)
                    {
                        Iterator<Map.Entry> cassMapEntryIter = cassMap.entrySet().iterator();
                        while (cassMapEntryIter.hasNext())
                        {
                            Map.Entry cassMapEntry = cassMapEntryIter.next();
                            Object key = null;
                            if (keyConv != null)
                            {
                                key = keyConv.toMemberType(cassMapEntry.getKey());
                            }
                            else
                            {
                                key = CassandraUtils.getJavaValueForDatastoreValue(cassMapEntry.getKey(), cassKeyCls.getName(), keyCls, ec);
                            }

                            Object val = null;
                            if (valConv != null)
                            {
                                val = valConv.toMemberType(cassMapEntry.getValue());
                            }
                            else
                            {
                                val = CassandraUtils.getJavaValueForDatastoreValue(cassMapEntry.getValue(), cassValCls.getName(), valCls, ec);
                            }

                            map.put(key, val);
                        }
                    }
                }

                if (op != null)
                {
                    // Wrap if SCO
                    map = (Map) SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
                }
                return map;
            }
            else if (!optional && mmd.hasArray())
            {
                if (row.isNull(mapping.getColumn(0).getName()))
                {
                    return null;
                }

                if (mmd.isSerialized())
                {
                    // Convert back from ByteBuffer
                    TypeConverter serialConv = null;
                    if (mmd.getType() == byte[].class)
                    {
                        serialConv = ec.getTypeManager().getTypeConverterForType(byte[].class, ByteBuffer.class);
                    }
                    else
                    {
                        serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                    }
                    ByteBuffer datastoreBuffer = row.getBytes(mapping.getColumn(0).getName());
                    return serialConv.toMemberType(datastoreBuffer);
                }

                NucleusLogger.DATASTORE_RETRIEVE.warn("Field=" + mmd.getFullFieldName() + " has datastore array; not supported yet");
                // Class elemCls = clr.classForName(mmd.getArray().getElementType());
                // String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
                // Class cassElemCls = CassandraUtils.getJavaTypeForCassandraType(elemCassType);
                /*
                 * List cassColl = row.getList(colName, cassElemCls); Object array =
                 * Array.newInstance(mmd.getType().getComponentType(), cassColl.size()); int i=0; for (Object
                 * cassElem : cassColl) { Object elem = CassandraUtils.getJavaValueForDatastoreValue(cassElem,
                 * elemCassType, elemCls, ec); Array.set(array, i++, elem); } return array;
                 */
                return null;
            }

            // Check for null member
            if (row.isNull(mapping.getColumn(0).getName()))
            {
                return optional ? Optional.empty() : null;
            }

            Class type = mmd.getType();
            if (optional)
            {
                type = clr.classForName(mmd.getCollection().getElementType());
            }

            Object value = null;
            if (mmd.isSerialized())
            {
                // Convert back from ByteBuffer
                TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                ByteBuffer datastoreBuffer = row.getBytes(mapping.getColumn(0).getName());
                value = serialConv.toMemberType(datastoreBuffer);
            }
            // TODO Fields below here likely have TypeConverter defined, so maybe could omit this block
            else if (BigInteger.class.isAssignableFrom(type))
            {
                // TODO There is a TypeConverter for this
                value = BigInteger.valueOf(row.getLong(mapping.getColumn(0).getName()));
            }
            else if (BigDecimal.class.isAssignableFrom(type))
            {
                // TODO There is a TypeConverter for this
                value = row.getDecimal(mapping.getColumn(0).getName());
            }
            else if (Byte.class.isAssignableFrom(type))
            {
                value = (byte) row.getInt(mapping.getColumn(0).getName());
            }
            else if (String.class.isAssignableFrom(type))
            {
                value = row.getString(mapping.getColumn(0).getName());
            }
            else if (Character.class.isAssignableFrom(type))
            {
                value = row.getString(mapping.getColumn(0).getName()).charAt(0);
            }
            else if (Double.class.isAssignableFrom(type))
            {
                value = row.getDouble(mapping.getColumn(0).getName());
            }
            else if (Float.class.isAssignableFrom(type))
            {
                value = row.getFloat(mapping.getColumn(0).getName());
            }
            else if (Long.class.isAssignableFrom(type))
            {
                value = row.getLong(mapping.getColumn(0).getName());
            }
            else if (Integer.class.isAssignableFrom(type))
            {
                value = row.getInt(mapping.getColumn(0).getName());
            }
            else if (Short.class.isAssignableFrom(type))
            {
                value = (short) row.getInt(mapping.getColumn(0).getName());
            }
            else if (Boolean.class.isAssignableFrom(type))
            {
                value = row.getBool(mapping.getColumn(0).getName());
            }
            else if (Enum.class.isAssignableFrom(type))
            {
                JdbcType jdbcType = TypeConversionHelper.getJdbcTypeForEnum(mmd, FieldRole.ROLE_FIELD, clr);
                Object datastoreValue = (MetaDataUtils.isJdbcTypeNumeric(jdbcType)) ? row.getInt(mapping.getColumn(0).getName()) : row.getString(mapping.getColumn(0).getName());
                value = TypeConversionHelper.getEnumForStoredValue(mmd, FieldRole.ROLE_FIELD, datastoreValue, clr);
            }
            else if (java.sql.Date.class.isAssignableFrom(type))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                    if (stringConverter != null)
                    {
                        value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                    }
                }
                else
                {
                    // TODO There is a TypeConverter for this
                    value = new java.sql.Date(row.getTimestamp(mapping.getColumn(0).getName()).getTime());
                }
            }
            else if (java.sql.Time.class.isAssignableFrom(type))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                    if (stringConverter != null)
                    {
                        value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                    }
                }
                else
                {
                    // TODO There is a TypeConverter for this
                    value = new java.sql.Time(row.getTimestamp(mapping.getColumn(0).getName()).getTime());
                }
            }
            else if (java.sql.Timestamp.class.isAssignableFrom(type))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                    if (stringConverter != null)
                    {
                        value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                    }
                }
                else
                {
                    // TODO There is a TypeConverter for this
                    value = new java.sql.Timestamp(row.getTimestamp(mapping.getColumn(0).getName()).getTime());
                }
            }
            else if (Calendar.class.isAssignableFrom(type))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                    if (stringConverter != null)
                    {
                        value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                    }
                }
                else
                {
                    // TODO Support Calendar with multiple columns and do via TypeConverter
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(row.getTimestamp(mapping.getColumn(0).getName()));
                    value = cal;
                }
            }
            else if (Date.class.isAssignableFrom(type))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                    if (stringConverter != null)
                    {
                        value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                    }
                }
                else
                {
                    value = new java.util.Date(row.getTimestamp(mapping.getColumn(0).getName()).getTime());
                }
            }
            else if (UUID.class.isAssignableFrom(type))
            {
                // uuid is the default type
                value = row.getUUID(mapping.getColumn(0).getName());
            }
            else
            {
                // TODO Support multi-column converters
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(type, String.class);
                if (stringConverter != null)
                {
                    value = stringConverter.toMemberType(row.getString(mapping.getColumn(0).getName()));
                }
                else
                {
                    TypeConverter longConverter = ec.getTypeManager().getTypeConverterForType(type, Long.class);
                    if (longConverter != null)
                    {
                        value = longConverter.toMemberType(row.getLong(mapping.getColumn(0).getName()));
                    }
                }
            }

            return optional ? Optional.of(value) : value;
        }
    }

    protected Object getValueForSingleRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        if (value == null)
        {
            return null;
        }

        Class type = mmd.getType();
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            type = clr.classForName(mmd.getCollection().getElementType());
        }

        String persistableId = (String) value;
        try
        {
            AbstractClassMetaData mmdCmd = ec.getMetaDataManager().getMetaDataForClass(type, clr);
            if (mmdCmd != null)
            {
                return IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
            }

            String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_FIELD, clr, ec.getMetaDataManager());
            if (implNames != null && implNames.length == 1)
            {
                // Only one possible implementation, so use that
                mmdCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                return IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
            }
            else if (implNames != null && implNames.length > 1)
            {
                // Multiple implementations, so try each implementation in turn (note we only need this if
                // some impls have different "identity" type from each other)
                for (String implName : implNames)
                {
                    try
                    {
                        mmdCmd = ec.getMetaDataManager().getMetaDataForClass(implName, clr);
                        return IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
                    }
                    catch (NucleusObjectNotFoundException nonfe)
                    {
                        // Object no longer present in the datastore, must have been deleted
                        throw nonfe;
                    }
                    catch (Exception e)
                    {
                        // Not possible with this implementation
                    }
                }
            }

            throw new NucleusUserException(
                    "We do not currently support the field type of " + mmd.getFullFieldName() + " which has an interdeterminate type (e.g interface or Object element types)");
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            NucleusLogger.PERSISTENCE.warn("Object=" + op + " field=" + mmd.getFullFieldName() + " has id=" + persistableId + " but could not instantiate object with that identity");
            return null;
        }
    }

    protected Object getValueForContainerRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        if (mmd.hasCollection())
        {
            // "a,b,c,d,..."
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length > 0)
                {
                    // Just use first implementation TODO What if the impls have different id type?
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException(
                            "We do not currently support the field type of " + mmd.getFullFieldName() + " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            // TODO Support serialised element which will be of type ByteBuffer
            Collection<String> collIds = (Collection<String>) value;
            Iterator<String> idIter = collIds.iterator();
            boolean changeDetected = false;
            while (idIter.hasNext())
            {
                String persistableId = idIter.next();
                if (persistableId.equals("NULL"))
                {
                    // Null element
                    coll.add(null);
                }
                else
                {
                    try
                    {
                        coll.add(IdentityUtils.getObjectFromPersistableIdentity(persistableId, elemCmd, ec));
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                        // Object no longer exists. Deleted by user? so ignore
                        changeDetected = true;
                    }
                }
            }

            if (coll instanceof List && mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null && !mmd.getOrderMetaData().getOrdering().equals("#PK"))
            {
                // Reorder the collection as per the ordering clause
                Collection newColl = QueryUtils.orderCandidates((List)coll, clr.classForName(mmd.getCollection().getElementType()), mmd.getOrderMetaData().getOrdering(), ec, clr);
                if (newColl.getClass() != coll.getClass())
                {
                    // Type has changed, so just reuse the input
                    coll.clear();
                    coll.addAll(newColl);
                }
            }

            if (op != null)
            {
                // Wrap if SCO
                coll = (Collection) SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
                if (changeDetected)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return coll;
        }
        else if (mmd.hasMap())
        {
            // Map<?,?>
            Map map;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                map = (Map) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            Map mapIds = (Map) value;
            AbstractClassMetaData keyCmd = null;
            if (mmd.getMap().keyIsPersistent())
            {
                keyCmd = mmd.getMap().getKeyClassMetaData(clr);
                if (keyCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_MAP_KEY, clr,
                        ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        keyCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (keyCmd == null)
                    {
                        throw new NucleusUserException(
                                "We do not currently support the field type of " + mmd.getFullFieldName() + " which has a map of interdeterminate key type (e.g interface or Object element types)");
                    }
                }
            }
            AbstractClassMetaData valCmd = null;
            if (mmd.getMap().valueIsPersistent())
            {
                valCmd = mmd.getMap().getValueClassMetaData(clr);
                if (valCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_MAP_VALUE, clr,
                        ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        valCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (valCmd == null)
                    {
                        throw new NucleusUserException(
                                "We do not currently support the field type of " + mmd.getFullFieldName() + " which has a map of interdeterminate value type (e.g interface or Object element types)");
                    }
                }
            }

            Iterator<Map.Entry> entryIter = mapIds.entrySet().iterator();
            boolean changeDetected = false;
            while (entryIter.hasNext())
            {
                Map.Entry entry = entryIter.next();
                Object key = null;
                Object val = null;
                boolean keySet = true;
                boolean valSet = true;
                if (mmd.getMap().keyIsPersistent())
                {
                    // TODO Support serialised key which will be of type ByteBuffer
                    String keyPersistableId = (String) entry.getKey();
                    try
                    {
                        key = IdentityUtils.getObjectFromPersistableIdentity(keyPersistableId, keyCmd, ec);
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                        // Object no longer exists. Deleted by user? so ignore
                        changeDetected = true;
                        keySet = false;
                    }
                }
                else
                {
                    // TODO Extract (non-persistable) map key
                    key = entry.getKey();
                }
                if (mmd.getMap().valueIsPersistent())
                {
                    // TODO Support serialised value which will be of type ByteBuffer
                    String valPersistableId = (String) entry.getValue();
                    if (valPersistableId.equals("NULL"))
                    {
                    }
                    else
                    {
                        try
                        {
                            val = IdentityUtils.getObjectFromPersistableIdentity(valPersistableId, valCmd, ec);
                        }
                        catch (NucleusObjectNotFoundException onfe)
                        {
                            // Object no longer exists. Deleted by user? so ignore
                            changeDetected = true;
                            valSet = false;
                        }
                    }
                }
                else
                {
                    // TODO Extract (non-persistable) map value
                    val = entry.getValue();
                }

                if (keySet && valSet)
                {
                    map.put(key, val);
                }
            }

            if (op != null)
            {
                // Wrap if SCO
                map = (Map) SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
                if (changeDetected)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return map;
        }
        else if (mmd.hasArray())
        {
            // "a,b,c,d,..."
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_ARRAY_ELEMENT, clr,
                    ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException(
                            "We do not currently support the field type of " + mmd.getFullFieldName() + " which has an array of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            Collection<String> collIds = (Collection<String>) value;
            Object array = Array.newInstance(mmd.getType().getComponentType(), collIds.size());
            Iterator<String> idIter = collIds.iterator();
            boolean changeDetected = false;
            int pos = 0;
            while (idIter.hasNext())
            {
                String persistableId = idIter.next();
                if (persistableId.equals("NULL"))
                {
                    // Null element
                    Array.set(array, pos++, null);
                }
                else
                {
                    try
                    {
                        Array.set(array, pos++, IdentityUtils.getObjectFromPersistableIdentity(persistableId, elemCmd, ec));
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                        // Object no longer exists. Deleted by user? so ignore
                        changeDetected = true;
                    }
                }
            }

            if (changeDetected)
            {
                if (pos < collIds.size())
                {
                    // Some elements not found, so resize the array
                    Object arrayOld = array;
                    array = Array.newInstance(mmd.getType().getComponentType(), pos);
                    for (int j = 0; j < pos; j++)
                    {
                        Array.set(array, j, Array.get(arrayOld, j));
                    }
                }
                if (op != null)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return array;
        }
        else
        {
            return value;
        }
    }
}