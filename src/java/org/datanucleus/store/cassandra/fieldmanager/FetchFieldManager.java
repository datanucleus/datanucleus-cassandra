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
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        return row.getBool(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        return row.getString(getColumnMapping(fieldNumber).getColumn(0).getIdentifier()).charAt(0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        return (byte) row.getInt(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        return (short) row.getInt(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        return row.getInt(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        return row.getLong(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        return row.getFloat(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        return row.getDouble(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        return row.getString(getColumnMapping(fieldNumber).getColumn(0).getIdentifier());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE)
        {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // TODO Null detection
                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                    embMmds.add(mmd);
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
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
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (row.isNull(mapping.getColumn(0).getIdentifier()))
            {
                return null;
            }

            Object value = row.getString(mapping.getColumn(0).getIdentifier());
            return getValueForSingleRelationField(mmd, value, clr);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            Object value = null;
            if (mmd.hasCollection())
            {
                Class elementCls = mmd.getCollection().isSerializedElement() ? ByteBuffer.class : String.class;
                if (Set.class.isAssignableFrom(mmd.getType()))
                {
                    value = row.getSet(mapping.getColumn(0).getIdentifier(), elementCls);
                }
                else if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                {
                    value = row.getList(mapping.getColumn(0).getIdentifier(), elementCls);
                }
                else
                {
                    value = row.getSet(mapping.getColumn(0).getIdentifier(), elementCls);
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
                value = row.getMap(mapping.getColumn(0).getIdentifier(), keyCls, valCls);
            }
            else if (mmd.hasArray())
            {
                value = row.getList(mapping.getColumn(0).getIdentifier(), String.class);
            }
            return getValueForContainerRelationField(mmd, value, clr);
        }
        else
        {
            // Check for null member
            if (mapping.getNumberOfColumns() > 1)
            {
                boolean allNull = true;
                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    if (!row.isNull(mapping.getColumn(i).getIdentifier()))
                    {
                        allNull = false;
                    }
                }
                if (allNull)
                {
                    return null;
                }
            }
            else
            {
                if (row.isNull(mapping.getColumn(0).getIdentifier()))
                {
                    return null;
                }
            }

            if (mapping.getTypeConverter() != null && !mmd.isSerialized())
            {
                // Convert any columns that have a converter defined back to the member type with the converter
                if (mapping.getNumberOfColumns() > 1)
                {
                    Object valuesArr = null;
                    Class[] colTypes = ((MultiColumnConverter)mapping.getTypeConverter()).getDatastoreColumnTypes();
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
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        Column col = mapping.getColumn(i);
                        if (col.getTypeName().equals("int"))
                        {
                            Array.set(valuesArr, i, row.getInt(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("bool"))
                        {
                            Array.set(valuesArr, i, row.getBool(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("timestamp"))
                        {
                            Array.set(valuesArr, i, row.getDate(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("decimal"))
                        {
                            Array.set(valuesArr, i, row.getDecimal(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("double"))
                        {
                            Array.set(valuesArr, i, row.getDouble(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("float"))
                        {
                            Array.set(valuesArr, i, row.getFloat(col.getIdentifier()));
                        }
                        else if (col.getTypeName().equals("bigint"))
                        {
                            Array.set(valuesArr, i, row.getLong(col.getIdentifier()));
                        }
                        else
                        {
                            Array.set(valuesArr, i, row.getString(col.getIdentifier()));
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
                else
                {
                    return CassandraUtils.getMemberValueForColumnWithConverter(row, mapping.getColumn(0), mapping.getTypeConverter());
                }
            }

            if (mmd.hasCollection())
            {
                // TODO Cater for serialised Collection field
                Collection cassColl = null;
                Class elemCls = clr.classForName(mmd.getCollection().getElementType());
                String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
                Class cassElemCls = CassandraUtils.getJavaTypeForCassandraType(elemCassType);
                // TODO Cater for type conversion, and update elementCls to the Cassandra type
                if (Set.class.isAssignableFrom(mmd.getType()))
                {
                    cassColl = row.getSet(mapping.getColumn(0).getIdentifier(), cassElemCls);
                }
                else if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                {
                    cassColl = row.getList(mapping.getColumn(0).getIdentifier(), cassElemCls);
                }
                else
                {
                    cassColl = row.getSet(mapping.getColumn(0).getIdentifier(), cassElemCls);
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                if (cassColl != null)
                {
                    Iterator cassCollIter = cassColl.iterator();
                    while (cassCollIter.hasNext())
                    {
                        Object cassElem = cassCollIter.next();
                        Object elem = CassandraUtils.getJavaValueForDatastoreValue(cassElem, elemCassType, elemCls, ec);
                        coll.add(elem);
                    }
                }
                if (op != null)
                {
                    // Wrap if SCO
                    coll = (Collection) op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false, false, true);
                }
                return coll;
            }
            else if (mmd.hasMap())
            {
                // TODO Cater for serialised Map field
                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                    map = (Map) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                String keyCassType = CassandraUtils.getCassandraTypeForNonPersistableType(keyCls, false, ec.getTypeManager(), null);
                Class cassKeyCls = CassandraUtils.getJavaTypeForCassandraType(keyCassType);
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                String valCassType = CassandraUtils.getCassandraTypeForNonPersistableType(valCls, false, ec.getTypeManager(), null);
                Class cassValCls = CassandraUtils.getJavaTypeForCassandraType(valCassType);
                Map cassMap = row.getMap(mapping.getColumn(0).getIdentifier(), cassKeyCls, cassValCls);
                if (cassMap != null)
                {
                    Iterator<Map.Entry> cassMapEntryIter = cassMap.entrySet().iterator();
                    while (cassMapEntryIter.hasNext())
                    {
                        Map.Entry cassMapEntry = cassMapEntryIter.next();
                        Object key = CassandraUtils.getJavaValueForDatastoreValue(cassMapEntry.getKey(), cassKeyCls.getName(), keyCls, ec);
                        Object val = CassandraUtils.getJavaValueForDatastoreValue(cassMapEntry.getValue(), cassValCls.getName(), valCls, ec);
                        map.put(key, val);
                    }
                }
                if (op != null)
                {
                    // Wrap if SCO
                    map = (Map) op.wrapSCOField(mmd.getAbsoluteFieldNumber(), map, false, false, true);
                }
                return map;
            }
            else if (mmd.hasArray())
            {
                // TODO Cater for serialised Array field
//                Class elemCls = clr.classForName(mmd.getArray().getElementType());
//                String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
//                Class cassElemCls = CassandraUtils.getJavaTypeForCassandraType(elemCassType);
                NucleusLogger.DATASTORE_RETRIEVE.warn("Field=" + mmd.getFullFieldName() + " has datastore array; not supported yet");
                /*List cassColl = row.getList(colName, cassElemCls);

                Object array = Array.newInstance(mmd.getType().getComponentType(), cassColl.size());
                int i=0;
                for (Object cassElem : cassColl)
                {
                    Object elem = CassandraUtils.getJavaValueForDatastoreValue(cassElem, elemCassType, elemCls, ec);
                    Array.set(array, i++, elem);
                }
                return array;*/
            }
            else if (mmd.isSerialized())
            {
                // Convert back from ByteBuffer
                TypeConverter<Serializable, ByteBuffer> serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                return serialConv.toMemberType(row.getBytes(mapping.getColumn(0).getIdentifier()));
            }
            // TODO Fields below here likely have TypeConverter defined, so maybe could omit this block
            else if (BigInteger.class.isAssignableFrom(mmd.getType()))
            {
            	// TODO There is a TypeConverter for this
            	return BigInteger.valueOf(row.getLong(mapping.getColumn(0).getIdentifier()));
            }
            else if (BigDecimal.class.isAssignableFrom(mmd.getType()))
            {
            	// TODO There is a TypeConverter for this
            	return row.getDecimal(mapping.getColumn(0).getIdentifier());
            }
            else if (Byte.class.isAssignableFrom(mmd.getType()))
            {
                return (byte)row.getInt(mapping.getColumn(0).getIdentifier());
            }
            else if (Character.class.isAssignableFrom(mmd.getType()))
            {
                return row.getString(mapping.getColumn(0).getIdentifier()).charAt(0);
            }
            else if (Double.class.isAssignableFrom(mmd.getType()))
            {
                return row.getDouble(mapping.getColumn(0).getIdentifier());
            }
            else if (Float.class.isAssignableFrom(mmd.getType()))
            {
                return row.getFloat(mapping.getColumn(0).getIdentifier());
            }
            else if (Long.class.isAssignableFrom(mmd.getType()))
            {
                return row.getLong(mapping.getColumn(0).getIdentifier());
            }
            else if (Integer.class.isAssignableFrom(mmd.getType()))
            {
                return row.getInt(mapping.getColumn(0).getIdentifier());
            }
            else if (Short.class.isAssignableFrom(mmd.getType()))
            {
                return (short)row.getInt(mapping.getColumn(0).getIdentifier());
            }
            else if (Boolean.class.isAssignableFrom(mmd.getType()))
            {
                return row.getBool(mapping.getColumn(0).getIdentifier());
            }
            else if (Enum.class.isAssignableFrom(mmd.getType()))
            {
                // Persist as ordinal unless user specifies jdbc-type of "varchar"
            	if (mapping.getColumn(0).getTypeName().equals("varchar"))
            	{
            		return Enum.valueOf(mmd.getType(), row.getString(mapping.getColumn(0).getIdentifier()));
            	}
                return mmd.getType().getEnumConstants()[row.getInt(mapping.getColumn(0).getIdentifier())];
            }
            else if (java.sql.Date.class.isAssignableFrom(mmd.getType()))
            {
            	if (mapping.getColumn(0).getTypeName().equals("varchar"))
            	{
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (stringConverter != null)
                    {
                        return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                    }
            	}
            	// TODO There is a TypeConverter for this
            	return new java.sql.Date(row.getDate(mapping.getColumn(0).getIdentifier()).getTime());
            }
            else if (java.sql.Time.class.isAssignableFrom(mmd.getType()))
            {
            	if (mapping.getColumn(0).getTypeName().equals("varchar"))
            	{
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (stringConverter != null)
                    {
                        return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                    }
            	}
            	// TODO There is a TypeConverter for this
            	return new java.sql.Time(row.getDate(mapping.getColumn(0).getIdentifier()).getTime());
            }
            else if (java.sql.Timestamp.class.isAssignableFrom(mmd.getType()))
            {
            	if (mapping.getColumn(0).getTypeName().equals("varchar"))
            	{
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (stringConverter != null)
                    {
                        return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                    }
            	}
            	// TODO There is a TypeConverter for this
            	return new java.sql.Timestamp(row.getDate(mapping.getColumn(0).getIdentifier()).getTime());
            }
            else if (Calendar.class.isAssignableFrom(mmd.getType()))
            {
            	if (mapping.getColumn(0).getTypeName().equals("varchar"))
            	{
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (stringConverter != null)
                    {
                        return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                    }
            	}
            	// TODO Support Calendar with multiple columns and do via TypeConverter
                Calendar cal = Calendar.getInstance();
                cal.setTime(row.getDate(mapping.getColumn(0).getIdentifier()));
            	return cal;
            }
            else if (Date.class.isAssignableFrom(mmd.getType()))
            {
                if (mapping.getColumn(0).getTypeName().equals("varchar"))
                {
                    TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (stringConverter != null)
                    {
                        return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                    }
                }
                return row.getDate(mapping.getColumn(0).getIdentifier());
            }
            else
            {
                // TODO Support multi-column converters
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(row.getString(mapping.getColumn(0).getIdentifier()));
                }
                TypeConverter longConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
                if (longConverter != null)
                {
                    return longConverter.toMemberType(row.getLong(mapping.getColumn(0).getIdentifier()));
                }
            }
        }

        // TODO Support other types
        NucleusLogger.PERSISTENCE.info("TODO FetchFM field=" + mmd.getFullFieldName() + " not supported currently. Type=" + mmd.getTypeName());
        return null;
    }

    protected Object getValueForSingleRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        if (value == null)
        {
            return null;
        }

        String persistableId = (String)value;
        try
        {
            AbstractClassMetaData mmdCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (mmdCmd != null)
            {
                return IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
            }
            else
            {
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_FIELD, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    // Only one possible implementation, so use that
                    mmdCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    return IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
                }
                else if (implNames != null && implNames.length > 1)
                {
                    // Multiple implementations, so try each implementation in turn (note we only need this if some impls have different "identity" type from each other)
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

                throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has an interdeterminate type (e.g interface or Object element types)");
            }
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            NucleusLogger.GENERAL.warn("Object=" + op + " field=" + mmd.getFullFieldName() + " has id=" + persistableId +
                " but could not instantiate object with that identity");
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
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length > 0)
                {
                    // Just use first implementation TODO What if the impls have different id type?
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            // TODO Support serialised element which will be of type ByteBuffer
            Collection<String> collIds = (Collection<String>)value;
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

            if (op != null)
            {
                // Wrap if SCO
                coll = (Collection) op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false, false, true);
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
                map = (Map) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            Map mapIds = (Map)value;
            AbstractClassMetaData keyCmd = null;
            if (mmd.getMap().keyIsPersistent())
            {
                keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                if (keyCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                        FieldRole.ROLE_MAP_KEY, clr, ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        keyCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (keyCmd == null)
                    {
                        throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                                " which has a map of interdeterminate key type (e.g interface or Object element types)");
                    }
                }
            }
            AbstractClassMetaData valCmd = null;
            if (mmd.getMap().valueIsPersistent())
            {
                valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
                if (valCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                        FieldRole.ROLE_MAP_VALUE, clr, ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        valCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (valCmd == null)
                    {
                        throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                                " which has a map of interdeterminate value type (e.g interface or Object element types)");
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
                        val = null;
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
                        }
                    }
                }
                else
                {
                    // TODO Extract (non-persistable) map value
                    val = entry.getValue();
                }

                map.put(key, val);
            }

            if (op != null)
            {
                // Wrap if SCO
                map = (Map) op.wrapSCOField(mmd.getAbsoluteFieldNumber(), map, false, false, true);
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
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_ARRAY_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has an array of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            Collection<String> collIds = (Collection<String>)value;
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
                    for (int j=0;j<pos;j++)
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