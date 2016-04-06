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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for the storing of field values into Cassandra. Note that for fields that are persistable
 * objects, we store the "persistable-identity" of that object (see IdentityUtils class). When this class is
 * invoked for all fields required it builds up a Map of column value keyed by the name of the column; this is
 * for use by the calling class.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    protected Table table;

    protected Map<String, Object> columnValueByName = new HashMap<String, Object>();

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.table = table;
    }

    public StoreFieldManager(ObjectProvider op, boolean insert, Table table)
    {
        super(op, insert);
        this.table = table;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public Map<String, Object> getColumnValueByName()
    {
        return columnValueByName;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeBooleanField (int, boolean)
     */
    @Override
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeCharField (int, char)
     */
    @Override
    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), "" + value);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeByteField (int, byte)
     */
    @Override
    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), Integer.valueOf(value));
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeShortField (int, short)
     */
    @Override
    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), Integer.valueOf(value));
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeIntField (int, int)
     */
    @Override
    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeLongField (int, long)
     */
    @Override
    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeFloatField (int, float)
     */
    @Override
    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column column = getColumnMapping(fieldNumber).getColumn(0);
        if (column.getJdbcType() == JdbcType.DECIMAL)
        {
            columnValueByName.put(column.getName(), BigDecimal.valueOf(value));
        }
        else if (column.getJdbcType() == JdbcType.DOUBLE)
        {
            columnValueByName.put(column.getName(), Double.valueOf(value));
        }
        else
        {
            columnValueByName.put(column.getName(), value);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeDoubleField (int, double)
     */
    @Override
    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column column = getColumnMapping(fieldNumber).getColumn(0);
        if (column.getJdbcType() == JdbcType.DECIMAL)
        {
            columnValueByName.put(column.getName(), BigDecimal.valueOf(value));
        }
        else
        {
            columnValueByName.put(column.getName(), value);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeStringField (int, java.lang.String)
     */
    @Override
    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeObjectField (int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                    {
                        // Related PC object not persistent, but can't do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }
                }

                // TODO Support discriminator on embedded object
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                int[] embMmdPosns = embCmd.getAllMemberPositions();
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                if (value == null)
                {
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, insert, embMmds, table);
                    for (int i = 0; i < embMmdPosns.length; i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd
                                .getTypeName()))
                        {
                            // Store a null for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            for (int j = 0; j < mapping.getNumberOfColumns(); j++)
                            {
                                columnValueByName.put(mapping.getColumn(j).getName(), null);
                            }
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    Map<String, Object> embColValuesByName = storeEmbFM.getColumnValueByName();
                    columnValueByName.putAll(embColValuesByName);
                    return;
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, insert, embMmds, table);
                embOP.provideFields(embMmdPosns, storeEmbFM);
                Map<String, Object> embColValuesByName = storeEmbFM.getColumnValueByName();
                columnValueByName.putAll(embColValuesByName);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Embedded Collection
                NucleusLogger.PERSISTENCE.warn("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), null);
                return;
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }

    protected void storeNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Object value)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        if (value instanceof Optional)
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }

            Optional opt = (Optional)value;
            if (opt.isPresent())
            {
                value = opt.get();
            }
            else
            {
                value = null;
            }
        }

        if (value == null)
        {
            for (int i = 0; i < mapping.getNumberOfColumns(); i++)
            {
                columnValueByName.put(mapping.getColumn(i).getName(), null);
            }
            return;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
            {
                if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                {
                    // Related PC object not persistent, but cant do cascade-persist so throw exception
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }
                    throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                }
            }

            if (mmd.isSerialized() && value instanceof Serializable)
            {
                // Assign an ObjectProvider to the serialised object if none present
                ObjectProvider pcOP = ec.findObjectProvider(value);
                if (pcOP == null || ec.getApiAdapter().getExecutionContext(value) == null)
                {
                    pcOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, value, false, op, fieldNumber);
                }

                if (pcOP != null)
                {
                    pcOP.setStoringPC();
                }

                // Convert to ByteBuffer and use that
                TypeConverter serialConv;
                if (value instanceof byte[])
                {
                    serialConv = ec.getTypeManager().getTypeConverterForType(byte[].class, ByteBuffer.class);
                }
                else
                {
                    serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, ByteBuffer.class);
                }
                Object serValue = serialConv.toDatastoreType(value);
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), serValue);

                if (pcOP != null)
                {
                    pcOP.unsetStoringPC();
                }
                return;
            }

            Object valuePC = ec.persistObjectInternal(value, op, fieldNumber, -1);
            Object valueID = ec.getApiAdapter().getIdForObject(valuePC);
            // TODO Support 1-1 storage using "FK" style column(s) for related object
            columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), IdentityUtils.getPersistableIdentityForId(valueID));
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (mmd.hasCollection())
            {
                if (mmd.getCollection().isSerializedElement())
                {
                    // TODO Support persistable element
                    throw new NucleusUserException("Don't currently support serialised collection elements at " + mmd.getFullFieldName() + ". Serialise the whole field");
                }

                Collection coll = (Collection) value;
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    // Field doesnt support cascade-persist so no reachability
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }

                    // Check for any persistable elements that aren't persistent
                    for (Object element : coll)
                    {
                        if (!ec.getApiAdapter().isDetached(element) && !ec.getApiAdapter().isPersistent(element))
                        {
                            // Element is not persistent so throw exception
                            throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), element);
                        }
                    }
                }

                Collection<String> cassColl = (value instanceof List || value instanceof Queue ? new ArrayList<String>() : new HashSet<String>());

                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        cassColl.add(IdentityUtils.getPersistableIdentityForId(elementID));
                    }
                    else
                    {
                        // Store as "NULL" and extract in FetchFieldManager
                        cassColl.add("NULL");
                    }
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), cassColl);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }
            else if (mmd.hasMap())
            {
                if (mmd.getMap().isSerializedKey() || mmd.getMap().isSerializedValue())
                {
                    // TODO Support persistable key/value
                    throw new NucleusUserException("Don't currently support serialised map keys/values at " + mmd.getFullFieldName() + ". Serialise the whole field");
                }

                // TODO Add check on reachability
                Map idMap = new HashMap();

                Map map = (Map) value;
                Iterator<Map.Entry> entryIter = map.entrySet().iterator();
                String keyCassType = null;
                if (!mmd.getMap().keyIsPersistent())
                {
                    Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                    keyCassType = CassandraUtils.getCassandraTypeForNonPersistableType(keyCls, false, ec.getTypeManager(), null);
                }
                String valCassType = null;
                if (!mmd.getMap().valueIsPersistent())
                {
                    Class valCls = clr.classForName(mmd.getMap().getValueType());
                    valCassType = CassandraUtils.getCassandraTypeForNonPersistableType(valCls, false, ec.getTypeManager(), null);
                }
                while (entryIter.hasNext())
                {
                    Map.Entry entry = entryIter.next();
                    Object key = entry.getKey();
                    Object val = entry.getValue();

                    if (mmd.getMap().keyIsPersistent())
                    {
                        Object keyPC = ec.persistObjectInternal(key, op, fieldNumber, -1);
                        Object keyID = ec.getApiAdapter().getIdForObject(keyPC);
                        key = IdentityUtils.getPersistableIdentityForId(keyID);
                    }
                    else
                    {
                        key = CassandraUtils.getDatastoreValueForNonPersistableValue(key, keyCassType, false, ec.getTypeManager());
                    }

                    if (mmd.getMap().valueIsPersistent())
                    {
                        if (val != null)
                        {
                            Object valPC = ec.persistObjectInternal(val, op, fieldNumber, -1);
                            Object valID = ec.getApiAdapter().getIdForObject(valPC);
                            val = IdentityUtils.getPersistableIdentityForId(valID);
                        }
                        else
                        {
                            val = "NULL";
                        }
                    }
                    else
                    {
                        val = CassandraUtils.getDatastoreValueForNonPersistableValue(val, valCassType, false, ec.getTypeManager());
                    }

                    idMap.put(key, val);
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), idMap);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }
            else if (mmd.hasArray())
            {
                if (mmd.getArray().isSerializedElement())
                {
                    // TODO Support Serialised elements
                    throw new NucleusUserException("Don't currently support serialised array elements at " + mmd.getFullFieldName() + ". Serialise the whole field");
                }

                Collection cassColl = new ArrayList();
                for (int i = 0; i < Array.getLength(value); i++)
                {
                    Object element = Array.get(value, i);
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, null, -1, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        cassColl.add(IdentityUtils.getPersistableIdentityForId(elementID));
                    }
                    else
                    {
                        // Store as "NULL" and extract in FetchFieldManager
                        cassColl.add("NULL");
                    }
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), cassColl);
                return;
            }
        }
        else
        {
            if (mapping.getTypeConverter() != null)
            {
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                if (mapping.getNumberOfColumns() > 1)
                {
                    for (int i = 0; i < Array.getLength(datastoreValue); i++)
                    {
                        columnValueByName.put(mapping.getColumn(i).getName(), Array.get(datastoreValue, i));
                    }
                }
                else
                {
                    columnValueByName.put(mapping.getColumn(0).getName(), datastoreValue);
                }
                return;
            }

            // Member with non-persistable object(s)
            if (Optional.class.isAssignableFrom(mmd.getType()))
            {
                String cassandraType = mapping.getColumn(0).getTypeName();
                Object datastoreValue = CassandraUtils.getDatastoreValueForNonPersistableValue(value, cassandraType, mmd.isSerialized(), ec.getTypeManager());
                if (datastoreValue != null)
                {
                    columnValueByName.put(mapping.getColumn(0).getName(), datastoreValue);
                    SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                    return;
                }
            }
            else if (mmd.hasCollection())
            {
                Collection coll = (Collection) value;
                if (coll.isEmpty())
                {
                    columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), null);
                    return;
                }

                Collection cassColl = null;
                if (value instanceof List || value instanceof Queue)
                {
                    cassColl = new ArrayList();
                }
                else
                {
                    cassColl = new HashSet();
                }

                TypeConverter elemConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_COLLECTION_ELEMENT);

                Class elemCls = clr.classForName(mmd.getCollection().getElementType());
                String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object datastoreValue = element;
                    if (elemConv != null)
                    {
                        datastoreValue = elemConv.toDatastoreType(element);
                    }
                    cassColl.add(CassandraUtils.getDatastoreValueForNonPersistableValue(datastoreValue, elemCassType, false, ec.getTypeManager()));
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), cassColl);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }
            else if (mmd.hasMap())
            {
                Map map = (Map) value;
                if (map.isEmpty())
                {
                    columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), null);
                    return;
                }

                TypeConverter keyConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_KEY);
                TypeConverter valConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_VALUE);

                Iterator<Map.Entry> entryIter = map.entrySet().iterator();
                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                String keyCassType = CassandraUtils.getCassandraTypeForNonPersistableType(keyCls, false, ec.getTypeManager(), null);
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                String valCassType = CassandraUtils.getCassandraTypeForNonPersistableType(valCls, false, ec.getTypeManager(), null);

                Map cassMap = new HashMap();
                while (entryIter.hasNext())
                {
                    Map.Entry entry = entryIter.next();

                    Object key = entry.getKey();
                    Object datastoreKey = key;
                    if (keyConv != null)
                    {
                        datastoreKey = keyConv.toDatastoreType(key);
                    }

                    Object val = entry.getValue();
                    Object datastoreVal = val;
                    if (valConv != null)
                    {
                        datastoreVal = valConv.toDatastoreType(val);
                    }

                    key = CassandraUtils.getDatastoreValueForNonPersistableValue(datastoreKey, keyCassType, false, ec.getTypeManager());
                    val = CassandraUtils.getDatastoreValueForNonPersistableValue(datastoreVal, valCassType, false, ec.getTypeManager());
                    cassMap.put(key, val);
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), cassMap);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }
            else if (mmd.hasArray())
            {
                if (Array.getLength(value) == 0)
                {
                    columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), null);
                    return;
                }

                if (mmd.isSerialized())
                {
                    String cassandraType = mapping.getColumn(0).getTypeName();
                    Object datastoreValue = CassandraUtils.getDatastoreValueForNonPersistableValue(value, cassandraType, mmd.isSerialized(), ec.getTypeManager());
                    columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), datastoreValue);
                    return;
                }

                Collection cassArr = new ArrayList();
                Class elemCls = clr.classForName(mmd.getArray().getElementType());
                String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
                for (int i = 0; i < Array.getLength(value); i++)
                {
                    if (mmd.getArray().isSerializedElement())
                    {
                        // TODO Support Serialised elements
                        throw new NucleusUserException(
                                "Don't currently support serialised array elements at " + mmd.getFullFieldName() + ". Serialise the whole field");
                    }
                    Object element = Array.get(value, i);
                    if (element != null)
                    {
                        cassArr.add(CassandraUtils.getDatastoreValueForNonPersistableValue(element, elemCassType, false, ec.getTypeManager()));
                    }
                }
                columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), cassArr);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }

            // TODO What if there are multiple columns?
            String cassandraType = mapping.getColumn(0).getTypeName();
            Object datastoreValue = CassandraUtils.getDatastoreValueForNonPersistableValue(value, cassandraType, mmd.isSerialized(), ec.getTypeManager());
            if (datastoreValue != null)
            {
                columnValueByName.put(mapping.getColumn(0).getName(), datastoreValue);
                SCOUtils.wrapSCOField(op, fieldNumber, value, true);
                return;
            }
        }

        NucleusLogger.PERSISTENCE.warn("Not generated persistable value for field " + mmd.getFullFieldName() + " so putting null");
        columnValueByName.put(mapping.getColumn(0).getName(), null);
    }
}