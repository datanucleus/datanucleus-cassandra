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
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.Row;

/**
 * FieldManager to use for retrieving values from Cassandra to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected Row row;

    /** Metadata of the owner field if this is for an embedded object. */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(ObjectProvider op, Row row)
    {
        super(op);
        this.row = row;
    }

    public FetchFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd)
    {
        super(ec, cmd);
        this.row = row;
    }

    protected String getColumnName(int fieldNumber)
    {
        return ec.getStoreManager().getNamingFactory().getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber), ColumnType.COLUMN);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        return row.getBool(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        return row.getString(getColumnName(fieldNumber)).charAt(0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        return (byte) row.getInt(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        return (short) row.getInt(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        return row.getInt(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        return row.getLong(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        return row.getFloat(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        return row.getDouble(getColumnName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        return row.getString(getColumnName(fieldNumber));
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
        String colName = getColumnName(fieldNumber);

        boolean embedded = isMemberEmbedded(mmd, relationType, ownerMmd);
        if (embedded)
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Embedded PC object
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded)");
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Embedded Collection
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded)");
            }
            return null; // Remove this when we support embedded
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            Object value = row.getString(colName);
            return getValueForSingleRelationField(mmd, value, clr);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            Object value = null;
            if (mmd.hasCollection())
            {
                if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                {
                    value = row.getList(colName, String.class);
                }
                else
                {
                    value = row.getSet(colName, String.class);
                }
                NucleusLogger.DATASTORE_RETRIEVE.info(">> FetchFM " + mmd.getFullFieldName() + " coll=" + StringUtils.collectionToString((Collection) value));
            }
            else if (mmd.hasMap())
            {
                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                if (mmd.getMap().keyIsPersistent())
                {
                    keyCls = String.class;
                }
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                if (mmd.getMap().valueIsPersistent())
                {
                    valCls = String.class;
                }
                value = row.getMap(colName, keyCls, valCls);
            }
            else if (mmd.hasArray())
            {
                // TODO Support arrays (use List)
            }
            return getValueForContainerRelationField(mmd, value, clr);
        }
        else
        {
            if (mmd.hasCollection())
            {
                Collection cassColl = null;
                Class elementCls = clr.classForName(mmd.getCollection().getElementType());
                if (List.class.isAssignableFrom(mmd.getType()) || mmd.getOrderMetaData() != null)
                {
                    cassColl = row.getList(colName, elementCls);
                }
                else
                {
                    cassColl = row.getSet(colName, elementCls);
                }
                NucleusLogger.DATASTORE_RETRIEVE.debug("Field=" + mmd.getFullFieldName() + " has datastore collection=" + StringUtils.collectionToString(cassColl) + " not supported yet");
                // TODO Support this
            }
            else if (mmd.hasMap())
            {
                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                Map cassMap = row.getMap(colName, keyCls, valCls);
                NucleusLogger.DATASTORE_RETRIEVE.debug("Field=" + mmd.getFullFieldName() + " has datastore map=" + StringUtils.mapToString(cassMap) + " not supported yet");
                // TODO Support this
            }
            else if (mmd.hasArray())
            {
                // TODO Support this
            }
            else if (mmd.isSerialized())
            {
                // Retrieve byte[] and convert back
                ByteBuffer byteBuffer = row.getBytes(colName);
                byte[] bytes = byteBuffer.array();
                TypeConverter serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, byte[].class);
                return serialConv.toMemberType(bytes);
            }
            else if (Enum.class.isAssignableFrom(mmd.getType()))
            {
                // Persist as ordinal unless user specifies jdbc-type of "varchar"
                ColumnMetaData[] colmds = mmd.getColumnMetaData();
                if (colmds != null && colmds.length == 1)
                {
                    if (colmds[0].getJdbcType().equalsIgnoreCase("varchar"))
                    {
                        return Enum.valueOf(mmd.getType(), row.getString(colName));
                    }
                }
                return mmd.getType().getEnumConstants()[row.getInt(colName)];
            }
            else if (Date.class.isAssignableFrom(mmd.getType()))
            {
                return row.getDate(colName);
            }
            else
            {
                TypeConverter stringConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                if (stringConverter != null)
                {
                    return stringConverter.toMemberType(row.getString(colName));
                }
                TypeConverter longConverter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
                if (longConverter != null)
                {
                    return longConverter.toMemberType(row.getLong(colName));
                }
            }
        }

        // TODO Support other types
        NucleusLogger.PERSISTENCE.info(">> FetchFM field=" + mmd.getFullFieldName() + " not supported currently. Type=" + mmd.getTypeName());
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
            return IdentityUtils.getObjectFromPersistableIdentity(persistableId, cmd, ec);
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
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }
            Collection<String> collIds = (Collection<String>)value;
            Iterator<String> idIter = collIds.iterator();
            boolean changeDetected = false;
            while (idIter.hasNext())
            {
                String persistableId = idIter.next();
                NucleusLogger.GENERAL.info(">> coll elemIdStr=" + persistableId);
                if (persistableId == null) // TODO Can you store null elements in a Cassandra Set/List?
                {
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
                    String keyPersistableId = (String) entry.getKey();
                    if (keyPersistableId == null) // TODO Can you store null keys in a Cassandra Map?
                    {
                        key = null;
                    }
                    else
                    {
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
                }
                else
                {
                    // TODO Extract (non-persistable) map key
                    key = entry.getKey();
                }
                if (mmd.getMap().valueIsPersistent())
                {
                    String valPersistableId = (String) entry.getValue();
                    if (valPersistableId == null) // TODO Can you store null values in a Cassandra Map?
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
            // TODO Support arrays
            return null;
        }
        else
        {
            return value;
        }
    }
}