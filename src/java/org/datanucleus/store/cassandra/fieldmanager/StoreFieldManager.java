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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for the storing of field values into Cassandra.
 * Note that for fields that are persistable objects, we store the "persistable-identity" of that object (see IdentityUtils class).
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    Map<Integer, Object> objectValues = new HashMap<Integer, Object>();

    public StoreFieldManager(ObjectProvider op, boolean insert)
    {
        super(op, insert);
    }

    public Object[] getValuesToStore()
    {
        // Make sure we return them in field order
        Object[] values = new Object[objectValues.size()];
        int i = 0;
        for (Integer fieldNum : objectValues.keySet())
        {
            values[i++] = objectValues.get(fieldNum);
        }
        return values;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeBooleanField(int, boolean)
     */
    @Override
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeCharField(int, char)
     */
    @Override
    public void storeCharField(int fieldNumber, char value)
    {
        objectValues.put(fieldNumber, "" + value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeByteField(int, byte)
     */
    @Override
    public void storeByteField(int fieldNumber, byte value)
    {
        objectValues.put(fieldNumber, (int)value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeShortField(int, short)
     */
    @Override
    public void storeShortField(int fieldNumber, short value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeIntField(int, int)
     */
    @Override
    public void storeIntField(int fieldNumber, int value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeLongField(int, long)
     */
    @Override
    public void storeLongField(int fieldNumber, long value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeFloatField(int, float)
     */
    @Override
    public void storeFloatField(int fieldNumber, float value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeDoubleField(int, double)
     */
    @Override
    public void storeDoubleField(int fieldNumber, double value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeStringField(int, java.lang.String)
     */
    @Override
    public void storeStringField(int fieldNumber, String value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE)
        {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // TODO Embedded PC object
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    // TODO Embedded Collection
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
                }
                objectValues.put(fieldNumber, null); // Remove this when we support embedded
                return;
            }
        }

        if (value == null)
        {
            objectValues.put(fieldNumber, null);
            return;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            Object valuePC = ec.persistObjectInternal(value, op, fieldNumber, -1);
            Object valueID = ec.getApiAdapter().getIdForObject(valuePC);
            objectValues.put(fieldNumber, IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valueID));
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (mmd.hasCollection())
            {
                Collection<String> idColl = (value instanceof List ? new ArrayList<String>() : new HashSet<String>());

                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                    idColl.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
                }
                objectValues.put(fieldNumber, idColl);
                return;
            }
            else if (mmd.hasMap())
            {
                Map idMap = new HashMap();

                Map map = (Map)value;
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
                        key = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), keyID);
                    }
                    else
                    {
                        key = CassandraUtils.getDatastoreValueForNonPersistableValue(key, keyCassType, false, ec.getTypeManager());
                    }
                    if (mmd.getMap().valueIsPersistent())
                    {
                        Object valPC = ec.persistObjectInternal(val, op, fieldNumber, -1);
                        Object valID = ec.getApiAdapter().getIdForObject(valPC);
                        val = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valID);
                    }
                    else
                    {
                        val = CassandraUtils.getDatastoreValueForNonPersistableValue(val, valCassType, false, ec.getTypeManager());
                    }

                    idMap.put(key, value);
                }
                objectValues.put(fieldNumber, idMap);
                return;
            }
            else if (mmd.hasArray())
            {
                // TODO Support arrays
            }
        }
        else
        {
            // Member with non-persistable object(s)
            if (mmd.hasCollection())
            {
                Collection cassColl = null;
                if (value instanceof List)
                {
                    cassColl = new ArrayList();
                }
                else
                {
                    cassColl = new HashSet();
                }
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                Class elemCls = clr.classForName(mmd.getCollection().getElementType());
                String elemCassType = CassandraUtils.getCassandraTypeForNonPersistableType(elemCls, false, ec.getTypeManager(), null);
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    cassColl.add(CassandraUtils.getDatastoreValueForNonPersistableValue(element, elemCassType, false, ec.getTypeManager()));
                }
                objectValues.put(fieldNumber, cassColl);
                return;
            }
            else if (mmd.hasMap())
            {
                Map cassMap = new HashMap();

                Map map = (Map)value;
                Iterator<Map.Entry> entryIter = map.entrySet().iterator();
                Class keyCls = clr.classForName(mmd.getMap().getKeyType());
                String keyCassType = CassandraUtils.getCassandraTypeForNonPersistableType(keyCls, false, ec.getTypeManager(), null);
                Class valCls = clr.classForName(mmd.getMap().getValueType());
                String valCassType = CassandraUtils.getCassandraTypeForNonPersistableType(valCls, false, ec.getTypeManager(), null);
                while (entryIter.hasNext())
                {
                    Map.Entry entry = entryIter.next();
                    Object key = entry.getKey();
                    Object val = entry.getValue();

                    key = CassandraUtils.getDatastoreValueForNonPersistableValue(key, keyCassType, false, ec.getTypeManager());
                    val = CassandraUtils.getDatastoreValueForNonPersistableValue(val, valCassType, false, ec.getTypeManager());
                    cassMap.put(key, value);
                }
                objectValues.put(fieldNumber, cassMap);
                return;
            }
            else if (mmd.hasArray())
            {
                // TODO Support arrays
            }

            String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmd, ec.getTypeManager(), clr);
            Object datastoreValue = CassandraUtils.getDatastoreValueForNonPersistableValue(value, cassandraType, mmd.isSerialized(), ec.getTypeManager());
            if (datastoreValue != null)
            {
                objectValues.put(fieldNumber, datastoreValue);
                return;
            }
        }

        NucleusLogger.PERSISTENCE.warn("Not generated persistable value for field " + mmd.getFullFieldName() + " so putting null");
        objectValues.put(fieldNumber, null);
    }
}