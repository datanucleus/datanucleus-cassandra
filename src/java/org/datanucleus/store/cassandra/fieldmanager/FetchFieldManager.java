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
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.Row;

/**
 * FieldManager to use for retrieving values from Cassandra to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    protected ExecutionContext ec;

    protected AbstractClassMetaData cmd;

    protected ObjectProvider op;

    protected Row row;

    /** Metadata of the owner field if this is for an embedded object. */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(ObjectProvider op, Row row)
    {
        this.op = op;
        this.cmd = op.getClassMetaData();
        this.ec = op.getExecutionContext();
        this.row = row;
    }

    public FetchFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd)
    {
        this.cmd = cmd;
        this.ec = ec;
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

        boolean embedded = false;
        if (relationType != RelationType.NONE)
        {
            // Determine if this relation field is embedded
            if (RelationType.isRelationSingleValued(relationType))
            {
                if (ownerMmd != null && ownerMmd.getEmbeddedMetaData() != null)
                {
                    // Is this a nested embedded (from JDO definition) with specification for this field?
                    AbstractMemberMetaData[] embMmds = ownerMmd.getEmbeddedMetaData().getMemberMetaData();
                    if (embMmds != null)
                    {
                        for (int i=0;i<embMmds.length;i++)
                        {
                            if (embMmds[i].getName().equals(mmd.getName()))
                            {
                                embedded = true;
                            }
                        }
                    }
                }
            }

            if (mmd.isEmbedded() || mmd.getEmbeddedMetaData() != null)
            {
                // Does this field have @Embedded definition?
                embedded = true;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // Is this an embedded element/key/value?
                if (mmd.hasCollection() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
                {
                    // Embedded collection element
                    embedded = true;
                }
                else if (mmd.hasArray() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
                {
                    // Embedded array element
                    embedded = true;
                }
                else if (mmd.hasMap() && 
                        ((mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null) || 
                        (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null)))
                {
                    // Embedded map key/value
                    embedded = true;
                }
            }
        }

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
            Object value = row.getString(colName);
            return getValueForContainerRelationField(mmd, value, clr);
        }
        else
        {
            if (mmd.isSerialized())
            {
                // Retrieve byte[] and convert back
                ByteBuffer byteBuffer = row.getBytes(colName);
                byte[] bytes = byteBuffer.array();
                TypeConverter serialConv = ec.getTypeManager().getTypeConverterForType(Serializable.class, byte[].class);
                return serialConv.toMemberType(bytes);
            }
            if (Enum.class.isAssignableFrom(mmd.getType()))
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

        // TODO Support other types
        NucleusLogger.PERSISTENCE.info(">> FetchFM field=" + mmd.getFullFieldName() + " not supported currently. Type=" + mmd.getTypeName());
        return null;
    }

    protected Object getValueForSingleRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        String idStr = (String)value;
        if (value == null)
        {
            return null;
        }

        try
        {
            return IdentityUtils.getObjectFromIdString(idStr, mmd, FieldRole.ROLE_FIELD, ec, true);
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            NucleusLogger.GENERAL.warn("Object=" + op + " field=" + mmd.getFullFieldName() + " has id=" + idStr +
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
                String elementIdStr = idIter.next();
                if (elementIdStr == null) // TODO Can you store null elements in a Cassandra Set/List?
                {
                    coll.add(null);
                }
                else
                {
                    try
                    {
                        coll.add(IdentityUtils.getObjectFromIdString(elementIdStr, elemCmd, ec, true));
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
            // TODO Support maps
            return null;
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