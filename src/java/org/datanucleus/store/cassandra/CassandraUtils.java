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
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;

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

        RelationType relType = mmd.getRelationType(clr);
        // TODO Support embedded relations
        if (RelationType.isRelationSingleValued(relType))
        {
            // TODO Are we going to store just the String form of the id like in MongoDB?
        }
        else if (RelationType.isRelationMultiValued(relType))
        {
            // TODO Are we going to store a Collection<String form of the id> like in MongoDB?
        }
        else
        {
            // TODO Support Collections/Sets/Lists/Map - return Set/List/Map of varchar/bigint for example
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

        // Just mark as no appropriate type
        return null;
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
}