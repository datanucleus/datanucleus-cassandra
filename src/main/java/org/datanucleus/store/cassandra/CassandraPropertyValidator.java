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

import org.datanucleus.properties.PropertyValidator;

/**
 * Validator for persistence properties used by Cassandra.
 */
public class CassandraPropertyValidator implements PropertyValidator
{
    /**
     * Validate the specified property.
     * @param name Name of the property
     * @param value Value
     * @return Whether it is valid
     */
    public boolean validate(String name, Object value)
    {
        if (name == null)
        {
            return false;
        }
        else if (name.equals(ConnectionFactoryImpl.CASSANDRA_COMPRESSION))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("none") ||
                    strVal.equalsIgnoreCase("snappy"))
                {
                    return true;
                }
            }
        }
        return false;
    }
}