/**********************************************************************
Copyright (c) 2014 Baris ERGUN and others. All rights reserved. 
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

package org.datanucleus.store.cassandra.query;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Used to hold arguments that will be passed in
 * QueryUtils.createResultObjectUsingDefaultConstructorAndSetters method*
 */
public class ResultClassInfo
{

    private final Field[] fields;

    private final String[] fieldNames;

    private final List<Integer> fieldsMatchingColumnIndexes;

    public ResultClassInfo(Field[] fields, String[] fieldNames, List<Integer> fieldsMatchingColumnIndexes)
    {
        this.fields = fields;
        this.fieldNames = fieldNames;
        this.fieldsMatchingColumnIndexes = fieldsMatchingColumnIndexes;
    }

    public Field[] getFields()
    {
        return fields;
    }

    public String[] getFieldNames()
    {
        return fieldNames;
    }

    public List<Integer> getFieldsMatchingColumnIndexes()
    {
        return fieldsMatchingColumnIndexes;
    }

}
