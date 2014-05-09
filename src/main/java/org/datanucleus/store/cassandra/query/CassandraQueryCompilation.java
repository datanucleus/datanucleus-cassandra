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
package org.datanucleus.store.cassandra.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Datastore-specific (Cassandra) compilation information for a java query.
 */
public class CassandraQueryCompilation
{
    boolean resultComplete = true;
    boolean filterComplete = true;
    boolean orderComplete = true;

    Map<String, String> cqlByClass = new HashMap<String, String>();

    boolean precompilable = true;

    public CassandraQueryCompilation()
    {
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setPrecompilable(boolean flag)
    {
        this.precompilable = flag;
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public void setResultComplete(boolean complete)
    {
        this.resultComplete = complete;
    }

    public boolean isOrderComplete()
    {
        return orderComplete;
    }

    public void setOrderComplete(boolean complete)
    {
        this.orderComplete = complete;
    }

    public void setCQLForClass(String className, String cql)
    {
        this.cqlByClass.put(className, cql);
    }

    public String getCQLForClass(String className)
    {
        return cqlByClass.get(className);
    }
    
    public Set<String> getClassNames()
    {
        return cqlByClass.keySet();
    }
}