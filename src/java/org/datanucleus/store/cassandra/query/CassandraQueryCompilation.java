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

/**
 * Datastore-specific (Cassandra) compilation information for a java query.
 */
public class CassandraQueryCompilation
{
    boolean filterComplete = true;

    String cql = null;

    boolean resultComplete = true;

    boolean precompilable = true;

    public CassandraQueryCompilation()
    {
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setPrecompilable(boolean flag)
    {
        this.precompilable = flag;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public void setResultComplete(boolean complete)
    {
        this.resultComplete = complete;
    }

    public void setCQL(String cql)
    {
        this.cql = cql;
    }

    public String getCQL()
    {
        return cql;
    }
}