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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import org.datanucleus.FetchPlan;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * QueryResult for Cassandra CQL queries.
 */
public class CQLQueryResult extends AbstractQueryResult
{
    private static final long serialVersionUID = -3428653031644090769L;

    /** The ResultSet containing the results. */
    private ResultSet rs;

    private Iterator<Row> resultIter;

    /** The Result Objects. */
    private List<Row> resultObjs = new ArrayList();

    /** Index where we are with the iterator (0=not started, 1=first, etc). */
    private int resultObjsIndex = 0;

    private boolean endOfResults = false;

    private int fetchSize = 1;

    // TODO applyRangeChecks

    public CQLQueryResult(Query query, ResultSet rs)
    {
        super(query);
        this.rs = rs;
        // TODO Support result caching
    }

    public void initialise()
    {
        resultIter = rs.iterator();

        if (!resultIter.hasNext())
        {
            // ResultSet is empty, so just close it
            endOfResults = true;
            closeResults();
            return;
        }

        // Support fetchSize batch loading of results
        fetchSize = query.getFetchPlan().getFetchSize();

        if (fetchSize == FetchPlan.FETCH_SIZE_GREEDY)
        {
            // "greedy"/"optimal" mode, so load all results now
            loadNumberOfResults(-1);
        }
        else if (fetchSize > FetchPlan.FETCH_SIZE_OPTIMAL)
        {
            // Load "fetchSize" results now
            loadNumberOfResults(fetchSize);
        }
    }

    /**
     * Method to advance through the results, loading the specified number of results.
     * @param number Number of results (negative means load all)
     */
    private void loadNumberOfResults(int number)
    {
        if (endOfResults)
        {
            return;
        }

        if (number < 0)
        {
            // Load all results
            resultObjs = rs.all();
            endOfResults = true;
        }
        else
        {
            // Just load a few
            int idx = 0;
            while (resultIter.hasNext())
            {
                Row row = resultIter.next();
                resultObjs.add(row);
                idx++;

                if (idx == number)
                {
                    if (!resultIter.hasNext())
                    {
                        // At end of RS
                        endOfResults = true;
                    }
                    break;
                }
            }
        }
    }

    @Override
    protected void closingConnection()
    {
    }

    @Override
    public void disconnect()
    {
        if (query == null)
        {
            // Already disconnected
            return;
        }

        resultIter = null;
        rs = null;
        resultObjs = null;
    }

    /**
     * Method to close the results, meaning that they are inaccessible after this point.
     */
    @Override
    public synchronized void close()
    {
        super.close();

        resultIter = null;
        rs = null;
        resultObjs = null;
    }

    /**
     * Internal method to close the ResultSet.
     */
    @Override
    protected void closeResults()
    {
    }

    @Override
    public boolean equals(Object o)
    {
        if (null == o || !(o instanceof CQLQueryResult))
        {
            return false;
        }

        CQLQueryResult other = (CQLQueryResult) o;
        if (rs != null)
        {
            return other.rs == rs;
        }
        else if (query != null)
        {
            return other.query == query;
        }
        return StringUtils.toJVMIDString(other).equals(StringUtils.toJVMIDString(this));
    }

    @Override
    public int hashCode()
    {
        if (rs != null)
        {
            return rs.hashCode();
        }
        else if (query != null)
        {
            return query.hashCode();
        }
        return StringUtils.toJVMIDString(this).hashCode();
    }

    @Override
    public Object get(int index)
    {
        if (index < 0 || index >= size())
        {
            throw new IndexOutOfBoundsException();
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Iterator iterator()
    {
        return new CQLResultIterator();
    }

    @Override
    public ListIterator listIterator()
    {
        return new CQLResultIterator();
    }

    private class CQLResultIterator extends AbstractQueryResultIterator
    {
        public boolean hasNext()
        {
            synchronized (CQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                if (resultObjsIndex < resultObjs.size())
                {
                    return true;
                }
                if (!endOfResults)
                {
                    return resultIter.hasNext();
                }
                return false;
            }
        }

        @Override
        public Object next()
        {
            synchronized (CQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }

                int resultObjsSize = resultObjs.size();
                if (resultObjsIndex < resultObjsSize)
                {
                    Row currentElement = resultObjs.get(resultObjsIndex++);
                    if (resultObjsIndex == resultObjsSize)
                    {
                        loadNumberOfResults(fetchSize);
                    }

                    return currentElement;
                }

                throw new NoSuchElementException(Localiser.msg("052602"));
            }
        }

        @Override
        public boolean hasPrevious()
        {
            // We only navigate in forward direction, Cassandra issues mention
            // about scrolling but looks like not implemented ref:CASSANDRA-2876
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int nextIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public Object previous()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int previousIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}