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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

public class CassandraQueryResult extends AbstractQueryResult
{
    private static final long serialVersionUID = -3428653031644090769L;

    /** The ResultSet containing the results. */
    private ResultSet rs;

    /** The Result Objects. */
    private List<Row> resultObjs = new ArrayList();

    private List resultIds = null;

    /** resultObjsIndex used by iterator */
    private int resultObjsIndex = 0;

    // TODO applyRangeChecks

    public CassandraQueryResult(Query query, ResultSet rs)
    {
        super(query);
        this.rs = rs;
        if (query.useResultsCaching())
        {
            resultIds = new ArrayList();
        }

    }

    public void initialise()
    {
        int fetchSize = query.getFetchPlan().getFetchSize();
        if (!rs.iterator().hasNext())
        {
            // ResultSet is empty, so just close it
            closeResults();
            return;
        }
        if (fetchSize == FetchPlan.FETCH_SIZE_GREEDY)
        {
            // "greedy" mode, so load all results now
            advanceToEndOfResultSet();
        }
        else if (fetchSize > FetchPlan.FETCH_SIZE_OPTIMAL)
        {
            // Load "fetchSize" results now
            processNumberOfResults(fetchSize);
        }
    }

    /**
     * Internal method to advance to the end of the ResultSet, populating the resultObjs, and close the
     * ResultSet when complete.
     */
    private void advanceToEndOfResultSet()
    {
        processNumberOfResults(-1);
    }

    /**
     * Internal method to fetch remaining results into resultsObjs
     */
    private void fetchMoreResults()
    {
        if (!rs.isFullyFetched())
        {
            resultObjsIndex = 0;
            resultObjs.clear();
            rs.fetchMoreResults();
            int fetchSize = query.getFetchPlan().getFetchSize();
            ExecutionContext ec = query.getExecutionContext();
            for (int i = 0; i < fetchSize; i++)
            {
                Row row = rs.one();
                if (null == row)
                {
                    break;
                }
                resultObjs.add(row);
                if (resultIds != null)
                {
                    resultIds.add(ec.getApiAdapter().getIdForObject(row));
                }
            }
        }
        else
        {
            closeResults();
        }

    }

    /**
     * Method to advance through the results, processing the specified number of results.
     * @param number Number of results (-1 means process all)
     */
    private void processNumberOfResults(int number)
    {
        ExecutionContext ec = query.getExecutionContext();
        if (FetchPlan.FETCH_SIZE_GREEDY >= number)
        {
            if (resultIds != null)
            {
                Iterator<Row> rowIter = rs.iterator();
                while (rowIter.hasNext())
                {
                    Row row = rs.one();
                    resultObjs.add(row);
                    resultIds.add(ec.getApiAdapter().getIdForObject(row));
                }
            }
            else
            {
                resultObjs = rs.all();
            }

        }
        else
        {
            for (int i = 0; i < number; i++)
            {
                Row row = rs.one();
                if (null == row)
                {
                    break;
                }
                resultObjs.add(row);
                if (resultIds != null)
                {
                    resultIds.add(ec.getApiAdapter().getIdForObject(row));
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
        rs = null;
        resultObjs = null;
    }

    /**
     * Internal method to close the ResultSet.
     */
    @Override
    protected void closeResults()
    {

        if (resultIds != null)
        {
            // Cache the results with the QueryManager
            query.getQueryManager().addQueryResult(query, query.getInputParameters(), resultIds);
            resultIds = null;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (null == o || !(o instanceof CassandraQueryResult))
        {
            return false;
        }

        CassandraQueryResult other = (CassandraQueryResult) o;
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
        return null;
    }

    @Override
    public Iterator iterator()
    {
        return new QueryResultIterator();
    }

    @Override
    public ListIterator listIterator()
    {
        return new QueryResultIterator();
    }

    private class QueryResultIterator extends AbstractQueryResultIterator
    {
        /** hold the last element **/
        Row currentElement = null;

        @Override
        public boolean hasNext()
        {
            synchronized (CassandraQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                if (null != resultObjs && resultObjsIndex < resultObjs.size())
                {
                    return true;
                }
                return false;

            }
        }

        @Override
        public Object next()
        {
            synchronized (CassandraQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }
                int resultObjsSize = resultObjs.size();

                if (resultObjsIndex < resultObjsSize)
                {
                    currentElement = resultObjs.get(resultObjsIndex++);
                    if (resultObjsIndex == resultObjsSize)
                    {
                        fetchMoreResults();
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