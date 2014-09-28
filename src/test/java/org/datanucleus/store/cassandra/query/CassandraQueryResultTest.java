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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.store.query.Query;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class CassandraQueryResultTest
{

    private static final int TEST_RESULT_SIZE = 6;

    private CQLQuery mockCqlQuery;

    private ResultSet mockResultSet;

    private CassandraQueryResult cqlQueryResult;

    public CassandraQueryResultTest()
    {
    }

    @BeforeClass
    public static void setUpClass()
    {

    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp()
    {

        mockCqlQuery = mock(CQLQuery.class);
        mockResultSet = mock(ResultSet.class);

        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        when(mockCqlQuery.getExecutionContext()).thenReturn(mockExecutionContext);
        when(mockExecutionContext.getApiAdapter()).thenReturn(mock(ApiAdapter.class));
        when(mockCqlQuery.getStringExtensionProperty(Query.EXTENSION_RESULT_SIZE_METHOD, "last")).thenReturn("resultSizeMethod");
        when(mockCqlQuery.getBooleanExtensionProperty(Query.EXTENSION_LOAD_RESULTS_AT_COMMIT, true)).thenReturn(true);
        when(mockCqlQuery.useResultsCaching()).thenReturn(false);

        cqlQueryResult = new CassandraQueryResult(mockCqlQuery, mockResultSet);

        verify(mockCqlQuery, times(1)).getExecutionContext();
        verify(mockExecutionContext, times(1)).getApiAdapter();
        verify(mockCqlQuery, times(1)).getStringExtensionProperty(eq(Query.EXTENSION_RESULT_SIZE_METHOD), eq("last"));
        verify(mockCqlQuery, times(1)).getBooleanExtensionProperty(eq(Query.EXTENSION_LOAD_RESULTS_AT_COMMIT), eq(true));
        verify(mockCqlQuery, times(1)).useResultsCaching();
        verifyNoMoreInteractions(mockResultSet);

    }

    @After
    public void tearDown()
    {

        mockCqlQuery = null;
        mockResultSet = null;
        cqlQueryResult = null;
    }

    @Test
    public void shouldCloseResultsIfNoMoreElementsOnInitialize()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(false);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(3);

        cqlQueryResult.initialise();
        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldFetchGreedyOnInitialize()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(true);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(FetchPlan.FETCH_SIZE_GREEDY);

        cqlQueryResult.initialise();

        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(1)).all();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldFetchLimitedOnInitialize()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(true);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(3);
        Row row1 = mock(Row.class);
        Row row2 = mock(Row.class);
        Row row3 = mock(Row.class);
        final List<Row> rows = Arrays.asList(row1, row2, row3);
        when(mockResultSet.one()).thenAnswer(new Answer()
        {
            int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                return rows.get(count++);
            }
        });

        cqlQueryResult.initialise();

        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(3)).one();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldFetchLimitedOnInitializeWithSmallerResultSize()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(true);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(3);
        Row row1 = mock(Row.class);
        final List<Row> rows = Arrays.asList(row1, null);
        when(mockResultSet.one()).thenAnswer(new Answer()
        {
            int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                return rows.get(count++);
            }
        });

        cqlQueryResult.initialise();

        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(2)).one();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldFetchAllRowsOnInitializeWithGreedyFetch()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(true);
        List<Row> listOfRows = mock(List.class);
        when(mockResultSet.all()).thenReturn(listOfRows);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(FetchPlan.FETCH_SIZE_GREEDY);

        cqlQueryResult.initialise();

        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockResultSet, times(1)).all();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldIterateWithFetchGreedy()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(true);
        List<Row> listOfRows = mock(List.class);
        when(mockResultSet.all()).thenReturn(listOfRows);
        when(listOfRows.size()).thenReturn(TEST_RESULT_SIZE);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(FetchPlan.FETCH_SIZE_GREEDY);
        when(mockResultSet.isFullyFetched()).thenReturn(true);
        for (int index = 0; index < TEST_RESULT_SIZE; index++)
        {
            when(listOfRows.get(index)).thenReturn(mock(Row.class));
        }

        cqlQueryResult.initialise();
        Iterator<Object> iter = cqlQueryResult.iterator();
        int queryResultSize = 0;
        while (iter.hasNext())
        {
            queryResultSize++;
            Object row = iter.next();
            Assert.assertNotNull(row);
        }

        Assert.assertEquals(TEST_RESULT_SIZE, queryResultSize);

        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockResultSet, times(1)).all();
        verify(mockResultSet, times(1)).isFullyFetched();
        verify(listOfRows, times(13)).size();

        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

}
