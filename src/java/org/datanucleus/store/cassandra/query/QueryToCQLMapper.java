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

import java.util.Map;
import java.util.Stack;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.store.cassandra.query.expression.CassandraExpression;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapper for converting a generic query into CQL.
 */
public class QueryToCQLMapper extends AbstractExpressionEvaluator
{
    final ExecutionContext ec;

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final Query query;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    /** Whether the result clause is completely evaluatable in the datastore. */
    boolean resultComplete = true;

    boolean precompilable = true;

    /** Stack of expressions, used during compilation process. */
    Stack<CassandraExpression> stack = new Stack();

    public QueryToCQLMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd,
            ExecutionContext ec, Query q)
    {
        this.ec = ec;
        this.query = q;
        this.compilation = compilation;
        this.parameters = parameters;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void compile()
    {
        compileFilter();
        compileResult();
        compileGrouping();
        compileHaving();
        compileOrdering();
        // TODO Compile range also since CQL has "LIMIT n" (but no offset)
    }

    /**
     * Method to compile the FILTER clause of the query
     */
    protected void compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            compileComponent = CompilationComponent.FILTER;

            try
            {
                compilation.getExprFilter().evaluate(this);
                // TODO Implement this
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug("Compilation of filter to be evaluated completely in-datastore was impossible : " + e.getMessage());
                }
                filterComplete = false;
            }

            compileComponent = null;
        }
    }
    /**
     * Method to compile the RESULT clause of the query
     */
    protected void compileResult()
    {
        if (compilation.getExprResult() != null)
        {
            // TODO Implement this
        }
    }
    /**
     * Method to compile the GROUPING clause of the query
     */
    protected void compileGrouping()
    {
        if (compilation.getExprFilter() != null)
        {
            // TODO Implement this
        }
    }
    /**
     * Method to compile the HAVING clause of the query
     */
    protected void compileHaving()
    {
        if (compilation.getExprHaving() != null)
        {
            // TODO Implement this
        }
    }
    /**
     * Method to compile the ORDERING clause of the query
     */
    protected void compileOrdering()
    {
        if (compilation.getExprOrdering() != null)
        {
            compileComponent = CompilationComponent.ORDERING;
            Expression[] orderingExpr = compilation.getExprOrdering();
            for (int i = 0; i < orderingExpr.length; i++)
            {
                OrderExpression orderExpr = (OrderExpression) orderingExpr[i];
                NucleusLogger.QUERY.debug(">> TODO Need to process " + orderExpr);
                // TODO Implement this
            }
            compileComponent = null;
        }
    }

    @Override
    protected Object processAndExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processAndExpression(expr);
    }

    @Override
    protected Object processOrExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processOrExpression(expr);
    }

    @Override
    protected Object processEqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processEqExpression(expr);
    }

    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processNoteqExpression(expr);
    }

    @Override
    protected Object processGtExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processGtExpression(expr);
    }

    @Override
    protected Object processGteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processGteqExpression(expr);
    }

    @Override
    protected Object processLtExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processLtExpression(expr);
    }

    @Override
    protected Object processLteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processLteqExpression(expr);
    }

    @Override
    protected Object compilePrimaryExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.compilePrimaryExpression(expr);
    }

    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        // TODO Auto-generated method stub
        return super.processParameterExpression(expr);
    }

    @Override
    protected Object processLiteral(Literal expr)
    {
        // TODO Auto-generated method stub
        return super.processLiteral(expr);
    }

    // TODO Override the processAndExpression methods etc to implement what is supported by this mapper
    
}