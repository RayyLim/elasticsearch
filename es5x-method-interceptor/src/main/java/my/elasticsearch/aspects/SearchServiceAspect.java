/********************************************************************
 * File Name:    SearchServiceAspect.java
 *
 * Date Created: Oct 26, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.aspects;

import java.util.Arrays;

import org.apache.logging.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
  
@Aspect
public class SearchServiceAspect
{
  // Query Then Fetch: Query Phase
  
  @Around("execution (public * org.elasticsearch.search.SearchService.executeQueryPhase(org.elasticsearch.search.internal.ShardSearchRequest,org.elasticsearch.action.search.SearchTask)) && args(request,task)")
  public Object executeQueryPhase(final ProceedingJoinPoint joinPoint, final ShardSearchTransportRequest request, final org.elasticsearch.action.search.SearchTask task)
  {
    try
    {
      logMethodSignature(joinPoint);
      LOGGER.error("### request [{}] : incides {}: index [{}]", request, request.indices(), request.shardId().getIndexName());
      return joinPoint.proceed();
    }
    catch (Throwable e)
    {
      LOGGER.error("An error occurred.", e);
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception.
    }
  }
  
  // Query Then Fetch: Fetch Phase
  @Around("execution (public * org.elasticsearch.search.SearchService.executeFetchPhase(org.elasticsearch.search.fetch.ShardFetchRequest,org.elasticsearch.action.search.SearchTask)) && args(request,task)")
  public Object executeFetchPhase(final ProceedingJoinPoint joinPoint, final ShardFetchSearchRequest request, final org.elasticsearch.action.search.SearchTask task)
  {
    try
    {
      logMethodSignature(joinPoint);
      LOGGER.error("### request [{}] : indices {} : docIds {}", request, request.indices(), Arrays.copyOf(request.docIds(), request.docIdsSize()));
      return joinPoint.proceed();
    }
    catch (Throwable e)
    {
      LOGGER.error("An error occurred.", e);
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception.
    }
  }

  // Query And Fetch: Single call
/*  
  @Around("execution (public * org.elasticsearch.search.SearchService.executeFetchPhase(org.elasticsearch.search.internal.ShardSearchRequest,org.elasticsearch.action.search.SearchTask)) && args(request,task)")
  public Object executeFetchPhase(final ProceedingJoinPoint joinPoint, final ShardSearchTransportRequest request, final org.elasticsearch.action.search.SearchTask task)
  {
    try
    {
      logMethodSignature(joinPoint);
      LOGGER.error("### request: {}", request);
      return joinPoint.proceed();
    }
    catch (Throwable e)
    {
      LOGGER.error("An error occurred.", e);
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception.
    }
  }
  */
  
  private void logMethodSignature(final ProceedingJoinPoint joinPoint)
  {
    LOGGER.error("### executing {}.{}", joinPoint.getSignature().getDeclaringTypeName(), joinPoint.getSignature().getName());
    LOGGER.error("### params: {}", joinPoint.getArgs());
  }

  private static final Logger LOGGER = ESLoggerFactory.getLogger(SearchServiceAspect.class);
}

