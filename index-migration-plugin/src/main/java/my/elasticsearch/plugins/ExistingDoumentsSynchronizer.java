/********************************************************************
 * File Name:    ExistingDoumentsSynchronizerService.java
 *
 * Date Created: Sep 27, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
  
public class ExistingDoumentsSynchronizer implements AutoCloseable
{
  public ExistingDoumentsSynchronizer(final Client client, final IndexMigrationConfig config)
  {
    this.client = client;
    this.config = config;
  }

  @Override
  public void close()
  {
    this.stopSynchronization.getAndSet(true);
  }
  
  public void synchronize()
  {
    LOGGER.error("### Synchronizing existing documents from {} to {}.", this.config.sourceIndex(), this.config.targetIndex());

    this.stopWatch.start(String.format("Synchronizing document from %s to %s.", this.config.sourceIndex(), this.config.targetIndex()));
    
    client.prepareSearch(config.sourceIndex())
          .setQuery(QueryBuilders.matchAllQuery())
          .setScroll(new Scroll(TimeValue.timeValueSeconds(60)))
          .setSearchType(SearchType.SCAN)
          .setSize(10000)
          .setVersion(true)
          .execute(new ActionListener<SearchResponse>()
          {                  
            @Override
            public void onResponse(final SearchResponse response)
            {
              //LOGGER.error("### Search result: {}", response);  
              LOGGER.error("### Total {} documents to be migrated from {} to {}. ScrollID: {}", 
                           response.getHits().getTotalHits(),
                           config.sourceIndex(),
                           config.targetIndex(),
                           response.getScrollId());
              
              synchronize(response.getScrollId(), 0);
            }
            
            @Override
            public void onFailure(final Throwable e)
            {
              LOGGER.error("Error during search.", e);
            }
          });
  }
  
  private void synchronize(final String scrollId, final long nMigratedDocuments)
  {
    if(this.stopSynchronization.get())
    {
      LOGGER.error("### Stopping synchronization...");
      return;
    }
    
    this.client.prepareSearchScroll(scrollId)
               .setScroll(TimeValue.timeValueMinutes(1))
               .execute(new ActionListener<SearchResponse>()
              {
                
                @Override
                public void onResponse(final SearchResponse response)
                {
                  //LOGGER.error("### Search result: {}", response);  
                  
                  if(response.getHits().getHits().length == 0)
                  {
                    LOGGER.error("### Done migrating all documents...");
                    
                    stopWatch.stop();

                    logTartgetIndexDocumentCount();
                    
                    stopWatch.start("Flusing the target index.");

                    LOGGER.error("### flusing the target index...");

                    client.admin().indices().prepareFlush(config.targetIndex()).get();
                    
                    stopWatch.stop();
                    stopWatch.start("Get document count in target index.");
                    
                    logTartgetIndexDocumentCount();

                    stopWatch.stop();
                    LOGGER.error("\n{}", stopWatch.prettyPrint());
                    
                    logTotalTime();
                    
                    // TODO: Ajey - Delete the scroll ???
                    return;
                  }
                  
                  synchronize(response, nMigratedDocuments);
                }
                
                @Override
                public void onFailure(final Throwable e)
                {
                  LOGGER.error("Error during search.", e);
                }
              });
  }

  private void synchronize(final SearchResponse searchResponse, final long nMigratedDocuments)
  {
    final BulkRequestBuilder bulkRequestBuilder = this.client.prepareBulk();
    for (SearchHit record : searchResponse.getHits().getHits())
    {
      final IndexRequestBuilder indexRequestBuilder = this.client.prepareIndex(this.config.targetIndex(), record.getType(), record.getId())
                                                                 .setSource(transformSource(record.getSource()))
                                                                 .setVersion(record.version())
                                                                 .setVersionType(VersionType.EXTERNAL);
      bulkRequestBuilder.add(indexRequestBuilder);
    }
    
    bulkRequestBuilder.execute(new ActionListener<BulkResponse>()
    {
      @Override
      public void onResponse(final BulkResponse bulkResponse)
      {
        LOGGER.error("### Bulk Response: ", bulkResponse);

        final long nTotalDocuments = searchResponse.getHits().getTotalHits();
        final long nReturnedDocuments = searchResponse.getHits().getHits().length;
        
        LOGGER.error("### Migrated {}/{} documents. ScrollID: {}", nMigratedDocuments + nReturnedDocuments, nTotalDocuments, searchResponse.getScrollId());
        
        synchronize(searchResponse.getScrollId(), nMigratedDocuments + nReturnedDocuments);
      }
      
      @Override
      public void onFailure(Throwable e)
      {
        LOGGER.error("Error during bulk insert.", e);
      }
    });
  }

  private Map<String, Object> transformSource(final Map<String, Object> source)
  {
    final Map<String, Object> transformedSource = new HashMap<>();
    
    source.forEach(new BiConsumer<String, Object>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public void accept(final String fieldName, final Object fieldValue)
      {
        final String key = fieldName.replaceAll("\\.", "\\$");
        final Object value = (fieldValue instanceof Map ? transformSource((Map<String, Object>) fieldValue) : fieldValue);
        
        transformedSource.put(key, value);
      }
    });
    
    return transformedSource;
  }

  private void logTartgetIndexDocumentCount()
  {
    try
    {
      final SearchResponse response = this.client.prepareSearch(this.config.targetIndex())
                                                  .setQuery(QueryBuilders.matchAllQuery())
                                                  .setSize(0)
                                                  .execute().get();
      
      LOGGER.error("### Total documents in index {} = {}", config.targetIndex(), response.getHits().getTotalHits());
    }
    catch (Exception e)
    {
      LOGGER.error("Error retrieving document count.", e);
    }
  }

  private void logTotalTime()
  {
    LOGGER.error("### Total migration time (hh:mm:ss.SSS): {}", formatInterval(this.stopWatch.totalTime().getMillis()));    
  }

  private static String formatInterval(final long l)
  {
      final long hr = TimeUnit.MILLISECONDS.toHours(l);
      final long min = TimeUnit.MILLISECONDS.toMinutes(l - TimeUnit.HOURS.toMillis(hr));
      final long sec = TimeUnit.MILLISECONDS.toSeconds(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
      final long ms = TimeUnit.MILLISECONDS.toMillis(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
      return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms);
  }  
  
  // Private
  private final Client client;
  private final IndexMigrationConfig config;
  private final AtomicBoolean stopSynchronization = new AtomicBoolean(false);
  private final StopWatch stopWatch = new StopWatch("Index Migration");
  private final ESLogger LOGGER = ESLoggerFactory.getLogger("ExistingDoumentsSynchronizer");
}

