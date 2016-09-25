/********************************************************************
 * File Name:    IndexSynchronizer.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
  
public class IndexSynchronizer implements AutoCloseable
{
  public IndexSynchronizer(final ShardIndexingService sourceIndexingService,
                           final String targetIndex,
                           final Client client)
  
  {
    this.sourceIndexingService = sourceIndexingService;
    this.targetIndex = targetIndex;
    this.client = client;
    
    final String sourceIndex = this.sourceIndexingService.shardId().getIndex();
    
    LOGGER.error("### Synchronizing documents from [{}] to [{}]", sourceIndex, targetIndex);
    
    this.indexingOperationListener = getIndexingOperationListener();
    
    this.sourceIndexingService.addListener(this.indexingOperationListener);
  }

  private IndexingOperationListener getIndexingOperationListener()
  {
    return new IndexingOperationListener()
    {
      @Override
      public void postCreate(final Create create)
      {
        logDocumentInfo("### postCreate", create);
        
        syncDocument(create);
      }
      
      @Override
      public void postIndex(final Index index)
      {
        logDocumentInfo("### postIndex", index);

        syncDocument(index);
      }

      private void syncDocument(final IndexingOperation sourceRequest)
      {
        client.prepareIndex(targetIndex, sourceRequest.type(), sourceRequest.id())
              .setSource(sourceRequest.source())
              .setVersion(sourceRequest.version())
              .setVersionType(VersionType.EXTERNAL) // Using external version to make sure that we always sync with the latest copy.
              .execute().addListener(new ActionListener<IndexResponse>()
              {                
                @Override
                public void onResponse(IndexResponse response)
                {
                }
                
                @Override
                public void onFailure(Throwable e)
                {
                  LOGGER.error("### Sync failed", e);
                }
              });
      }
      private void logDocumentInfo(final String context, final IndexingOperation operation)
      {
        LOGGER.error("### {}\n_type: {}\n_id: {}\n_version: {}\n_versionType: {}\nstartTime: {}\nOrigin: {}\nsource: \n{}",
                     context,
                     operation.type(), 
                     operation.id(),
                     operation.version(),
                     operation.versionType(),
                     operation.startTime(),
                     operation.origin(),
                     operation.parsedDoc().source().toUtf8());
      }
    };
  }

  @Override
  public void close()
  {
    if(this.indexingOperationListener != null)
    {
      this.sourceIndexingService.removeListener(this.indexingOperationListener);
      this.indexingOperationListener = null;
      LOGGER.error("### Removed indexing operation listener for {}", this.sourceIndexingService.shardId());
    }      
  }
  
  // Private
  private IndexingOperationListener indexingOperationListener;
  private final ShardIndexingService sourceIndexingService;
  private final String targetIndex;
  private final Client client;
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexSynchronizer");
}

