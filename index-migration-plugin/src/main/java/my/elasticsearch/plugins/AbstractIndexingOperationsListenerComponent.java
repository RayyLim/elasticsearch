/********************************************************************
 * File Name:    AbstractIndexingOperationsListenerComponent.java
 *
 * Date Created: Sep 26, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
  
public abstract class AbstractIndexingOperationsListenerComponent<T> extends AbstractIndexLifecycleComponent<T>
{
  protected AbstractIndexingOperationsListenerComponent(final Settings settings, 
                                                        final IndicesService indicesService, 
                                                        final String indexName)
  {
    super(settings, indicesService, indexName);
  }

  @Override
  protected void afterIndexShardStarted(final IndexShard indexShard)
  {
    this.indexingOperationListener = getIndexingOperationListener();
    
    indexShard.indexingService().addListener(this.indexingOperationListener);
    
    logger.error("### Started monitoring indexing operations for index {}.", this.indexName);        
  }

  @Override
  protected void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings)
  {
    this.indexingOperationListener = null;
    
    // TODO: Ajey - Will the listener be removed since the index is deleted?
    
    logger.error("### Stopped monitoring indexing operations for {} since the index is deleted.", this.indexName);        
  }
  
  @Override
  protected void doStop() throws ElasticsearchException
  {
    if(this.indexingOperationListener != null)
    {
      final IndexService indexService = this.indicesService.indexService(this.indexName);
      if(indexService != null)
      {
        // TODO: Ajey - We are considering only index with 1 shard.
        indexService.shard(0).indexingService().removeListener(this.indexingOperationListener);
        
        logger.error("### Stopped monitoring indexing operations for index {}.", this.indexName);        
      }
    }
    super.doStop();
  }
  
  protected IndexingOperationListener getIndexingOperationListener()
  {
    return new IndexingOperationListener()
    {
      @Override
      public Create preCreate(final Create create)
      {
        logDocumentInfo("### preCreate", create);
        return preCreateDocument(create);
      }
      
      @Override
      public void postCreate(final Create create)
      {
        logDocumentInfo("### postCreate", create);
        postCreateDocument(create);
      }
      
      @Override
      public Index preIndex(final Index index)
      {
        logDocumentInfo("### preIndex", index);
        return preIndexDocument(index);
      }
      
      @Override
      public void postIndex(final Index index)
      {
        logDocumentInfo("### postIndex", index);
        postIndexDocument(index);
      }
      
      @Override
      public Delete preDelete(Delete delete)
      {
        return preDeleteDocument(delete);
      }
      
      @Override
      public void postDelete(final Delete delete)
      {
        postDeleteDocument(delete);
      }
    };
  }
  
  protected Engine.Create preCreateDocument(final Engine.Create create)
  {
    return create;
  }
  
  protected void postCreateDocument(final Create create)
  {    
  }

  protected Index preIndexDocument(final Index index)
  {
    return index;
  }
  
  protected void postIndexDocument(final Index index)
  {    
  }

  protected Delete preDeleteDocument(final Delete delete)
  {
    return delete;
  }
  
  protected void postDeleteDocument(final Delete delete)
  {    
  }
  
  private void logDocumentInfo(final String context, final IndexingOperation operation)
  {
    logger.error("### {}\n_type: {}\n_id: {}\n_version: {}\n_versionType: {}\nstartTime: {}\nOrigin: {}\nsource: \n{}",
                 context,
                 operation.type(), 
                 operation.id(),
                 operation.version(),
                 operation.versionType(),
                 operation.startTime(),
                 operation.origin(),
                 operation.parsedDoc().source().toUtf8());
  }
  
  protected IndexingOperationListener indexingOperationListener;
}

