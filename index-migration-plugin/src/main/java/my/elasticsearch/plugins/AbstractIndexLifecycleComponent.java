/********************************************************************
 * File Name:    AbstractIndexLifecycleComponent.java
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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
  
public abstract class AbstractIndexLifecycleComponent<T> extends AbstractLifecycleComponent<T>
{
  protected AbstractIndexLifecycleComponent(final Settings settings,
                                            final IndicesService indicesService,
                                            final String indexName, 
                                            final boolean exactMatchIndexName) 
  {
    super(settings);
    
    this.indicesService = indicesService;
    this.indexName = indexName;
    this.exactMatchIndexName = exactMatchIndexName;
  }
  
  @Override
  protected void doStart() throws ElasticsearchException
  {
    logger.error("### doStart...");
    
    this.indexStartStopListener = getIndexStartStopListener();
    this.indicesService.indicesLifecycle().addListener(this.indexStartStopListener);
  }
  
  @Override
  protected void doStop() throws ElasticsearchException
  {
    logger.error("### doStop...");
    doClose();
  }

  @Override
  protected void doClose() throws ElasticsearchException
  {
    logger.error("### doClose...");
  
    if(this.indexStartStopListener != null)
    {
      this.indicesService.indicesLifecycle().removeListener(this.indexStartStopListener);
    }
  }
  
  protected Listener getIndexStartStopListener()
  {
    return new Listener()
    {
      @Override
      public void afterIndexShardStarted(final IndexShard indexShard)
      {
        if(!isIndexToBeWatched(indexShard.shardId()))
           return;
        
        logger.error("### afterIndexShardStarted called for {}", indexShard.shardId());
        
        AbstractIndexLifecycleComponent.this.afterIndexShardStarted(indexShard);
      }
      
      @Override
      public void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings)
      {
        if(!isIndexToBeWatched(shardId))
           return;
        
        logger.error("### afterIndexShardDeleted called for {}", shardId);
        
        AbstractIndexLifecycleComponent.this.afterIndexShardDeleted(shardId, indexSettings);
      }
      
      private boolean isIndexToBeWatched(final ShardId shardId)
      {
        if(exactMatchIndexName && indexName.equals(shardId.getIndex())) // TODO: Ajey - Should consider only primary shard.
           return true;  
       
        return (!exactMatchIndexName && indexName.startsWith(shardId.getIndex()));
      }
    };
  }

  protected abstract void afterIndexShardStarted(final IndexShard indexShard);
  protected abstract void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings);

  protected Listener indexStartStopListener;
  protected final IndicesService indicesService;
  protected final String indexName;
  protected final boolean exactMatchIndexName;
}

