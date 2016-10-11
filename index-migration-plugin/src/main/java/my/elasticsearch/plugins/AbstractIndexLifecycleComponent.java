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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
  
public abstract class AbstractIndexLifecycleComponent<T> extends AbstractLifecycleComponent<T>
{
  protected AbstractIndexLifecycleComponent(final Settings settings,
                                            final IndicesService indicesService,
                                            final String indexName) 
  {
    super(settings);
    
    this.indicesService = indicesService;
    this.indexName = indexName;
  }
  
  @Override
  protected void doStart() throws ElasticsearchException
  {
    logger.error("### doStart...");
    
    this.indexStartStopListener = getIndexStartStopListener();
    this.indicesService.indicesLifecycle().addListener(this.indexStartStopListener);
    
    // If the index has already been created then we won't get the start notification.
    // Let's check manually.
    final IndexService indexService = this.indicesService.indexService(indexName);
    if(indexService == null)
       return;
    
    AbstractIndexLifecycleComponent.this.afterIndexShardStarted(indexService.shard(0)); // TODO: Ajey - We are considering single share scenario only for now.
  }
  
  @Override
  protected void doStop() throws ElasticsearchException
  {
    logger.error("### doStop...");
    if(this.indexStartStopListener != null)
    {
      this.indicesService.indicesLifecycle().removeListener(this.indexStartStopListener);
      this.indexStartStopListener = null;
      
      logger.error("### Stopped monitoring start/stop operations for index {}.", this.indexName);
    }
  }

  @Override
  protected void doClose() throws ElasticsearchException
  {
  }
  
  protected Listener getIndexStartStopListener()
  {
    return new Listener()
    {
      @Override
      public void afterIndexShardStarted(final IndexShard indexShard)
      {
        if(!isIndexToBeWatched(indexShard.shardId().getIndex()))
           return;
        
        logger.error("### afterIndexShardStarted called for {}", indexShard.shardId());
        
        AbstractIndexLifecycleComponent.this.afterIndexShardStarted(indexShard);
      }
      
      @Override
      public void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings)
      {
        if(!isIndexToBeWatched(shardId.getIndex()))
           return;
        
        logger.error("### afterIndexShardDeleted called for {}", shardId);
        
        AbstractIndexLifecycleComponent.this.afterIndexShardDeleted(shardId, indexSettings);
      }
    };
  }

  protected boolean isIndexToBeWatched(final String sourceIndexName)
  {
    //logger.error("### isIndexToBeWatched({}, {})", this.indexName, sourceIndexName);
    
    return this.indexName.equals(sourceIndexName); // TODO: Ajey - Should consider only primary shard.
  }

  protected abstract void afterIndexShardStarted(final IndexShard indexShard);
  protected abstract void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings);

  protected Listener indexStartStopListener;
  protected final IndicesService indicesService;
  protected final String indexName;
}

