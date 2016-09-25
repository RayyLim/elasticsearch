/********************************************************************
 * File Name:    IndexLifecycleListenerService.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
import org.elasticsearch.indices.IndicesService;
  
public class IndexLifecycleListenerService extends AbstractLifecycleComponent<IndexLifecycleListenerService>
{
  @Inject
  public IndexLifecycleListenerService(final Settings settings,
                                       final IndicesService indicesService, 
                                       final IndexMigrationConfigurationService configurationService,
                                       final Client client)
  {    
    super(settings);
    
    this.indicesService = indicesService;
    this.listener = getListener();
    this.configurationService = configurationService;
    this.client = client;
    
    LOGGER.error("### Created...Client [{}]", this.client);
  }

  private Listener getListener()
  {
    return new Listener()
    {
      @Override
      public void afterIndexShardCreated(final IndexShard indexShard)
      {
        final String indexName = indexShard.shardId().getIndex();
        
        final String targetIndexName = configurationService.targetIndexToBeSynchronizedWith(indexName);
        if(targetIndexName == null)
        {
          super.afterIndexShardCreated(indexShard);
          return;
        }

        LOGGER.error("### Need to synchronize documents from index [{}] with index [{}]", indexName, targetIndexName);
        synchronized (SYNC_BLOCK)
        {
          indexSynchronizers.put(indexShard.shardId(), new IndexSynchronizer(indexShard.indexingService(), targetIndexName, client));
        }

        super.afterIndexShardCreated(indexShard);
      }
      
      @Override
      public void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings)
      {
        synchronized (SYNC_BLOCK)
        {
          final IndexSynchronizer indexSynchronizer = indexSynchronizers.remove(shardId);
          if(indexSynchronizer != null)
          {
            indexSynchronizer.close();
          }
        }  
        super.afterIndexShardDeleted(shardId, indexSettings);
      }
    };
  }

  @Override
  protected void doStart() throws ElasticsearchException
  {
    this.indicesService.indicesLifecycle().addListener(this.listener);
  }

  @Override
  protected void doStop() throws ElasticsearchException
  {
    LOGGER.error("### Closing...");
    this.doClose();
  }

  @Override
  protected void doClose() throws ElasticsearchException
  {
    LOGGER.error("### Closing...");
    if(this.listener != null)
    {
      this.indicesService.indicesLifecycle().removeListener(this.listener);
      this.listener = null;
      synchronized (SYNC_BLOCK)
      {
        this.indexSynchronizers.forEach(new BiConsumer<ShardId, IndexSynchronizer>() {
                                          @Override
                                          public void accept(final ShardId shardId, final IndexSynchronizer indexSynchronizer)
                                          {
                                            indexSynchronizer.close();
                                          }
                                        });
      }
    }
  }
  
  // Private
  private Listener listener;
  private final Client client;
  private final IndicesService indicesService;
  private final IndexMigrationConfigurationService configurationService;
  private final Map<ShardId, IndexSynchronizer> indexSynchronizers = new HashMap<>();// TODO: Ajey - needs to be in sync block
  private final Object SYNC_BLOCK = new Object();
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexLifecycleListener");
}

