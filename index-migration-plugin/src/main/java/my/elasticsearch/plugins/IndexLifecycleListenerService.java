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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                                final ClusterName clusterName,
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
        synchronized (SYNC_LOCK)
        {          
          //enumerateExistingIndexes();
          final String indexName = indexShard.shardId().getIndex();
          final boolean newIndex = !existingIndexes.contains(indexName);
          LOGGER.error("### afterIndexShardCreated: {} - New Index [{}]", indexShard.shardId(), newIndex);
          
          final String targetIndexName = configurationService.targetIndexToBeSynchronizedWith(indexName);
          if(targetIndexName == null)
          {
            super.afterIndexShardCreated(indexShard);
            return;
          }

          LOGGER.error("### Need to synchronize documents from index [{}] with index [{}]", indexName, targetIndexName);
          //LOGGER.error("### Index settings: {}", indexShard.indexSettings().toDelimitedString(','));
          final IndexService targetIndexService = indicesService.indexService(targetIndexName);
          if(targetIndexService != null)
          {
            indexSynchronizers.add(new IndexSynchronizer(indexShard.indexingService(), targetIndexService.shard(0)));
          }
          // TODO: Ajey - When this index is being loaded the the target index may exists but not yet loaded.
          // In such case we cannot create the index.
          /*
          IndexService targerIndexService = indicesService.indexService(targetIndexName); 
          if (targerIndexService == null)
          { 
            final Settings settings = ImmutableSettings.builder().put("index.number_of_shards", 1)
                                                                 .put("index.version.created", indexShard.indexSettings().getAsInt("index.version.created", 1060299))
                                                                 .build();
            
            targerIndexService = indicesService.createIndex(targetIndexName, settings, indexShard.nodeName());
            final IndexShard targetIndexShard = targerIndexService.createShard(0, true);            
            LOGGER.error("### Created [{}] : state [{}]", targetIndexName, targetIndexShard.state());            
          }*/
          
          super.afterIndexShardCreated(indexShard);
        }
      }
      
      @Override
      public void afterIndexShardDeleted(final ShardId shardId, final Settings indexSettings)
      {
        synchronized (SYNC_LOCK)
        {
          //enumerateExistingIndexes();
          final String indexName = shardId.getIndex();
          final boolean newIndex = !existingIndexes.remove(indexName);
          LOGGER.error("### afterIndexShardDeleted: {} - New Index [{}]", shardId, newIndex);
          super.afterIndexShardDeleted(shardId, indexSettings);
        }
      }
    };
  }

  private void enumerateExistingIndexes(final String clusterName)
  {
    
    // TODO: Ajey - Enumerating using this.indicesService.iterator() does not work since when we call this from ctor then the node is not initialized yet.
    // Hence, as a work around enumerating the index folders.
    // Tried using LocalGateway but that results in circular dependency error.
    LOGGER.error("### Enumerating existing indexes : {}", clusterName);
    /*
    for (IndexService indexService : this.indicesService)
    {
      final String existingIndex = indexService.index().getName();
      LOGGER.error("### Existing index: {}", existingIndex);
      this.existingIndexes.add(existingIndex);
    }*/
        
    // TODO: Ajey - Need to get the data directory path from settings
    final File dataDirectory = new File(String.format("E:\\Binaries\\elasticsearch-1.6.2\\data\\%s\\nodes\\0\\indices\\", clusterName));
    for (String indexFolderName : dataDirectory.list())
    {
      LOGGER.error("### Existing index: {}", indexFolderName);
      this.existingIndexes.add(indexFolderName);
    }
  }
  
  @Override
  protected void doStart() throws ElasticsearchException
  {
    this.indicesService.indicesLifecycle().addListener(this.listener);

    enumerateExistingIndexes(this.settings.get("cluster.name"));
  }

  @Override
  protected void doStop() throws ElasticsearchException
  {
    LOGGER.error("### Closing...");
  }

  @Override
  protected void doClose() throws ElasticsearchException
  {
    LOGGER.error("### Closing...");
    if(this.listener != null)
    {
      this.indicesService.indicesLifecycle().removeListener(this.listener);
      this.listener = null;
      
      for (IndexSynchronizer synchronizer : this.indexSynchronizers)
      {
        synchronizer.close();
      }
    }
  }
  
  // Private
  private Listener listener;
  private final Client client;
  private final IndicesService indicesService;
  private final IndexMigrationConfigurationService configurationService;
  private final Set<String> existingIndexes = new HashSet<>(); // Ok to use HashSet since index name is always lower case.
  private final List<IndexSynchronizer> indexSynchronizers = new ArrayList<>();
  private final Object SYNC_LOCK = new Object();
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexLifecycleListener");
}

