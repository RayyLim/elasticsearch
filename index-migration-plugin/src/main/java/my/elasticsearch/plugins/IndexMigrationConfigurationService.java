/********************************************************************
 * File Name:    IndexMigrationConfigurationService.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;

public class IndexMigrationConfigurationService extends AbstractIndexingOperationsListenerComponent<IndexMigrationConfigurationService>
{
  @Inject
  public IndexMigrationConfigurationService(final Settings settings, 
                                            final IndicesService indicesService,
                                            final MetaDataCreateIndexService metaDataCreateIndexService,
                                            final ClusterService clusterService,
                                            final Client client,
                                            final ScriptService scriptService)
  {
    super(settings, indicesService, MIGRATION_INDEX);
    
    this.metaDataCreateIndexService = metaDataCreateIndexService;
    this.clusterService = clusterService;
    this.client = client;
    this.scriptService = scriptService;
  }
  
  @Override
  protected Create preCreateDocument(final Create create)
  {
    // TODO: Ajey - Throw custom exception
    throw new RuntimeException(String.format("Create the migration configuration as /%s/sourceIndex/targetIndex", MIGRATION_INDEX));
  }

  @Override
  protected Index preIndexDocument(final Index index)
  {
    // Both type and id should be valid index names since type maps to source index and id to target index.
    try
    {
      validateIndexName(index.type());
      validateIndexName(index.id());
      return super.preIndexDocument(index);
    }
    catch (Exception e)
    {
      // TODO: Ajey - Throw custom exception
      logger.error("An error occurred while adding migration configuration.", e);
      throw new RuntimeException(String.format("Create the migration configuration as /%s/sourceIndex/targetIndex with sourceIndex & targetIndex a valid index name.", MIGRATION_INDEX), e);
    }
  }
  
  // TODO: Ajey - Added support for 
  /*
   * - enumerating existing configuration and start the sync.
   * - stop existing sync using enable flag.
   * - support for matching index name using start with instead of exact match.
   * 
   */
  @Override
  protected void postIndexDocument(final Index index)
  {
    final IndexMigrationConfig configuration = new IndexMigrationConfig(index);
    logger.error("### {}", configuration);

    synchronized (SYNC_BLOCK)
    {
      IncomingDocumentSynchronizer existingIndexSynchronizer = this.indexSynchronizers.remove(configuration);
      if(existingIndexSynchronizer != null)
      {
        logger.error("### Index migration configuration has been updated.");
        existingIndexSynchronizer.close();
      }
      
      final IncomingDocumentSynchronizer synchronizer = new IncomingDocumentSynchronizer(this.settings, 
                                                                                         this.indicesService, 
                                                                                         configuration, 
                                                                                         this.client,
                                                                                         this.scriptService);
      synchronizer.start();
      this.indexSynchronizers.put(configuration, synchronizer);
      
      // Test migration of existing document. Remove !!!
      /*
      if(this.existingDoumentsSynchronizer != null)
      {
        this.existingDoumentsSynchronizer.close();
      }
      
      this.existingDoumentsSynchronizer = new ExistingDoumentsSynchronizer(this.client, configuration);
      this.existingDoumentsSynchronizer.synchronize();
      */
    }
  }
  
  private void validateIndexName(final String indexName)
  {
    try
    {
      metaDataCreateIndexService.validateIndexName(indexName, clusterService.state());
    }
    catch(IndexAlreadyExistsException e)
    {
      // Source and/or target index can exist so we suppress this exception.
    }
  }

  // Private
  private ExistingDoumentsSynchronizer existingDoumentsSynchronizer; 
  
  private final MetaDataCreateIndexService metaDataCreateIndexService;
  private final ClusterService clusterService;
  private final Client client;
  private final ScriptService scriptService;
  
  private final Map<IndexMigrationConfig, IncomingDocumentSynchronizer> indexSynchronizers = new HashMap<>();
  
  private final Object SYNC_BLOCK = new Object();
  
  private final static String MIGRATION_INDEX = "es_migration";
}

