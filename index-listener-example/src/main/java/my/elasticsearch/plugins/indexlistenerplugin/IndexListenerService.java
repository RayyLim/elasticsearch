/********************************************************************
 * File Name:    IndexListenerService.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins.indexlistenerplugin;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
  
public class IndexListenerService extends AbstractIndexShardComponent implements AutoCloseable
{
  @Inject
  protected IndexListenerService(final ShardId shardId, 
                                 final Settings indexSettings, 
                                 final String prefixSettings,
                                 final ShardIndexingService shardIndexingService)
  {
    super(shardId, indexSettings, prefixSettings);

    LOGGER = Slf4jESLoggerFactory.getLogger("IndexListenerService - " + shardId);
    
    this.shardIndexingService = shardIndexingService;
    
    if(! shardId.getIndex().startsWith(".marvel") ) // Ignore the marvel plug-in notifications
    {
      this.indexingOperationListener = getIndexingOperationListener();
      this.shardIndexingService.addListener(this.indexingOperationListener);
    }
  }
  
  private IndexingOperationListener getIndexingOperationListener()
  {
    return new IndexingOperationListener()
    {
      @Override
      public void postCreate(final Create create)// Create is called when document is created and ID is auto-generated
      {
        logDocumentInfo("postCreate", create);
        super.postCreate(create);
      }
      
      @Override
      public void postIndex(final Index index) // Index is called when document is created using specified id or existing document is updated
      {
        logDocumentInfo("postIndex", index);
        super.postIndex(index);
      }
      
      private void logDocumentInfo(final String context, final IndexingOperation operation)
      {
        LOGGER.error("### {}\n_type: {}\n_id: {}\n_version: {}\n_versionType: {}\nsource: \n{}",
                     context,
                     operation.type(), 
                     operation.id(),
                     operation.version(),
                     operation.versionType(),
                     operation.parsedDoc().source().toUtf8());
      }
    };
  }

  @Override
  public void close() throws Exception
  {
    LOGGER.error("### Closing service");
    
    if(this.indexingOperationListener != null)
    {
      this.shardIndexingService.removeListener(this.indexingOperationListener);
      this.indexingOperationListener = null;
    }
  }

  private final ShardIndexingService shardIndexingService;
  private IndexingOperationListener indexingOperationListener;
  
  private final ESLogger LOGGER;
}

