/********************************************************************
 * File Name:    IncomingDocumentSynchronizer.java
 *
 * Date Created: Sep 26, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
  
public class IncomingDocumentSynchronizer extends AbstractIndexingOperationsListenerComponent<IncomingDocumentSynchronizer>
{
  protected IncomingDocumentSynchronizer(final Settings settings, 
                                         final IndicesService indicesService, 
                                         final IndexMigrationConfig migrationConfig,
                                         final Client client,
                                         final ScriptService scriptService)
  {
    super(settings, indicesService, migrationConfig.sourceIndex());
    
    this.migrationConfig = migrationConfig;
    this.client = client;
    this.scriptService = scriptService;
  }
  
  @Override
  protected void postCreateDocument(final Create create)
  {
    synchronizeDocument(create);
  }
  
  @Override
  protected void postIndexDocument(final Index index)
  {
    synchronizeDocument(index);
  }
  
  // TODO: Ajey - Handle delete !!!
  
  private void synchronizeDocument(final IndexingOperation currentDocumentInfo)
  {
    logger.error("### Synchronizing document from {} to {}", this.migrationConfig.sourceIndex(), this.migrationConfig.targetIndex());
    
    // TODO: Ajey - In case we use startsWith for index matching then the target index should be formed accordingly.
    
    final IndexRequestBuilder request = 
    this.client.prepareIndex(this.migrationConfig.targetIndex(), currentDocumentInfo.type(), currentDocumentInfo.id())
               .setVersion(currentDocumentInfo.version())
               .setVersionType(VersionType.EXTERNAL); // Using external version to make sure that we always sync with the latest copy.
    
    this.addSource(request, currentDocumentInfo);
    
    request.execute(new ActionListener<IndexResponse>() {
      
                    @Override
                    public void onResponse(final IndexResponse response)
                    {
                    }
                    
                    @Override
                    public void onFailure(Throwable e)
                    {// TODO: Ajey - add more details like source, target or migration configuration to get more context.
                      logger.error("### Error synching document.", e);                      
                    }
              });
  }
  
  @SuppressWarnings("unchecked")
  private void addSource(final IndexRequestBuilder request, final IndexingOperation currentDocumentInfo)
  {
    if(this.migrationConfig.script() == null)
    {
      request.setSource(currentDocumentInfo.source());
      return;
    }
    
    // These scripts are compiled already. We cannot cache this since we will be setting the variable as source on each run.
    final ExecutableScript executableScript = this.scriptService
                                                   .executable("groovy", 
                                                               this.migrationConfig.script(), 
                                                               ScriptType.FILE, 
                                                               ScriptContext.Standard.UPDATE, 
                                                               null);
    
    final Map<String, Object> source = XContentHelper.convertToMap(currentDocumentInfo.source(), false).v2();
    
    executableScript.setNextVar("_source", source);
    
    final Map<String, Object> transformedSource = (Map<String, Object>) executableScript.run(); // TODO: Ajey - if script return null then we skip adding this source
    logger.error("### Script output: {}", transformedSource);
    
    request.setSource(transformedSource);
  }

  private final IndexMigrationConfig migrationConfig;
  private final Client client;
  private ScriptService scriptService;
}

