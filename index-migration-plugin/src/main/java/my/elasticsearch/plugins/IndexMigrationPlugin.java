/********************************************************************
 * File Name:    IndexMigrationPlugin.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.search.type.TransportSearchHelper;
import org.elasticsearch.action.search.type.TransportSearchQueryAndFetchAction;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;
  
public class IndexMigrationPlugin extends AbstractPlugin
{
  public IndexMigrationPlugin()
  {
    LOGGER.error("### Created...");
  }

  @Override
  public String name()
  {
    return "index-migration-plugin";
  }

  @Override
  public String description()
  {
    return "Plugin to migrate documents from one index to another doing required transformations.";
  }
  
  @Override
  public Collection<Class<? extends Module>> modules()
  {
    final List<Class<? extends Module>> modules = new ArrayList<>();
    
    modules.add(IndexMigrationModule.class);
    
    return modules;
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public Collection<Class<? extends LifecycleComponent>> services()
  {
    final List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
    
    services.add(IndexMigrationConfigurationService.class);

    return services;
  }

/*  
  @Override
  public void processModule(final Module module)
  {
    //SearchServiceListener<T>
    SearchService service;
    TransportService t;
    TransportSearchQueryAndFetchAction a;
    TransportClientNodesService t;
    TransportSearchModule m;
    
    LOGGER.error("### Module: {}", module);
    super.processModule(module);
  }
*/  
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexMigrationPlugin");
}

