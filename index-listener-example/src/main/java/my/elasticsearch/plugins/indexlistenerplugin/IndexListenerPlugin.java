/********************************************************************
 * File Name:    IndexListenerPlugin.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins.indexlistenerplugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.plugins.AbstractPlugin;
  
public class IndexListenerPlugin extends AbstractPlugin
{  
  public IndexListenerPlugin()
  {
    LOGGER.error("### IndexListenerPlugin created...");
  }
  
  @Override
  public String description()
  {
    return "Plugin to listen for index actions.";
  }

  @Override
  public String name()
  {
    return "index-listener-plugin";
  }
  
  @Override
  public void processModule(final Module module) 
  {
    //LOGGER.error("### processModule called with [{}]", module);
  }
  
  @Override
  public Collection<Class<? extends Module>> shardModules()
  {
    final List<Class<? extends Module>> modules = new ArrayList<>();
    
    modules.add(IndexListenerShardModule.class);
    
    return modules;
  }
  
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexListenerPlugin");
}

