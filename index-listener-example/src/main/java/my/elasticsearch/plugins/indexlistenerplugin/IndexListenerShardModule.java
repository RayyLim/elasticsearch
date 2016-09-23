/********************************************************************
 * File Name:    IndexListenerShardModule.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins.indexlistenerplugin;

import org.elasticsearch.common.inject.AbstractModule;
  
public class IndexListenerShardModule extends AbstractModule
{
  @Override
  protected void configure()
  {
    bind(IndexListenerService.class).asEagerSingleton();
  }
}

