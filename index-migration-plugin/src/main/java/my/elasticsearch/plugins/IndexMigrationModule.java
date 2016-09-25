/********************************************************************
 * File Name:    IndexMigrationModule.java
 *
 * Date Created: Sep 23, 2016
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2016 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch.plugins;

import org.elasticsearch.common.inject.AbstractModule;
  
public class IndexMigrationModule extends AbstractModule
{
  @Override
  protected void configure()
  {
    bind(IndexMigrationConfigurationService.class).asEagerSingleton();
  }
}

