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

import java.io.InputStream;

import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.jackson.core.JsonParser;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
  
public class IndexMigrationConfigurationService
{
  @Inject
  public IndexMigrationConfigurationService()
  {
    try
    {
      ClasspathResourceLoader loader = new ClasspathResourceLoader();
      try(final InputStream stream = loader.openResource("migration.properties")) // TODO: Ajey - Load this from external folder.
      {
        final JsonFactory factory = new JsonFactory();
        try(JsonParser parser = factory.createParser(stream))
        {
          //parser.getText()
        }
      }
    }
    catch (Exception e)
    {
      LOGGER.error("Error loading configuration.", e);
    }
  }
  
  public String targetIndexToBeSynchronizedWith(final String sourceIndex)
  {
    if("source_01".equals(sourceIndex))
    {
      return "target_01";
    }
    
    return null;
  }
  
  private final static ESLogger LOGGER = Slf4jESLoggerFactory.getLogger("IndexMigrationConfigurationService");
}

