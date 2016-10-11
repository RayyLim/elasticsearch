/********************************************************************
 * File Name:    IndexMigrationConfig.java
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

import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.lang3.builder.EqualsBuilder;
import org.elasticsearch.common.lang3.builder.HashCodeBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine.IndexingOperation;
  
public class IndexMigrationConfig
{
  public IndexMigrationConfig(final IndexingOperation indexingOperation)
  {
    this.indexingOperation = indexingOperation;
    this.configuration = XContentHelper.convertToMap(indexingOperation.source(), false).v2();

    // We use source and target index name for uniqueness. Whether the same configuration has changed or not can be determined using version.
    this._hashCode = new HashCodeBuilder().append(this.sourceIndex())
                                          .append(this.targetIndex())
                                          .build();
  }
  
  public String sourceIndex()
  {
    return this.indexingOperation.type();
  }
  
  public String targetIndex()
  {
    return this.indexingOperation.id();
  }
  
  public boolean enabled()
  {
    final Boolean enabled = (Boolean) this.configuration.get("enabled"); 
    return (enabled != null ? enabled : true);
  }
  
  public boolean exactMatchIndexName()
  {
    final Boolean exactMatchIndexName = (Boolean) this.configuration.get("exactMatchIndexName"); 
    return (exactMatchIndexName != null ? exactMatchIndexName : true);
  }
  
  public String script()
  {
    return (String) this.configuration.get("script");
  }
  
  public long version()
  {
    return this.indexingOperation.version();
  }
  
  @Override
  public int hashCode()
  {
    return this._hashCode;
  }
  
  @Override
  public boolean equals(final Object target)
  {
    final IndexMigrationConfig rhs = (IndexMigrationConfig) target;
    if(rhs == null)
       return false;
    
    return new EqualsBuilder().append(this.sourceIndex(), rhs.sourceIndex())
                              .append(this.targetIndex(), rhs.targetIndex())
                              .build();
  }
  
  @Override
  public String toString()
  {
    return new ToStringBuilder(this.getClass().getSimpleName())
               .add("sourceIndex", this.sourceIndex())
               .add("targetIndex", this.targetIndex())
               .add("version", this.version())
               .add("enabled", this.enabled())
               .add("script", this.script())
               .add("exactMatchIndexName", this.exactMatchIndexName())
               .toString();
  }
  
  private final IndexingOperation indexingOperation;
  private final Map<String, Object> configuration;
  private final int _hashCode;
}

