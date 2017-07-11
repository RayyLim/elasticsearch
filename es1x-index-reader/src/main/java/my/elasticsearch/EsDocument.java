/********************************************************************
 * File Name:    EsDocument.java
 *
 * Date Created: Jun 29, 2017
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2017 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch;

import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lang3.builder.ToStringBuilder;
  
public class EsDocument
{
  public EsDocument(final String id, final String type, final BytesReference source)
  {
    this.id = id;
    this.type = type;
    this.source = source;
  }
  
  public String getId()
  {
    return this.id;
  }
  
  public String getType()
  {
    return this.type;
  }
  
  public BytesReference getSource()
  {
    return this.source;
  }
  
  public long getVersion()
  {
    return this.version;
  }

  public void setVersion(final long version)
  {
    this.version = version;
  }
  
  @Override
  public String toString()
  {
    return new ToStringBuilder(this).append("id", this.id)
                                    .append("type", this.type)
                                    .append("version", this.version)
                                    .append("source", this.source.toUtf8()) // TODO: Ajey - Should not log source
                                    .build();
  }
  
  private final String id;
  private final String type;
  private final BytesReference source;
  private long version = -1L;
}

