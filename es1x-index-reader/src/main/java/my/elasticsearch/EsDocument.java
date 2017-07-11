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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lang3.builder.EqualsBuilder;
import org.elasticsearch.common.lang3.builder.HashCodeBuilder;
import org.elasticsearch.common.lang3.builder.ToStringBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

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
  
  @Override
  public boolean equals(final Object target)
  {
    if(!(target instanceof EsDocument))
      return super.equals(target);

    final EsDocument rhs = (EsDocument) target;
    return new EqualsBuilder().append(this.type, rhs.type)
                              .append(this.id, rhs.id)
                              .append(this.version, rhs.version)
                              .append(getSourceAsMap(this), getSourceAsMap(rhs)) // Bytes can be out of order so using map.
                              .build();
  }
  
  @Override
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.type)
                                .append(this.id)
                                .append(this.version)
                                .append(getSourceAsMap(this)) // Only adding id should suffice if id is going to be different always. No need to add source at least because then equals will take care of comparing properly if id clashes.
                                .build();
  }
  
  private static Map<String, Object> getSourceAsMap(final EsDocument document)
  {
    try
    {
      return JsonXContent.jsonXContent.createParser(document.source).map();
    }
    catch (IOException e)
    {
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception !!!
    }
  }
  
  private final String id;
  private final String type;
  private final BytesReference source;
  private long version = -1L;
}

