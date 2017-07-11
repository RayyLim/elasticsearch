/********************************************************************
 * File Name:    EsIndexReaderTests.java
 *
 * Date Created: Jun 28, 2017
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2017 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import my.elasticsearch.EsIndexReader.Documents;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;
import org.junit.Test;
  
public class EsIndexReaderTests
{

  @Test
  public void test() throws IOException
  {
    //final String indexName = "migration_index_01";
    //final String indexName = "ajey_migrate_01";
    final String indexName = "epmp_trackdb";
    //final String esDataDirectory = "E:\\Binaries\\Migration\\elasticsearch-1.7.1\\data\\elasticsearch.atp";
    final String esDataDirectory = "E:\\Binaries\\atp_data\\elasticsearch.atp";
    
    final EsIndexReader reader = new EsIndexReader(esDataDirectory);
    final Documents documents = reader.read(indexName, false);
    int nCount = 0;
    for (EsDocument document : documents)
    {
      ++nCount;
      System.out.println(document);
    }
    System.out.println(String.format("Total docs = %s : max. doc Id = %d : Deleted docs = %s", documents.totalDocs(), documents.maxDocId(), documents.deletedDocs()));
    System.out.println("Total document enumerated = " + nCount);
  }
}

