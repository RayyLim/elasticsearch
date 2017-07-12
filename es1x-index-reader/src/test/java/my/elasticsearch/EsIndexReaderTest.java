/********************************************************************
 * File Name:    EsIndexReaderTest.java
 *
 * Date Created: Jun 28, 2017
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2017 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import my.elasticsearch.EsIndexReader.Documents;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
  
public class EsIndexReaderTest
{
  @Test
  public void empty_index_Returns_Zero_Documents()
  {
    final Documents documents = getDocuments("empty_index", false);
    assertThat(documents.totalDocs()).as("Total documents").isEqualTo(0);
    assertThat(documents.deletedDocs()).as("Deleted documents").isEqualTo(0);
    assertThat(documents.maxDocId()).as("Max. document id").isEqualTo(0);
  }

  @Test
  public void small_data_index_with_single_type_Without_Version_Returns_3_Documents()
  {
    validate_small_data_index_with_single_type(false);
  }

  @Test
  public void small_data_index_with_single_type_With_Version_Returns_3_Documents()
  {
    validate_small_data_index_with_single_type(true);
  }

  @Test
  public void small_data_index_with_two_types_Without_Version_Returns_5_Documents()
  {
    validate_small_data_index_with_two_types(false);
  }

  @Test
  public void small_data_index_with_two_types_With_Version_Returns_5_Documents()
  {
    validate_small_data_index_with_two_types(true);
  }
  
  // To generate data for such test use the populate_documents.py script to generate data.
  // Run the script multiple times for update (delete + add) and immediately stop elasticsearch when the script completes.
  @Test
  public void index_with_deletes_Ignores_Deleted_Documents()
  {
    final Documents documents = getDocuments("index_with_deletes", false);
    assertThat(documents.totalDocs()).as("Total documents").isEqualTo(10000);
    assertThat(documents.deletedDocs()).as("Deleted documents").isEqualTo(2000);
    assertThat(documents.maxDocId()).as("Max. document id").isEqualTo(12000);
    
    int nCount = 0;
    for (EsDocument esDocument : documents)
    {
      ++nCount;
      LOGGER.debug("{}: {}", nCount, esDocument);
    }
    
    assertThat(nCount).as("Total documents enumerated").isEqualTo(10000); // If we don't ignore deleted docs then this count will be totalDocs + deletedDocs i.e. 12000
  }
  
  private void validate_small_data_index_with_single_type(final boolean nIncludeVersion)
  {
    final Documents documents = getDocuments("small_data_index_with_single_type", nIncludeVersion);
    assertThat(documents.totalDocs()).as("Total documents").isEqualTo(3);
    assertThat(documents.deletedDocs()).as("Deleted documents").isEqualTo(0);
    assertThat(documents.maxDocId()).as("Max. document id").isEqualTo(3);
    
    final Set<EsDocument> expectedEsDocuments = new HashSet<>(getExpectedEventDocumentsForSmallData(nIncludeVersion));
    
    int nCount = 0;
    for (EsDocument esDocument : documents)
    {
      ++nCount;
      LOGGER.debug("{}: {}", nCount, esDocument);
      
      assertThat(esDocument).as("Expected document").isIn(expectedEsDocuments);
    }
    
    assertThat(nCount).as("Total documents enumerated").isEqualTo(3);
  }

  private void validate_small_data_index_with_two_types(final boolean nIncludeVersion)
  {
    final Documents documents = getDocuments("small_data_index_with_two_types", nIncludeVersion);
    assertThat(documents.totalDocs()).as("Total documents").isEqualTo(5);
    assertThat(documents.deletedDocs()).as("Deleted documents").isEqualTo(0);
    assertThat(documents.maxDocId()).as("Max. document id").isEqualTo(5);
    
    final Set<EsDocument> expectedEsDocuments = new HashSet<>(getExpectedEventDocumentsForSmallData(nIncludeVersion));
    expectedEsDocuments.addAll(getExpectedHostDocumentsForSmallData(nIncludeVersion));
    
    int nCount = 0;
    for (EsDocument esDocument : documents)
    {
      ++nCount;
      LOGGER.debug("{}: {}", nCount, esDocument);
      
      assertThat(esDocument).as("Expected document").isIn(expectedEsDocuments);
    }
    
    assertThat(nCount).as("Total documents enumerated").isEqualTo(5);
  }
  
  private Documents getDocuments(final String indexFolder, final boolean bIncludeVersion)
  {
    return new EsIndexReader(getEsDataPath(indexFolder)).read(bIncludeVersion);
  }
  
  private String getEsDataPath(final String indexFolder)
  {
    return "src/test/resources/es_indices_raw_data/" + indexFolder;
  }

  private Set<EsDocument> getExpectedEventDocumentsForSmallData(final boolean bIncludeVersion)
  {
    final Set<EsDocument> documents = new HashSet<>();
    
    documents.add(createEsDocument("AV0w6AIktvLp91hLUt39", bIncludeVersion ? 1 : -1, "10.11.22.10", "file_01", 10, 1.0));
    documents.add(createEsDocument("AV0w6ak0tvLp91hLUt7Y", bIncludeVersion ? 2 : -1, "10.11.22.20", "file_changed_02", 20, 2.0));
    documents.add(createEsDocument("AV0w6fyItvLp91hLUt7_", bIncludeVersion ? 1 : -1, "10.11.22.30", "file_03", 30, 1.3));
    
    return documents;
  }

  private Set<EsDocument> getExpectedHostDocumentsForSmallData(final boolean bIncludeVersion)
  {
    final Set<EsDocument> documents = new HashSet<>();
    
    documents.add(createEsDocument("AV0xPRc2eAqt6v09rON8", bIncludeVersion ? 1 : -1, "host-10", "10.11.22.10", "myDomain"));
    documents.add(createEsDocument("AV0xPVp7eAqt6v09rOOj", bIncludeVersion ? 1 : -1, "host-20", "10.11.22.20", "myDomain"));
    
    return documents;
  }
  
  private EsDocument createEsDocument(final String id, 
                                      final long version, 
                                      final String hostName, 
                                      final String fileName,
                                      final int fileSize,
                                      final double fileVersion)
  {
    try
    {
      final Map<String, Object> file = new HashMap<>();
      file.put("name", fileName);
      file.put("size", fileSize);
      file.put("version", fileVersion);
      
      final Map<String, Object> event = new HashMap<>();
      event.put("host", hostName);
      event.put("file", file);
      
      final EsDocument document = new EsDocument(id, "event", XContentFactory.jsonBuilder().map(event).bytes());
      
      document.setVersion(version);
      
      return document;
    }
    catch (Exception e)
    {
      LOGGER.error("An error occurred.", e);
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception !!!
    }
  }

  private EsDocument createEsDocument(final String id, 
                                      final long version, 
                                      final String hostName, 
                                      final String ipAddressName,
                                      final String domain)
  {
    try
    {
      final Map<String, Object> host = new HashMap<>();
      host.put("hostname", hostName);
      host.put("ipaddress", ipAddressName);
      host.put("domain", domain);
      
      final EsDocument document = new EsDocument(id, "host", XContentFactory.jsonBuilder().map(host).bytes());
      
      document.setVersion(version);
      
      return document;
    }
    catch (Exception e)
    {
      LOGGER.error("An error occurred.", e);
      throw new RuntimeException(e); // TODO: Ajey - Throw custom exception !!!
    }
  }

  private final static ESLogger LOGGER = ESLoggerFactory.getLogger("EsIndexReaderTest");
}

