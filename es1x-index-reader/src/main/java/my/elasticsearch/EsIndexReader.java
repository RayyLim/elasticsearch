/********************************************************************
 * File Name:    EsIndexReader.java
 *
 * Date Created: Jun 28, 2017
 *
 * ------------------------------------------------------------------
 * 
 * Copyright @ 2017 ajeydudhe@gmail.com
 *
 *******************************************************************/

package my.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
  
/**
 * Wrapper to enumerate the documents in the given index.
 * If the documents are to be enumerated for, say, migrating to ES5x instance then better avoid such wrapper
 * and directly loop through the documents using the {@link IndexReader} for better performance.
 * @author ajey_dudhe
 *
 */
public class EsIndexReader
{
  public EsIndexReader(final String dataDirectoryPath)
  {
    this.dataDirectoryPath = dataDirectoryPath;
  }

  /**
   * Read the documents from the given index.
   * @param bIncludeVersion True if document version needs to be retrieved. If False then version is set to -1. Retrieving document version is an additional lookup so should be avoided if not required.
   * @return The {@link Documents} instance having details like total documents, deleted documents etc. and is also an {@link Iterable}&lt;EsDocument>.
   */
  public Documents read(final boolean bIncludeVersion)
  {    
    return new Documents(bIncludeVersion);
  }
  
  public class Documents implements Iterable<EsDocument>
  {
    private Documents(final boolean bIncludeVersion)
    {
      try
      {
        this.bIncludeVersion = bIncludeVersion;

        LOGGER.info("Reading index files under [{}]", dataDirectoryPath);
        
        this.indexReader = DirectoryReader.open(FSDirectory.open(new File(dataDirectoryPath)));
      }
      catch (Exception e)
      {
        LOGGER.error("An error occurred while reading index.", e);
        throw new RuntimeException(e); // TODO: Ajey - Throw custom exception !!!
      }          
    }
    
    public int totalDocs()
    {
      return this.indexReader.numDocs();
    }
    
    public int maxDocId()
    {
      return this.indexReader.maxDoc();
    }
    
    public int deletedDocs()
    {
      return this.indexReader.numDeletedDocs();
    }
    
    @Override
    public Iterator<EsDocument> iterator()
    {
      return new Iterator<EsDocument>()
      {        
        @Override
        public EsDocument next()
        {
          if(!this.hasNext())
             throw new NoSuchElementException();
          
          try
          {
            final UidAndSourceFieldsVisitor fieldsVisitor = new UidAndSourceFieldsVisitor();
            indexReader.document(this.nDocIndex, fieldsVisitor);
            
            final EsDocument document = new EsDocument(fieldsVisitor.uid().id(), fieldsVisitor.uid().type(), fieldsVisitor.source());
            
            if(bIncludeVersion)
            {
              document.setVersion(Versions.loadVersion(indexReader, new Term(UidFieldMapper.NAME, fieldsVisitor.uid().toBytesRef())));
            }
            
            ++this.nDocIndex;
            
            return document;
          }
          catch (Exception e)
          {
            throw new RuntimeException(e); // TODO: Ajey - Throw custom exception
          }
        }
        
        @Override
        public boolean hasNext()
        {
          try
          {
            if(this.liveDocs == null)
               return this.nDocIndex < indexReader.maxDoc();
            
            for(; this.nDocIndex < indexReader.maxDoc() && !this.liveDocs.get(this.nDocIndex); ++this.nDocIndex)
            {
              //LOGGER.info("Ignoring deleted document: {}", this.nDocIndex);
            }
            
            return this.nDocIndex < indexReader.maxDoc();
          }
          finally
          {
            if (this.nDocIndex >= indexReader.maxDoc())
            {
              try
              {
                indexReader.close();
              }
              catch (IOException e)
              {
                LOGGER.error("An error occurred while closing document reader.", e); // TODO: Ajey - Ignore the exception ???
              }
            }
          }
        }
        
        private final Bits liveDocs = MultiFields.getLiveDocs(indexReader);
        private int nDocIndex = 0;
      };
    }

    // Private members
    private IndexReader indexReader;
    private boolean bIncludeVersion = false;
  }

  // Private members
  private final String dataDirectoryPath;
  private final static ESLogger LOGGER = ESLoggerFactory.getLogger("EsIndexReader");
}

