package com.cassiomolin.example;

import com.cassiomolin.example.lucene.LuceneIndexer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import java.io.IOException;

import static org.apache.lucene.util.IOUtils.closeWhileHandlingException;

public class ApplicationOfSearcherManager {

    public static void main(String[] args) throws IOException {
        Weld weld = new Weld();
        WeldContainer container = weld.initialize();

        Directory index = new RAMDirectory();
        LuceneIndexer indexer = container.select(LuceneIndexer.class).get();

        indexer.index(index, 1);

        SearcherManager manager = new SearcherManager(index, null);

        IndexSearcher indexSearcher = null;
        IndexSearcher acquiredIndexSearcherWithoutRefresh = null;
        IndexSearcher acquiredIndexSearcherAfterRefresh = null;

        try {
            indexSearcher = manager.acquire();
            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 1;

            //Insert a new document to the directory
            indexer.index(index, 2);

            //Since the indexSearcher has not be update, the new inserted data would not be searched
            topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 1;

            acquiredIndexSearcherWithoutRefresh = manager.acquire();
            topDocs = acquiredIndexSearcherWithoutRefresh.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 1;
            assert indexSearcher == acquiredIndexSearcherWithoutRefresh;

            //After refresh current searcher of manager, the newly inserted data could be searched.
            manager.maybeRefreshBlocking();
            acquiredIndexSearcherAfterRefresh = manager.acquire();
            topDocs = acquiredIndexSearcherAfterRefresh.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 2;

            //The old indexSearcher still can not get the newly inserted data.
            topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 1;

        } finally {
            manager.release(indexSearcher);
            manager.release(acquiredIndexSearcherWithoutRefresh);
            manager.release(acquiredIndexSearcherAfterRefresh);
        }

        closeWhileHandlingException(manager);
        container.shutdown();
    }
}
