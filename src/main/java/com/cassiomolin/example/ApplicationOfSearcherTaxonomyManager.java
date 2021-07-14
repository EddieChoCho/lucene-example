package com.cassiomolin.example;

import com.cassiomolin.example.lucene.LuceneIndexer;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.cassiomolin.example.lucene.DocumentFields.NAME_FACET_FIELD;

public class ApplicationOfSearcherTaxonomyManager {

    public static void main(String[] args) throws IOException {
        Weld weld = new Weld();
        WeldContainer container = weld.initialize();

        Directory index = new RAMDirectory();
        Directory taxonomyIndex = new RAMDirectory();
        FacetsConfig facetsConfig = new FacetsConfig();
        TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxonomyIndex);
        LuceneIndexer indexer = container.select(LuceneIndexer.class).get();

        indexer.index(index, taxonomyWriter, facetsConfig, 1);

        SearcherTaxonomyManager manager = new SearcherTaxonomyManager(index, taxonomyIndex, new SearcherFactory());

        //Search first time
        SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy = manager.acquire();
        IndexSearcher indexSearcher = null;
        TaxonomyReader taxonomyReader = null;
        try {
            indexSearcher = searcherAndTaxonomy.searcher;
            assert indexSearcher.getIndexReader().getRefCount() == 2;

            taxonomyReader = searcherAndTaxonomy.taxonomyReader;
            assert taxonomyReader.getRefCount() == 2;

            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 1;

            FacetsCollector facetsCollector = new FacetsCollector();
            indexSearcher.search(new MatchAllDocsQuery(), facetsCollector);

            Facets facets = new FastTaxonomyFacetCounts(taxonomyReader, facetsConfig, facetsCollector);
            List<FacetResult> facetResults = facets.getAllDims(10);
            assert facetResults.size() == 1;
            assert facetResults.get(0).value.intValue() == 1;
            assert 1 == facetResults.stream()
                    .filter(result -> NAME_FACET_FIELD.equals(result.dim))
                    .map(result -> result.labelValues)
                    .flatMap(Stream::of)
                    .filter(labelValue -> "John Doe".equals(labelValue.label))
                    .map(labelAndValue -> labelAndValue.value)
                    .map(Number::intValue)
                    .reduce(0, Integer::sum);

        } finally {
            manager.release(searcherAndTaxonomy);
            assert indexSearcher.getIndexReader().getRefCount() == 1;
            assert taxonomyReader.getRefCount() == 1;
        }

        //Update index
        taxonomyWriter = new DirectoryTaxonomyWriter(taxonomyIndex);
        indexer.index(index, taxonomyWriter, facetsConfig, 3);
        manager.maybeRefreshBlocking();
        assert indexSearcher.getIndexReader().getRefCount() == 0;
        assert taxonomyReader.getRefCount() == 0;

        //Search second time
        searcherAndTaxonomy = manager.acquire();
        IndexSearcher secondIndexSearcher = null;
        TaxonomyReader secondTaxonomyReader = null;
        try {
            secondIndexSearcher = searcherAndTaxonomy.searcher;
            assert secondIndexSearcher.getIndexReader().getRefCount() == 2;

            secondTaxonomyReader = searcherAndTaxonomy.taxonomyReader;
            assert secondTaxonomyReader.getRefCount() == 2;

            TopDocs topDocs = secondIndexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 2;

            FacetsCollector facetsCollector = new FacetsCollector();
            secondIndexSearcher.search(new MatchAllDocsQuery(), facetsCollector);

            Facets facets = new FastTaxonomyFacetCounts(secondTaxonomyReader, facetsConfig, facetsCollector);
            List<FacetResult> facetResults = facets.getAllDims(10);
            assert facetResults.size() == 1;
            assert facetResults.get(0).value.intValue() == 2;
            assert 1 == facetResults.stream()
                    .filter(result -> NAME_FACET_FIELD.equals(result.dim))
                    .map(result -> result.labelValues)
                    .flatMap(Stream::of)
                    .filter(labelValue -> "Sam Smith".equals(labelValue.label))
                    .map(labelAndValue -> labelAndValue.value)
                    .map(Number::intValue)
                    .reduce(0, Integer::sum);

        } finally {
            manager.release(searcherAndTaxonomy);
            assert secondIndexSearcher.getIndexReader().getRefCount() == 1;
            assert secondTaxonomyReader.getRefCount() == 1;
        }

        //Update index again
        taxonomyWriter = new DirectoryTaxonomyWriter(taxonomyIndex);
        indexer.index(index, taxonomyWriter, facetsConfig, 4);
        manager.maybeRefreshBlocking();
        assert secondIndexSearcher.getIndexReader().getRefCount() == 0;
        assert secondTaxonomyReader.getRefCount() == 1;

        //Search third time
        searcherAndTaxonomy = manager.acquire();
        IndexSearcher thirdIndexSearcher = null;
        TaxonomyReader thirdTaxonomyReader = null;
        try {
            thirdIndexSearcher = searcherAndTaxonomy.searcher;
            assert thirdIndexSearcher.getIndexReader().getRefCount() == 2;

            thirdTaxonomyReader = searcherAndTaxonomy.taxonomyReader;
            assert thirdTaxonomyReader.getRefCount() == 2;
            assert thirdTaxonomyReader == secondTaxonomyReader;

            TopDocs topDocs = thirdIndexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assert topDocs.totalHits == 3;

            FacetsCollector facetsCollector = new FacetsCollector();
            thirdIndexSearcher.search(new MatchAllDocsQuery(), facetsCollector);

            Facets facets = new FastTaxonomyFacetCounts(thirdTaxonomyReader, facetsConfig, facetsCollector);
            List<FacetResult> facetResults = facets.getAllDims(10);
            assert facetResults.size() == 1;
            assert facetResults.get(0).value.intValue() == 3;
            assert 2 == facetResults.stream()
                    .filter(result -> NAME_FACET_FIELD.equals(result.dim))
                    .map(result -> result.labelValues)
                    .flatMap(Stream::of)
                    .filter(labelValue -> "John Doe".equals(labelValue.label))
                    .map(labelAndValue -> labelAndValue.value)
                    .map(Number::intValue)
                    .reduce(0, Integer::sum);

        } finally {
            manager.release(searcherAndTaxonomy);
            assert thirdIndexSearcher.getIndexReader().getRefCount() == 1;
            assert thirdTaxonomyReader.getRefCount() == 1;
        }
    }
}
