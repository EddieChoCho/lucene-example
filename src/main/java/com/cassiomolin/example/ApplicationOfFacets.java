package com.cassiomolin.example;

import com.cassiomolin.example.lucene.DocumentFields;
import com.cassiomolin.example.lucene.LuceneIndexer;
import com.cassiomolin.example.lucene.LuceneSearcher;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ApplicationOfFacets {

    public static void main(String[] args) throws IOException {
        Weld weld = new Weld();
        WeldContainer container = weld.initialize();

        Directory index = new RAMDirectory();
        Directory taxonomyIndex = new RAMDirectory();
        FacetsConfig facetsConfig = new FacetsConfig();
        TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxonomyIndex);
        LuceneIndexer indexer = container.select(LuceneIndexer.class).get();

        indexer.index(index, taxonomyWriter, facetsConfig);

        IndexReader indexReader = DirectoryReader.open(index);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxonomyIndex);

        Query query = new TermQuery(new Term(DocumentFields.NAME_FIELD, "John Doe"));

        FacetsCollector facetsCollector = new FacetsCollector();
        indexSearcher.search(query, facetsCollector);
        Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);

        List<FacetResult> facetResults = facets.getAllDims(1);
        Optional<Integer> numberOfDoc = facetResults.stream()
                .map(result -> result.value)
                .map(number -> (int) number)
                .reduce(Integer::sum);

        assert numberOfDoc.isPresent();
        assert numberOfDoc.get() == 2;

        container.shutdown();
    }
}
