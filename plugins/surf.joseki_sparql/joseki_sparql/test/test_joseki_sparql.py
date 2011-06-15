# -*- coding: UTF-8 -*-
""" Module for joseki_sparql plugin tests. """

from unittest import TestCase

from joseki_sparql.reader import SparqlReaderException
from joseki_sparql.writer import SparqlWriterException

import surf
from surf.query import select
from surf.rdf import Literal, URIRef
from surf.exc import CardinalityException
from surf.test.plugin import PluginTestMixin

class JosekiSparqlTestMixin(object):

    def _get_store_session(self, use_default_context = True):
        """ Return initialized SuRF store and session objects. """

        # FIXME: take endpoint from configuration file,
        # maybe we can mock SPARQL endpoint.
        kwargs = {"reader": "joseki_sparql",
                  "writer" : "joseki_sparql",
                  "endpoint" : "http://localhost:9980/sparql",
                  "writer_endpoint" : "http://localhost:9980/sparul",
                  "use_subqueries" : True,
                  "combine_queries" : True}

        if True: #use_default_context:
            kwargs["default_context"] = "http://surf_test_graph/dummy2"

        store = surf.Store(**kwargs)
        session = surf.Session(store)

        # Fresh start!
        store.clear("http://surf_test_graph/dummy2")
        store.clear(URIRef("http://my_context_1"))
        store.clear(URIRef("http://other_context_1"))
#        store.clear()

        return store, session

class StandardPluginTest(TestCase, JosekiSparqlTestMixin, PluginTestMixin):
    pass

class TestJosekiSparql(TestCase, JosekiSparqlTestMixin):
    """ Tests for joseki_sparql plugin. """

    def test_to_table(self):
        """ Test _to_table with empty bindings.  """

        data = {'results' : {'bindings' : [{'c' : {}}]}}

        # This should not raise exception.
        store = surf.store.Store(reader = "joseki_sparql")
        store.reader._to_table(data)

    def test_exceptions(self):
        """ Test that exceptions are raised on invalid queries. """

        store = surf.Store(reader = "joseki_sparql",
                           writer = "joseki_sparql",
                           endpoint = "invalid")

        def try_query():
            store.execute(query)

        query = select("?a")
        self.assertRaises(SparqlReaderException, try_query)

        def try_add_triple():
            store.add_triple("?s", "?p", "?o")

        self.assertRaises(SparqlWriterException, try_add_triple)
