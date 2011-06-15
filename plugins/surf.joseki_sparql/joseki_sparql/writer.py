# Copyright (c) 2009, Digital Enterprise Research Institute (DERI),
# NUI Galway
# All rights reserved.
# author: Cosmin Basca
# email: cosmin.basca@gmail.com
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer
#      in the documentation and/or other materials provided with
#      the distribution.
#    * Neither the name of DERI nor the
#      names of its contributors may be used to endorse or promote  
#      products derived from this software without specific prior
#      written permission.

# THIS SOFTWARE IS PROVIDED BY DERI ''AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL DERI BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.

# -*- coding: utf-8 -*-
__author__ = 'Cosmin Basca, Adam Gzella'

import sys
from sparql_protocol.writer import WriterPlugin as SPARQLWriterPlugin
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound, QueryBadFormed, SPARQLWrapperException
import urllib2

from reader import ReaderPlugin
from surf.plugin.writer import RDFWriter
from surf.query import Filter, Group, NamedGroup, Union
from surf.query.update import insert, delete, clear, load
from surf.rdf import BNode, Literal, URIRef

class SparqlWriterException(Exception): pass

class WriterPlugin(SPARQLWriterPlugin):
    # The overloaded methods of the basic SPARQL WriterPlugin
    def __init__(self, reader, *args, **kwargs):
        # Joseki likes its SPAR(Q|U)L endpoints in different places
        SPARQLWriterPlugin.__init__(self, reader, *args, **kwargs)
        
        self.__endpoint = kwargs.get("writer_endpoint", self.reader.endpoint)

        self.__sparql_wrapper = JosekiWrapper(self.__endpoint, self.__results_format)
        self.__sparql_wrapper.setMethod("POST")

    def __prepare_add_many_query(self, resources, context = None):
        # Joseki doesn't like a pattern-less INSERT without DATA
        query = insert(data=True)

        if context:
            query.into(context)

        for resource in resources:
            s = resource.subject
            for p, objs in resource.rdf_direct.items():
                for o in objs:
                    query.template((s, p, o))    

        return query
    
    def __prepare_delete_many_query(self, resources, context, inverse = False):
        # Joseki doesn't like OR
        query = delete()
        if context:
            query.from_(context)

        query.template(("?s", "?p", "?o"))

        if context:
            where_clause = NamedGroup(context)
        else:
            where_clause = Group()

        subjects = [resource.subject for resource in resources]
        filter = " || ".join(["?s = <%s>" % subject for subject in subjects])
        filter = Filter("(%s)" % filter)

        if inverse:
            filter2 = " || ".join(["?o = <%s>" % subject for subject in subjects])
            filter2 = Filter("(%s)" % filter2)

            where1 = Group([("?s", "?p", "?o"), filter])
            where2 = Group([("?s", "?p", "?o"), filter2])
            where_clause.append(Union([where1, where2]))
        else:
            where_clause.append(("?s", "?p", "?o"))
            where_clause.append(filter)

        query.where(where_clause)
        
        return query        
    
    def __prepare_selective_delete_query(self, resources, context = None):
        # Joseki doesn't like AND
        query = delete()
        if context:
            query.from_(context)

        query.template(("?s", "?p", "?o"))
        
        clauses = []
        for resource in resources:
            for p in resource.rdf_direct:
                filter = Filter("(?s = <%s> && ?p = <%s>)" % (resource.subject, p))
                clauses.append(Group([("?s", "?p", "?o"), filter]))
                 
        query.union(*clauses)
        return query        

    def __add_many(self, triples, context = None):
        # Joseki doesn't like a pattern-less INSERT without DATA
        self.log.debug("ADD several triples")

        query = insert(data=True)

        if context:
            query.into(context)

        for s, p, o in triples:
            query.template((s, p, o))

        try:
            query_str = unicode(query)
            self.log.debug(query_str)
            self.__sparql_wrapper.setQuery(query_str)
            self.__sparql_wrapper.query()#.convert()
            return True

        except EndPointNotFound, notfound:
            raise SparqlWriterException("Endpoint not found"), None, sys.exc_info()[2]
        except QueryBadFormed, badquery:
            raise SparqlWriterException("Bad query: %s" % query_str), None, sys.exc_info()[2]
        except Exception, e:
            raise SparqlWriterException("Exception: %s" % e), None, sys.exc_info()[2]

    def __build_filter(self, s, p, o):
        # Joseki doesn't like AND
        vars = [(s, '?s'), (p, '?p'), (o, '?o')]
        parts = []
        for var in vars:
            if var[0] != None:
                parts.append("%s = %s" % (var[1], self._term(var[0])))

        return " && ".join(parts)


class JosekiWrapper(SPARQLWrapper):
    def _query(self):
        """Internal method to execute the query. Returns the output of the
        C{urllib2.urlopen} method of the standard Python library
        
        This patch fixes the method to work with Joseki 3.4.4. 
        """
        request = self._createRequest()
        
        if request.data:
            request.data = request.data.replace('query=', 'request=')
        try:
            response = urllib2.urlopen(request)
            return response
        except urllib2.HTTPError, e:
            if e.code == 400:
                raise QueryBadFormed
            elif e.code == 404:
                raise EndPointNotFound
            else:
                raise e
            return None