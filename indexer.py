'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from linkedlist import LinkedList
from collections import OrderedDict


class Indexer:
    def __init__(self):
        """ Add more attributes if needed"""
        self.inverted_index = OrderedDict({})
        self.i=0
        self.unique_doc_ids=set()

    def get_index(self):
        """ Function to get the index.
            Already implemented."""
        return self.inverted_index

    def generate_inverted_index(self, doc_id, tokenized_document):
        """ This function adds each tokenized document to the index. This in turn uses the function add_to_index
            Already implemented."""
        self.unique_doc_ids.add(doc_id)
        for t in tokenized_document:
            self.add_to_index(t, doc_id)

    def add_to_index(self, term_, doc_id_):
        """ This function adds each term & document id to the index.
            If a term is not present in the index, then add the term to the index & initialize a new postings list (linked list).
            If a term is present, then add the document to the appropriate position in the posstings list of the term.
            To be implemented."""

        if(term_ not in self.inverted_index.keys()):
            postingsList=LinkedList()
            postingsList.insert_at_end(doc_id_)
            #Doc freq
            postingsList.length=postingsList.length+1
            self.inverted_index[term_]=postingsList
#             self.inverted_index[term_].start_node.term_frequency += 1
        elif(not self.is_doc_id_in_posting_list(self.inverted_index[term_],doc_id_,term_)):
            self.inverted_index[term_].insert_at_end(doc_id_)
            self.inverted_index[term_].length=self.inverted_index[term_].length+1

#         self.i=self.i+1
#         if(self.i==1):
#             print("IM HERE::::::::::::::",term_)
#             self.inverted_index[term_].traverse_list()

    def is_doc_id_in_posting_list(self,postingsList,doc_id,term):
        doc_id_present=False
        current=postingsList.start_node
        while current is not None:
            if(doc_id==current.value):
                doc_id_present=True
                current.term_frequency += 1
                break
            current=current.next
        return doc_id_present

    def sort_terms(self):
        """ Sorting the index by terms.
            Already implemented."""
        sorted_index = OrderedDict({})
        for k in sorted(self.inverted_index.keys()):
            sorted_index[k] = self.inverted_index[k]
        self.inverted_index = sorted_index

    def add_skip_connections(self):
        """ For each postings list in the index, add skip pointers.
            To be implemented."""
        for term_ in self.inverted_index.keys():
            postingsList=self.inverted_index[term_]
            postingsList.add_skip_connections()

#             postingsList_length=postingsList.length
#
#             number_of_skips=floor(sqrt(postingsList_length))
#             length_of_skips=round(sqrt(postingsList_length))
#
#             postingsList.n_skips=number_of_skips
#             postingsList.skip_length=length_of_skips


    def calculate_tf_idf(self,doc_token_number,document_count):
        """ Calculate tf-idf score for each document in the postings lists of the index.
            To be implemented."""
        for term_ in self.inverted_index.keys():
            postingsList=self.inverted_index[term_]
            len_of_posting_list=postingsList.length
            idf=document_count/len_of_posting_list
            if postingsList.start_node is None:
                print("List has no element")
                return
            else:
                n = postingsList.start_node
                 # Start traversal from head, and go on till you reach None
                while n is not None:
                    freq=n.term_frequency
                    tf=freq/doc_token_number[n.value]
                    tf_idf_value=tf*idf
                    n.tf_idf=tf_idf_value
                    n = n.next


