'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedlist import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time
import random
import flask
from flask import Flask
from flask import request
import hashlib
import heapq

app = Flask(__name__)


class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()

############################ NORMAL MERGE##############################
    def _merge(self,postings_list1,postings_list2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        merged_postings_list=LinkedList()
        node1=postings_list1.start_node
        node2=postings_list2.start_node
        num_docs=0
        num_comparisons=0
        dictionary=dict()
        merge_node=merged_postings_list.start_node
        while(node1 is not None and node2 is not None):
            num_comparisons=num_comparisons+1
            if(node1.value==node2.value):
                num_docs=num_docs+1
                merged_postings_list.insert_at_end(node1.value)
                node1=node1.next
                node2=node2.next
            elif(node1.value<node2.value):
                node1=node1.next
            else:
                node2=node2.next
        dictionary['linkedlist']=merged_postings_list
        dictionary['num_docs']=num_docs
        dictionary['num_comparisons']=num_comparisons

        return dictionary

    def merge_test(self,input_term_arr):
        term_sorted_list=[]
        inverted_index= self.indexer.get_index()
        heapq.heapify(term_sorted_list)
        for term in input_term_arr:
            heapq.heappush(term_sorted_list,(len(inverted_index[term].traverse_list()),term))
        merge_liked_list=inverted_index[term_sorted_list[0][1]]
        for i in range(len(term_sorted_list)):
            output=self._merge(merge_liked_list,inverted_index[term_sorted_list[i][1]])
        return output

############################ TF-IDF MERGE##############################
    def _merge_tf_idf(self,postings_list1,postings_list2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        merged_postings_list=LinkedList()
        node1=postings_list1.start_node
        node2=postings_list2.start_node
        num_docs=0
        num_comparisons=0
        dictionary=dict()
        merge_node=merged_postings_list.start_node
        while(node1 is not None and node2 is not None):
            num_comparisons=num_comparisons+1
            if(node1.value==node2.value):
                num_docs=num_docs+1
                merged_postings_list.insert_at_end_tf_idf(node1.value,max(node1.tf_idf,node2.tf_idf))
                node1=node1.next
                node2=node2.next
            elif(node1.value<node2.value):
                node1=node1.next
            else:
                node2=node2.next
        dictionary['linkedlist']=merged_postings_list
        dictionary['num_docs']=num_docs
        dictionary['num_comparisons']=num_comparisons
        return dictionary

    def merge_test_tf_idf(self,input_term_arr):
        term_sorted_list=[]
        inverted_index= self.indexer.get_index()
        heapq.heapify(term_sorted_list)
        for term in input_term_arr:
            heapq.heappush(term_sorted_list,(len(inverted_index[term].traverse_list()),term))
        merge_liked_list=inverted_index[term_sorted_list[0][1]]
        for i in range(len(term_sorted_list)):
            output=self._merge_tf_idf(merge_liked_list,inverted_index[term_sorted_list[i][1]])
        return output

############################ NORMAL SKIP MERGE##############################
    def _merge_skip(self,postings_list1,postings_list2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        merged_postings_list=LinkedList()
        node1=postings_list1.start_node
        node2=postings_list2.start_node
        num_docs=0
        num_comparisons=0
        dictionary=dict()
        merge_node=merged_postings_list.start_node
        while(node1 is not None and node2 is not None):
            num_comparisons=num_comparisons+1
            if(node1.value==node2.value):
                num_docs=num_docs+1
                merged_postings_list.insert_at_end(node1.value)
                node1=node1.next
                node2=node2.next
            elif(node1.value<node2.value):
                if(node1.skip_next is not None and (node1.skip_next.value<=node2.value)):
                    while(node1.skip_next is not None and (node1.skip_next.value<=node2.value)):
                        node1=node1.skip_next
                else:
                    node1=node1.next
            else:
                if(node2.skip_next is not None and (node2.skip_next.value<=node1.value)):
                    while(node2.skip_next is not None and (node2.skip_next.value<=node1.value)):
                        node2=node2.skip_next
                else:
                    node2=node2.next

        dictionary['linkedlist']=merged_postings_list
        dictionary['num_docs']=num_docs
        dictionary['num_comparisons']=num_comparisons
        return dictionary

    def merge_test_skip(self,input_term_arr):
        term_sorted_list=[]
        inverted_index= self.indexer.get_index()
        heapq.heapify(term_sorted_list)
        for term in input_term_arr:
            heapq.heappush(term_sorted_list,(len(inverted_index[term].traverse_list()),term))
        merge_liked_list=inverted_index[term_sorted_list[0][1]]
        for i in range(len(term_sorted_list)):
            output=self._merge_skip(merge_liked_list,inverted_index[term_sorted_list[i][1]])
        return output

############################ SKIP MERGE - TF-IDF##############################
    def _merge_skip_tf_idf(self,postings_list1,postings_list2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        merged_postings_list=LinkedList()
        node1=postings_list1.start_node
        node2=postings_list2.start_node
        num_docs=0
        num_comparisons=0
        dictionary=dict()
        merge_node=merged_postings_list.start_node
        while(node1 is not None and node2 is not None):
            num_comparisons=num_comparisons+1
            if(node1.value==node2.value):
                num_docs=num_docs+1
                merged_postings_list.insert_at_end_tf_idf(node1.value,max(node1.tf_idf,node2.tf_idf))
                node1=node1.next
                node2=node2.next
            elif(node1.value<node2.value):
                if(node1.skip_next is not None and (node1.skip_next.value<=node2.value)):
                    while(node1.skip_next is not None and (node1.skip_next.value<=node2.value)):
                        node1=node1.skip_next
                else:
                    node1=node1.next
            else:
                if(node2.skip_next is not None and (node2.skip_next.value<=node1.value)):
                    while(node2.skip_next is not None and (node2.skip_next.value<=node1.value)):
                        node2=node2.skip_next
                else:
                    node2=node2.next

        dictionary['linkedlist']=merged_postings_list
        dictionary['num_docs']=num_docs
        dictionary['num_comparisons']=num_comparisons
        return dictionary

    def merge_test_skip_tf_idf(self,input_term_arr):
        term_sorted_list=[]
        inverted_index= self.indexer.get_index()
        heapq.heapify(term_sorted_list)
        for term in input_term_arr:
            heapq.heappush(term_sorted_list,(len(inverted_index[term].traverse_list()),term))
        merge_liked_list=inverted_index[term_sorted_list[0][1]]
        for i in range(len(term_sorted_list)):
            output=self._merge_skip_tf_idf(merge_liked_list,inverted_index[term_sorted_list[i][1]])
        return output


    def _daat_and(self,input_term_arr,k):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        list_of_term_postings_list=[]
        query_terms_list=input_term_arr
        lenTerms = len(query_terms_list)
        inverted_index= self.indexer.get_index()
        unique_doc_ids=self.indexer.unique_doc_ids

        for term in query_terms_list:
            list_of_term_postings_list.append(inverted_index[term])

        num_docs=0
        num_comparisons=0
        heap_list=[]
        heapq.heapify(heap_list)
        test=[]

        output=dict()
        for document_id in unique_doc_ids:
            score=0
            lterm = 0
            for postings_list in list_of_term_postings_list:
                n=postings_list.start_node
                while(n is not None):
                    if(document_id==n.value):
                        num_comparisons=num_comparisons+1
                        score=score+n.tf_idf
                        lterm += 1
                        break
                    n=n.next
            if score != 0 and lterm == lenTerms: heapq.heappush(heap_list,(score,document_id))

        for item in heapq.nlargest(100, heap_list):
            print(item)

        print("Heap pop",heapq.heappop(heap_list))

        output['heapq']=heapq.nlargest(100, heap_list)
        output['num_comparisons']=num_comparisons
        return output


    def _daat_skip_and(self,input_term_arr,k):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        list_of_term_postings_list=[]
        query_terms_list=input_term_arr
        lenTerms = len(query_terms_list)
        inverted_index= self.indexer.get_index()
        unique_doc_ids=self.indexer.unique_doc_ids

        for term in query_terms_list:
            list_of_term_postings_list.append(inverted_index[term])

        num_docs=0
        num_comparisons=0
        heap_list=[]
        heapq.heapify(heap_list)
        output=dict()
        for document_id in unique_doc_ids:
            score=0
            lterm = 0
            for postings_list in list_of_term_postings_list:
                n=postings_list.start_node
                while(n is not None):
                    if(document_id==n.value):
                        score=score+n.tf_idf
                        lterm += 1
                        break
#                     n=n.skip_next
                    elif(n.skip_next is not None):
                        while(n.skip_next is not None):
                            n=n.skip_next
                    else:
                        n=n.next
            if score != 0 and lterm == lenTerms: heapq.heappush(heap_list,(score,document_id))

        output['heapq']=heapq.nlargest(100, heap_list)
        output['num_comparisons']=num_comparisons
        return output

    def _get_postings(self):
        """ Function to get the postings list of a term from the index.
            Use appropriate parameters & return types.
            To be implemented."""
        raise NotImplementedError

    def _output_formatter(self, op):
        """ This formats the result in the required format.
            Do NOT change."""
        if op is None or len(op) == 0:
            return [], 0
        op_no_score = [int(i) for i in op]
        results_cnt = len(op_no_score)
        return op_no_score, results_cnt

#TODO - complete Index building
    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        doc_token_number=dict()
        document_count=0
        with open(corpus, 'r') as fp:
            for line in tqdm(fp.readlines()):
                document_count=document_count+1
                doc_id, document = self.preprocessor.get_doc_id(line)
                tokenized_document = self.preprocessor.tokenizer(document)
                doc_token_number[doc_id]=len(tokenized_document)
                self.indexer.generate_inverted_index(doc_id, tokenized_document)
        self.indexer.sort_terms()
        self.indexer.add_skip_connections()
        self.indexer.calculate_tf_idf(doc_token_number,document_count)

    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}

#TODO- Complete Query Processing
    def run_queries(self, query_list, random_command):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': self.sanity_checker(random_command)}

        for query in tqdm(query_list):
            """ Run each query against the index. You should do the following for each query:
                1. Pre-process & tokenize the query.
                2. For each query token, get the postings list & postings list with skip pointers.
                3. Get the DAAT AND query results & number of comparisons with & without skip pointers.
                4. Get the DAAT AND query results & number of comparisons with & without skip pointers,
                    along with sorting by tf-idf scores."""

             # Tokenized query. To be implemented.
            input_term_arr=self.preprocessor.tokenizer(query)
            inverted_index= self.indexer.get_index()

            for term in input_term_arr:
                postings, skip_postings = None, None

                """ Implement logic to populate initialize the above variables.
                    The below code formats your result to the required format.
                    To be implemented."""

                postings=inverted_index[term].traverse_list()
                skip_postings=inverted_index[term].traverse_skips()

                output_dict['postingsList'][term] = postings
                output_dict['postingsListSkip'][term] = skip_postings

            and_op_no_skip, and_op_skip, and_op_no_skip_sorted, and_op_skip_sorted = None, None, None, None
            and_comparisons_no_skip, and_comparisons_skip, \
                and_comparisons_no_skip_sorted, and_comparisons_skip_sorted = None, None, None, None
            """ Implement logic to populate initialize the above variables.
                The below code formats your result to the required format.
                To be implemented."""


            #DAAT
#             output_daat=self._daat_and(input_term_arr,20)
#             heapq_nlargest=output_daat['heapq']
#             num_comparisons_daat=output_daat['num_comparisons']
#             some_array=[]
#             for i in range(len(heapq_nlargest)):
#                 some_array.append(heapq_nlargest[i][1])
#             and_op_no_skip_sorted=some_array
#             and_comparisons_no_skip_sorted=num_comparisons_daat

            #DAAT Skip
            output_daat_skip=self._daat_skip_and(input_term_arr,20)
            heapq_nlargest_skip=output_daat_skip['heapq']
            num_comparisons_daat_skip=output_daat_skip['num_comparisons']
            some_array_skip=[]
            for i in range(len(heapq_nlargest_skip)):
                some_array_skip.append(heapq_nlargest_skip[i][1])
            and_op_skip_sorted=some_array_skip
            and_comparisons_skip_sorted=num_comparisons_daat_skip

            #MERGE
            merge_output=self.merge_test(input_term_arr)
            and_op_no_skip=merge_output['linkedlist'].traverse_list()
            and_comparisons_no_skip=merge_output['num_comparisons']
            #MERGE Skips
            merge_output_skip=self.merge_test_skip(input_term_arr)
            and_op_skip=merge_output_skip['linkedlist'].traverse_list()
            and_comparisons_skip=merge_output_skip['num_comparisons']
            #MERGE TFIDF Sorted
            merge_output_tf_idf=self.merge_test_tf_idf(input_term_arr)
            and_op_no_skip_sorted=merge_output_tf_idf['linkedlist'].traverse_list_sort()
            and_comparisons_no_skip_sorted=merge_output_tf_idf['num_comparisons']
            #MERGE TFIDF Sorted Skips
            merge_output_skip_tf_idf=self.merge_test_skip_tf_idf(input_term_arr)
            and_op_skip_sorted=merge_output_skip_tf_idf['linkedlist'].traverse_list_sort()
            and_comparisons_skip_sorted=merge_output_tf_idf['num_comparisons']

            and_op_no_score_no_skip, and_results_cnt_no_skip = self._output_formatter(and_op_no_skip)
            and_op_no_score_skip, and_results_cnt_skip = self._output_formatter(and_op_skip)
            and_op_no_score_no_skip_sorted, and_results_cnt_no_skip_sorted = self._output_formatter(and_op_no_skip_sorted)
            and_op_no_score_skip_sorted, and_results_cnt_skip_sorted = self._output_formatter(and_op_skip_sorted)

            output_dict['daatAnd'][query.strip()] = {}
            output_dict['daatAnd'][query.strip()]['results'] = and_op_no_score_no_skip
            output_dict['daatAnd'][query.strip()]['num_docs'] = and_results_cnt_no_skip
            output_dict['daatAnd'][query.strip()]['num_comparisons'] = and_comparisons_no_skip

            output_dict['daatAndSkip'][query.strip()] = {}
            output_dict['daatAndSkip'][query.strip()]['results'] = and_op_no_score_skip
            output_dict['daatAndSkip'][query.strip()]['num_docs'] = and_results_cnt_skip
            output_dict['daatAndSkip'][query.strip()]['num_comparisons'] = and_comparisons_skip

            output_dict['daatAndTfIdf'][query.strip()] = {}
            output_dict['daatAndTfIdf'][query.strip()]['results'] = and_op_no_score_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_docs'] = and_results_cnt_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_no_skip_sorted

            output_dict['daatAndSkipTfIdf'][query.strip()] = {}
            output_dict['daatAndSkipTfIdf'][query.strip()]['results'] = and_op_no_score_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_docs'] = and_results_cnt_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_skip_sorted

        return output_dict


@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)


if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    #TODO - Changes
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()

    corpus = argv.corpus
    output_location = argv.output_location
    username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)

    app.run(host="0.0.0.0", port=9999)
