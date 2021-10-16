'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import math


class Node:

    def __init__(self, value=None, next=None,skip_next=None,term_frequency=0,tf_idf=0.0):
        """ Class to define the structure of each node in a linked list (postings list).
            Value: document id, Next: Pointer to the next node
            Add more parameters if needed.
            Hint: You may want to define skip pointers & appropriate score calculation here"""
        self.value = value
        self.next = next
        self.skip_next = skip_next
        self.term_frequency=term_frequency
        self.tf_idf=tf_idf

class LinkedList:
    """ Class to define a linked list (postings list). Each element in the linked list is of the type 'Node'
        Each term in the inverted index has an associated linked list object.
        Feel free to add additional functions to this class."""
    def __init__(self):
        self.start_node = None
        self.end_node = None
        self.length, self.n_skips, self.idf = 0, 0, 0.0
        self.skip_length = None

    def traverse_list(self):
        traversal = []
        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
                # Start traversal from head, and go on till you reach None
            while n is not None:
                traversal.append(n.value)
                n = n.next
        return traversal

    def traverse_list_sort(self):
        traversal = []
        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
                # Start traversal from head, and go on till you reach None
            while n is not None:
                traversal.append((n.tf_idf,n.value))
                n = n.next
        sorted_traveral_list=sorted(traversal, key=lambda t: (t[0], -t[1]), reverse=True)
        output_array=[]
        for i in range(len(sorted_traveral_list)):
            output_array.append(sorted_traveral_list[i][1])
        return output_array

    def traverse_skips(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list using skip pointers.
                To be implemented."""
            n = self.start_node
            while n is not None:
                traversal.append(n.value)
                n=n.skip_next

#             while n is not None:
#                 traversal.append(n.value)
#                 n=n.skip_next

            return traversal

    def add_skip_connections(self):
        """ Write logic to add skip pointers to the linked list.
                This function does not return anything.
                To be implemented."""
        n_skips = math.floor(math.sqrt(self.length))
        if n_skips * n_skips == self.length:
            n_skips = n_skips - 1

        self.skip_length=round(math.sqrt(self.length))

        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
            # Start traversal from head, and go on till you reach None
            i=0
            x=0
            temp = n
            while n is not None and i <n_skips:
                if (x==self.skip_length):
                    temp.skip_next=n
                    temp = n
                    x=0
                    i=i+1
                n = n.next
                x+=1

    def insert_at_end(self, value):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        new_node = Node(value=value)
        n = self.start_node
        new_node.term_frequency+=1

        if self.start_node is None:
            self.start_node = new_node
            self.end_node = new_node
            return

        elif self.start_node.value >= value:
            self.start_node = new_node
            self.start_node.next = n
            return

        elif self.end_node.value <= value:
            self.end_node.next = new_node
            self.end_node = new_node
            return

        else:
            while n.value < value < self.end_node.value and n.next is not None:
                n = n.next

            m = self.start_node
            while m.next != n and m.next is not None:
                m = m.next

            m.next = new_node
            new_node.next = n
            return

    def insert_at_end_tf_idf(self, value,tf_idf_ip):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        new_node = Node(value=value,tf_idf=tf_idf_ip)
        n = self.start_node
        new_node.term_frequency+=1

        if self.start_node is None:
            self.start_node = new_node
            self.end_node = new_node
            return

        elif self.start_node.value >= value:
            self.start_node = new_node
            self.start_node.next = n
            return

        elif self.end_node.value <= value:
            self.end_node.next = new_node
            self.end_node = new_node
            return

        else:
            while n.value < value < self.end_node.value and n.next is not None:
                n = n.next

            m = self.start_node
            while m.next != n and m.next is not None:
                m = m.next

            m.next = new_node
            new_node.next = n
            return

