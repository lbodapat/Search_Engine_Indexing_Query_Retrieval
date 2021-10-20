
import collections
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')


class Preprocessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.ps = PorterStemmer()

    def get_doc_id(self, doc):
        """ Splits each line of the document, into doc_id & text.
            Already implemented"""
        arr = doc.split("\t")
        return int(arr[0]), arr[1]

    def tokenizer(self, text):
        """ Implement logic to pre-process & tokenize document text.
            Write the code in such a way that it can be re-used for processing the user's query.
            To be implemented."""
        #Clean Text
        clean_text=self.clean_text(text)
        #Tokenize
        stopword_tokens=self.tokenize_text(clean_text)
        #Remove Stop Words Tokens to get final Tokens
        tokens=self.remove_stop_words(stopword_tokens)
        #Stem to convert tokens to terms
        terms=self.perform_stemming(tokens)
        print(terms)
        return terms

    def clean_text(self,text):
#         print("Step1: Remove Special Characters, Convert text to Lower Case and remove white spaces")
        return re.sub(r'[^a-z\s0-9-]', ' ', text.strip().lower()).strip()

    def tokenize_text(self,clean_text):
#         print("Step2: Tokenizing the text")
        return re.split(r'[\s-]+', clean_text)

    def remove_stop_words(self,stopword_tokens):
        tokens= [stk for stk in stopword_tokens if stk not in self.stop_words]
        return tokens

    def perform_stemming(self,tokens):
        terms=[]
        for token in tokens:
            term=self.ps.stem(token)
            terms.append(term)
        return terms