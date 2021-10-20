# Mini Search Engine


## Requirements

Use the package manager [pip3](https://pip.pypa.io/en/stable/) to install the requirements. Update apt if you are using EC2 Ubuntu instance.

```bash
sudo apt-get update
sudo apt install python3-pip -y
pip3 install tqdm Flask nltk
```

## Files and Tasks

1. `run_project.py` is the driver file, which will create the Flask app. 
2. `indexer.py` contains code to create and manipulate the index.
3. `preprocessor.py` contains code to pre-process documents & queries. 
3. `linkedlist.py` defines the basic data structures for the postings list and the nodes of the postings list. It also contains code to manipulate the postings list. 
4. Execute `run_project.py` to create your index and start your API endpoint. Endpoint will be available at `http://<ec2 public ipv4:9999>/execute_query`


## License
[MIT](https://choosealicense.com/licenses/mit/)
