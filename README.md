# metadatum_sgs

This is a demo application of metadatum self generative systems platform (SGS). SGS is a platform for building self generative systems. It is  a framework for building applications that can change their own structure and behavior at runtime. 
You can find introduction into John von Neumann's self-reproducing automata in the following article: 
https://medium.com/@alexmy_29874/introduction-to-metadata-graphs-and-self-generative-systems-sgs-574ebd45be91

This is still work in progress, so, the documentation is not ready yet and available as a draft only.

Metadatum application is a collection of user defined processors. As everything else in SGS, user defined processors are entities and as entities they are defined by schema. It means that each processor has its own index where it keeps history of all executions.

```
├── schemas
│   ├── core
│   │   └── schemas
│   │       ├── big_idx.yaml
│   │       ├── commit_tail.yaml
│   │       ├── commit.yaml
│   │       ├── edge.yaml
│   │       ├── logging.yaml
│   │       ├── registry.yaml
│   │       └── transaction.yaml
│   └── user
│       ├── proc_schemas
│       │   ├── file_ingest.yaml
│       │   ├── file_meta.yaml
│       │   ├── graph_builder.yaml
│       │   ├── load_search.yaml
│       │   └── summary.yaml
│       └── schemas
│           └── file.yaml
└── scripts
    └── metadatum_lib.lua
```

## Installation

This project is a PoC and is not available on PyPI. To install it, you first should install dependencies from requirements.txt file:

```
pip install -r requirements.txt
```

File requirements.txt doesn't include metadatum package and dependencies related to the summary.py processor. This processor can be used as a template for implementation of user defined processors working with transformer's based LLM. 

In our case we are using PyTorch based transformers, so, you should install PyTorch and transformers packages. To install PyTorch, please, follow instructions on https://pytorch.org/get-started/locally/. To install transformers, please, run the following command:

```
pip install transformers
```

Then you can install the metadatum package. This package currently located on (https://test.pypi.org/project/metadatum/), so, it is not included in the requirements.txt file yet. To install it, you should run the following command:

```
pip install -i https://test.pypi.org/simple/ metadatum==0.2.5
```

## Usage

This is a web based application that works with redis database. To run the application, you should first run the redis server. You can do it by running the following command:

```
podman run -p 6379:6379 --name redis-7.0 -it --rm redis/redis-stack:7.0.0-RC4
```

or

```
docker run -p 6379:6379 --name redis-7.0 -it --rm redis/redis-stack:7.0.0-RC4
```

Application depends on the system variable DYNACONF_DOT_META. This variable should point to the directory where the configuration files are located. 

```
export DYNACONF_DOT_META=/<abs path to project>/metadatum_sgs/.meta
```

Then you should run the web server. You can do it by running the following command:

```
uvicorn server:app --reload
```

We are using uvicorn as a web server. It is a lightning-fast ASGI server implementation, using uvloop and httptools. You can find more information about uvicorn on https://www.uvicorn.org/.

Running processors using postman:

```
POST http://127.0.0.1:8000/post
```

First we should run file_meta processor. It collects metadata from pdf files in specified directory and stores it in the database. To run it, you should send the following request:

```
{
    "label": "SOURCE",
    "name": "file_meta",
    "version": "0.1.0",
    "package": "",
    "language": "python",
    "props": {
        "parent_id": "",
        "dir": "/home/alexmy/Downloads/",
        "file_type": ".pdf"
    }
}
```

Then we should run file_ingest processor. It ingests pdf files from specified directory and stores them in the database. To run it, you should send the following request:

```
{
    "label": "COMPLETE",
    "name": "file_ingest",
    "version": "0.1.0",
    "package": "",
    "language": "python",
    
    "props": {
        "parent_id": "file_meta",
        "query": "(@processor_ref: file_meta @status: waiting @item_prefix: file)",
        "limit": 20
    }
}
```

Then we should run load_search processor. It loads the graph into the search engine. To run it, you should send the following request:

```
{
    "label": "SOURCE",
    "name": "load_search",
    "version": "0.1.0",
    "package": "",
    "language": "python",
    
    "props": {
        "parent_id": "file_meta",
        "query": "(hyperparameter | tuning | algorithm)",
        "limit": 20
    }
}
```

Then we should run graph_builder processor. It builds a graph of pdf files and their metadata. To run it, you should send the following request:

```
{
    "label": "BATCH_TRANSFORM",
    "name": "graph_builder",
    "version": "0.1.0",
    "package": "",
    "language": "python",
    
    "props": {
        "graph_name": "mds",
        "rebuild": "yes",
        "parent_id": "load_search",
        "l_threshold": 0.3,
        "r_threshold": 0.30,
        "m_threshold": 0.30,
        "query": "(@processor_ref: load_search @status: waiting @item_prefix: file)",
        "e_query": "*",
        "limit": 20
    }
}
```

Then we should run summary processor. It generates summaries for pdf files. To run it, you should send the following request:

```
{
    "label": "BATCH_TRANSFORM",
    "name": "summary",
    "version": "0.1.0",
    "package": "",
    "language": "python",    
    "props": {
        "model": "sshleifer/distilbart-xsum-6-6",
        "tokenizer":"sshleifer/distilbart-xsum-6-6",
        "query": "*",
        "limit": 20
    }
}
```

PS. Definitely you should replace all hardcoded paths and references with the references to you file location.
