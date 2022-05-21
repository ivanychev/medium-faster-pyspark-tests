## Faster PySpark tests

This is the example PySpark test set up for my Medium article written [here](https://medium.com/p/1cb7dfa6bdf6/edit).

## How to use it?

* Install Python 3.8.
* Create a virtual environment via `python3 -m venv venv` and activate it `source venv/bin/activate`
* Install poetry `pip3 install poetry`
* Install all dependencies `poetry install`
* Run tests

```
pytest tests --dist loadfile -n 2
```