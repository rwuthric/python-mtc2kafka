# Unit tests

## Test environment
Create the folder `venv` within the project folder. This folder will contain your virtual test environment:
```
mkdir venv
```
Create a virtual environment for your tests and activate it:
```
python3 -m venv venv/testenv
source venv/testenv/bin/activate
```
Add the required libraries:
```
(testenv) python3 -m pip install kafka-python requests
```
Note: to test your changes made to the code, it is important to use a virtual environment which does not have python-mtc2kafka installed.

## Run tests
To run all unit tests in the `tests` folder run in the project folder (make sure your test environment is activated):
```
python -m unittest discover tests
```
A more verbouse output can be optained like so:
```
python -m unittest discover tests -v
```
Prior committing new code to the repository all tests must run successfully.