# petulant_rutabaga.py setup


Installing Python
=========
* Please download anaconda with Python 3 from: https://www.continuum.io/downloads/
* Install it according to your system using the instructions found on that website
* In your command prompt ensure you have downloaded anaconda by typing
```
conda --version

```
* Also download tqdm (this may require admin rights) by typing
```
pip install pymongo

```

Running petulant_rutabaga.py
===========

To run the function petulant_rutabaga:
* Load your command prompt (cmd in Windows, terminal in Mac and Linux).
* Change the directory to the one where petulant_rutabaga.py is located.
* Either make sure your csv file is in this folder, or in the command
  give the full filepath as well as the csv name.
* Type the following command into the command prompt:
```
python petulant_rutabaga.py csv_filename
```
* This will create a new database with the data from the csv specified.
  It will then run a series of 8 queries on these data.
* To run any of the code within the python interpreter type in the command
```
ipython
```
```
%run petulant_rutabaga.py
```
* To run entire pipeline type

```
run_entire_pipeline()
```
  * If using a Windows machine then the file paths should be like the following:

```
'C:\\Users\\user\\cw2'
```
