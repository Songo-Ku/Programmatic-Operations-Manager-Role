# Programmatic-Operations-Manager-Role
**Data analysis** Python/Pandas 
Repo prepared to recruitment process to role: **Programmatic Operations Manager**.

https://github.com/Songo-Ku/Programmatic-Operations-Manager-Role
---

## Content

1. [Download from github](#download-from-github)
2. [Installation](#instalacja)
3. [File Manipulation](#File-Manipulation)
4. [Part I data analysis](#DATA-ANALYSIS)
5. [Part II ETL](#ETL)
6. [Part III Prebid](#PREBID-TASK-3)

---


---
## download-from-github

### 1. clone repo

```bash git clone https://github.com/TWOJE-KONTO/Programmatic-Operations-Manager-Test.git```

### 2. Przejdź do katalogu projektu

```bash cd Programmatic-Operations-Manager-Test```



## Instalacja

### 1. Wirtualne środowisko - utworzenie
Tworzenie wirtualnego środowiska pozwala na izolację zależności projektu od globalnych bibliotek Pythona.
Komenda może się różnić w zależności od systemu operacyjnego, ale też instalacji pythona czy roznych zmiennych srodowiskowych na systemie operacyjnym.
W terminalu/cmd wpisz:

Windows:

```bash python -m venv venv```

Linux/Mac:

```bash python3 -m venv venv```

### 2. Venv aktywacja
Trzeba aktywować wirtualne środowisko, aby mieć wyizolowane biblioteki i projekt i działał zgodnie z wymaganiami projektu.
Windows:

```bash venv\Scripts\activate```

Linux/Mac:

```bash source venv/bin/activate```

### 3. Zależności - instalacja
W pliku requirements.txt znajduja sie wymagane biblioteki do poprawnego działania projektu.

```bash 
python.exe -m pip install --upgrade pip
pip install -r requirements.txt
```

Po samym zainstalowaniu zależności, projekt jest gotowy do uruchomienia, ale warto sprawdzić jakie mamy zainstalowane zależności komendą:
pip freeze
wyśweitlona zostanie lista zależności i wersji zainstalowanych w projekcie.

## DATA-ANALYSIS

### Description
I recommend use pycharm to use this project. It is easy to use and run scripts.


### 1. Run script
You should be in place where you downloaded project recruRTB from github as git clone.
If you are not in main directory of project "recruRTB" use commend:

```cd recruRTB```

but you have to be in place where this directory exist. You should load it to pycharm as project. 

To properly script might work you have to delivery:
to folder xlsx_files you should paste your data set: "Data_Analysis_Programmatic_Operations_Manager.xlsx"

Then you run:

you have full access to use analysis by python file:
excel_analysis.py

Everything will be control by this script. Results and so on. I also generated reuslt and will paste as answer to link with solution.

``````
in terminal run:
python excel_analysis.py
``````

## ETL

### 1. Run script

You should be in place where you downloaded project recruRTB from github as git clone.
If you are not in main directory of project "recruRTB" use commend:

```cd recruRTB```

but you have to be in place where this directory exist in your system linux or win. 

To properly script might work you have to delivery:
to folder csv_files you should paste your data set: "marketing_campaigns.csv" and "sales_data.csv"

Then you run:

whole ETL process as pipeline by python file/script:
etl.py

``````
in terminal run:
python etl.py
``````

## PREBID-TASK-3

### Description

I tried to create simple solution for Prebid task 3 as requested in requirements. 

I cannot agree it works 100% but I fixed a lot of issue I handled when requested for help.

In chrome it deosnt work at all, in firefox it works but I cant evaluate 100% to be sure. 


### Run:
You should be in directory recruRTB this is main project directory.

Go to: Prebid_solution_task_3 by commend:

```cd Prebid_solution_task_3```

Possible way to test solution with local server like Live Server builtin in VS Code or similar.
I used pycharm and basic commend in terminal:
```
python -m http.server 8000 
```
It caused we can use port 8000 to this task. Then user should open browser.
define adress url:

``` http://localhost:8000```

Then file: demo.html and double click. It changes adress url into: 

```http://localhost:8000/demo.html```

Browser use demo.html as main file to render.

Solution should work correctly and it is answer to PREBID task 3.

It should display banner add as required in task with 2 condition with and without bid as passback. 
Solution does not rely on Adserver like DFP/GAM.
I embedded and run scirpt to prebid.js from that link:. 
```
https://cdn.jsdelivr.net/npm/prebid.js@latest/dist/not-for-prod/prebid.js
```
It should generate bids from RTBhouse if exist winner and bid or display passback banner
