# Ginan - UI

The newly maintained repository of Ginan-UI

An intelligent user friendly interface for using the Geoscience Australia GNSS processing tool ginan. Made using pyside6 by students of the 2025 ANU Techlauncher program.

[User manual available here](https://drive.google.com/file/d/1ThoVqijgRQ1KV9QfP7m5iflhWILdRoD3/view?usp=drive_link)

## Installation 
### Option A: Install from distribution

Download a binary from the releases tab on the right hand side.
```
gunzip ginan-ui-<os>-<cpu>.tar.gz 
tar -xf ginan-ui-<os>-<cpu>.tar
cd ginan-ui
./ginan-ui 
```

### Option B: From source
Tested with python 3.9+
```
git clone https://github.com/u7327620/Ginan-UI
cd Ginan-UI
pip install -r requirements.txt
python main.py
```
