# Methods Utils

## Ubuntu nested
```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.11 python3.11-distutils python3.11-venv python3.11-dev
```


## Create VirtualEnv
```
python3.11 -m venv ./venv
```

## Enable virtual
```
source venv/bin/activate
```

## Install deps
```
pip install -r requirements.txt
```

## Generate Wheel
```
python3 -m build
```

## Tests

```
pytest -v -s
```