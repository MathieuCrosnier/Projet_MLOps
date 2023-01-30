import os

os.environ["TEST"] = "1"
os.system("pytest tests.py")
os.environ["TEST"] = "0"