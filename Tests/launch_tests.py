import os

os.environ["TEST"] = "1"
os.system("pytest")
os.environ["TEST"] = "0"
