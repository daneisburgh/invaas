import os
import sys
import warnings

warnings.filterwarnings("ignore")

module_path = os.path.abspath(os.path.join(".."))

if module_path not in sys.path:
    sys.path.append(module_path)
