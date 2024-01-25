import sys
import pandas as pd

print(sys.argv)

#sys = common line arguments pass to script. [0]=filename [1]=what we pass
day = sys.argv[1]

#sample python pipeline

print(f'Job finished successfully for day = {day}')