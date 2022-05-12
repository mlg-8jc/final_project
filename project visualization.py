# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats




#import data
filename= r"C:\Users\Student\Business Sales Transaction.csv"
data= pd.read_csv(filename)

#scatterplot
vis=sns.scatterplot(x="Quantity", y= "Price", data=data)
print(vis)



