"""
This script implements the event study testing methodology from the following paper:
@article{kolari2018event,
  title={Event study testing with cross-sectional correlation due to partially overlapping event windows},
  author={Kolari, James W and Pape, Bernd and Pynnonen, Seppo},
  journal={Mays Business School Research Paper},
  number={3167271},
  year={2018}
}
"""
import pandas as pd


# step 1. calculate the combined number of estimation window residuals of the event firms.
