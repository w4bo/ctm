# -*- coding: utf-8 -*-
from gen_run_CTM import params, datasets
import pandas as pd
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import math
import sys

stats = "../../../../results/CTM_stats.csv"
results = "../../../../results/"

# ==============================================================================
# Chart variables
# ==============================================================================
titlesize = 16
subtitlesize = 14
labelsize = 14
axessize = 12
legendsize = 11
markersize = 5

# http://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
plt.rcParams.update(plt.rcParamsDefault)
# plt.style.use('grayscale')
# plt.rc('text', usetex=True)
plt.rc('font', family='serif')
plt.rcParams['mathtext.fontset'] = 'dejavuserif'
font = font_manager.FontProperties(family='serif', size=legendsize)

# You typically want your plot to be ~1.33x wider than tall. This plot is a rare
# exception because of the number of lines being plotted on it.
# Common sizes: (10, 7.5) and (12, 9)
# Make room for the ridiculously large title.
# plt.subplots_adjust(top=0.8)
figsize = (12, 9)
# figsize = (12,3)

# Markers
# https://matplotlib.org/api/markers_api.html
# Lines
# https://matplotlib.org/gallery/lines_bars_and_markers/line_styles_reference.html

# =============================================================================
# These are the "Tableau 20" colors as RGB.
# http://www.randalolson.com/2014/06/28/how-to-make-beautiful-data-visualizations-in-python-with-matplotlib/
# =============================================================================

# =============================================================================
# Location String	Location Code
# 'best'	0
# 'upper right'	1
# 'upper left'	2
# 'lower left'	3
# 'lower right'	4
# 'right'	5
# 'center left'	6
# 'center right'	7
# 'lower center'	8
# 'upper center'	9
# 'center'	10
# =============================================================================
markers = ["v", "^", "<", ">", "8", "s", "p", "P", "*", "+", "X", "D", "o", "s"]
tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
for i in range(len(tableau20)):
    r, g, b = tableau20[i]
    tableau20[i] = (r / 255., g / 255., b / 255.)

# ==============================================================================
path = '../../../../resources/test/'
outpath = '../../../../resources/test/charts/'


def marker(filename):
    return 6, 'o', '-', 'CTM', 'black'


df = pd.read_csv(stats)
for dataset in datasets:
    for key, values in params.items():
        df = pd.read_csv(stats)
        df = df.loc[df["intable"] == dataset]
        df = df.replace("Infinity", "inf")
        df = df.replace("NoScale", "notime")
        df = df.replace("DailyScale", "daily")
        df = df.replace("WeeklyScale", "weekly")
        df = df.replace("AbsoluteScale", "absolute")
        df = df.replace("trajectory.", "")
        df = df.replace("_standard", "")
        for ikey, ivalues in params.items():
            if key != ikey:
                try:
                    v_def = float(ivalues["default"])
                    df = df.loc[df[ikey].astype(float) == v_def]
                except ValueError:
                    v_def = ivalues["default"]
                    df = df.loc[df[ikey] == v_def]
    
        df = df.groupby([key])
        k = sorted(list(df.groups.keys()))
    
        if (len(k) < 2): # useless to plot a chart with a single X
            continue
    
        fig, ax = plt.subplots(1, 2, figsize=(8, 3))
        msize, m, ls, l, c = marker("foo")
    
        # Plot itemsets
        ax[0].set_ylabel("Itemsets", fontsize=axessize)
        ax[0].plot(k, df['nitemsets'].mean(), label=l, marker=m, markersize=msize, linestyle=ls, fillstyle='none', linewidth=0.8, color=c)
        ax[0].set_xticks(k)
        ax[0].grid(color="lightgray", linestyle='-', linewidth=0.2)
        ax[0].set_axisbelow(True)
        ax[0].set_xlabel(key, fontsize=axessize)
        ax[0].set_yscale('log')
        
        # Plot time
        ax[1].set_ylabel("Time(s)", fontsize=axessize)
        ax[1].plot(k, df['time(ms)'].mean() / 1000.0, label=l, marker=m, markersize=msize, linestyle=ls, fillstyle='none', linewidth=0.8, color=c)
        ax[1].set_xticks(k)
        ax[1].grid(color="lightgray", linestyle='-', linewidth=0.2)
        ax[1].set_axisbelow(True)
        ax[1].set_xlabel(key, fontsize=axessize)
        ax[1].set_yscale('log')
        # ax[1].set_yticks([1, 10, 100, 1000])
        ax[1].legend(handletextpad=0, columnspacing=0.2, labelspacing=0.2, frameon=False, fontsize=legendsize)
        # ax[1].set_ylim([math.pow(10, math.floor(math.log(df['time(ms)'].min() / 1000.0, 10))), math.pow(10, math.ceil(math.log(df['time(ms)'].max() / 1000.0, 10)))])
    
        fig.tight_layout()
        fig.savefig(results + key + "_" + dataset.replace("trajectory.", "").replace("_standard", "") + '.pdf')
        fig.show()