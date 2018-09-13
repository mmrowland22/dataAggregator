import matplotlib.pyplot as plt
import numpy as np
import re


# sorts the numbers ascending alphanumerically
def numsort(list):
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    list = sorted(list, key=alphanum_key)
    return list


# generates graph
def histogram(label, ninety, ninetyfive, path):
    width = 0.35
    index = np.arange(len(label))
    plt.bar(index, ninety, width, color='blue', label='90%')
    plt.bar(index + 0.25, ninetyfive, width, color='green', label='95%')
    plt.ylabel('Response Time (ms)')
    plt.xticks(index + width / 2, label)
    plt.xticks(rotation=90)
    plt.title('Response times at 90% and 95%')
    plt.legend(loc='upper right')
    plt.subplots_adjust(bottom=0.6)
    # Save graph to pdf
    plt.savefig(path + 'statistics_graph.png')
    plt.close()
