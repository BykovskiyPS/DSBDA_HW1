import time
import random
import string

METRIC_NAMES = {
    1: "Node1CPU",
    2: "Node2CPU",
    3: "Node1RAMmb",
    4: "Node2RAMmb"
}
# The maximum value accepted by the metric
MAX_VALUE = 100
# Event time window
MILLIS = 1000
# Probability of hitting a broken string
PROBABILITY = 1000

# Determines the occurrence of an event (broken string)
# with a given probability
def flipCoin(probability=0):
    value = random.randint(1, probability)
    if value == 1:
        return True
    return False


# return random string consists from letters and punctuation marks
def randomString():
    lowercases = string.ascii_lowercase
    uppercases = string.ascii_uppercase
    punctuations = string.punctuation
    symbols = lowercases + uppercases + punctuations
    return ''.join(random.choice(symbols) for i in range(20))


print("Enter amount lines: ")
amountLines = int(input())
print("Enter output file name: ")
outputfile = input()
now = int(time.time()) * 1000

with open("metricNames", 'w', encoding='utf-8') as mfile:
    metrics = []
    mfile.write(str(now) + " ")
    for item in METRIC_NAMES.items():
        metric = "{metricid}-{metricName}".format(metricid=item[0], metricName=item[1])
        metrics.append(metric)
    mfile.write(" ".join(metrics) + "\n")

with open(outputfile, 'w', encoding='utf-8') as f:
    for i in range(amountLines):
        now += random.randint(0, MILLIS)
        if flipCoin(probability=PROBABILITY):
            f.write(randomString() + "\n")
            continue

        str = "{metricId}, {millis}, {value}\n".format(
            metricId=random.randint(1, len(METRIC_NAMES)),
            millis=now,
            value=random.randint(1, MAX_VALUE)
        )
        f.write(str)
