import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import boto3
from matplotlib.ticker import PercentFormatter

# === Step 1. Download data ===
dynamodb = boto3.resource("dynamodb",
    region_name='ap-southeast-1',
    aws_access_key_id='AKIAICTJL7QVLLWZ5MIQ',
    aws_secret_access_key='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'
)
table = dynamodb.Table('EE5111_A0039875M_JetEngine')
response = table.scan()
data = response['Items']
while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])
data = pd.DataFrame(data)

# === Step 2. Plot Graph ===
# Plot single engine single sensor
data1Sensor2 = data[data['id'].str.contains('FD001')].loc[:, ['id','cycle','sensor2']]
data1Sensor2['id'] = list(map(lambda x: int(x.split('_')[-1]), data1Sensor2['id']))
data1Sensor2.sort_values(by=['id','cycle'], inplace=True)
plt.figure()
plt.plot(range(len(data1Sensor2)), data1Sensor2['sensor2'], linewidth=0.1)
plt.title('Engine 1 Sensor 2 in 99 Cycles')
plt.figure()
plt.plot(range(len(data1Sensor2[data1Sensor2['id']==1])), data1Sensor2[data1Sensor2['id']==1]['sensor2'])
plt.title('Engine 1 Sensor 2 in First Cycle')
plt.show()

# Comparsion of time plot
data1 = data[data['id'] == 'FD001_1']
data2 = data[data['id'] == 'FD002_1']
for i in range(1, 22):
    plt.figure()
    plt.plot(data1['cycle'], data1['sensor'+str(i)], 'b-', label='Engine 1')
    plt.plot(data2['cycle'], data2['sensor'+str(i)], 'g-', label='Engine 2')
    plt.legend()
    plt.xlabel('Cycle')
    plt.title('Sensor '+str(i)+' Data from Two Engines')
    plt.savefig('./output/compare_sensor'+str(i)+'.png')
    plt.close()

# Comparision of lifecyle
dataCycle1 = data[data['id'].str.contains('FD001')].groupby('id', as_index=False)['cycle'].max()
dataCycle2 = data[data['id'].str.contains('FD002')].groupby('id', as_index=False)['cycle'].max()
dataCycle1['id'] = list(map(lambda x: int(x.split('_')[-1]), dataCycle1['id']))
dataCycle2['id'] = list(map(lambda x: int(x.split('_')[-1]), dataCycle2['id']))
plt.figure()
plt.hist(dataCycle1.loc[:,'cycle'].astype(int), rwidth=0.9, weights=np.ones(len(dataCycle1))/len(dataCycle1))
plt.gca().yaxis.set_major_formatter(PercentFormatter(1,decimals=0))
plt.xlabel('Cycle')
plt.ylabel('Percentage')
plt.title('Lifecycle Distribution for the First Engine')
plt.figure()
plt.hist(dataCycle2.loc[:,'cycle'].astype(int), rwidth=0.9, weights=np.ones(len(dataCycle2))/len(dataCycle2))
plt.gca().yaxis.set_major_formatter(PercentFormatter(1,decimals=0))
plt.xlabel('Cycle')
plt.ylabel('Percentage')
plt.title('Lifecycle Distribution for the Second Engine')
print("Average lifecycle of first Engine:", np.mean(dataCycle1.loc[:,'cycle'].astype(int)))
print("Average lifecycle of second Engine:", np.mean(dataCycle2.loc[:,'cycle'].astype(int)))
plt.show()
