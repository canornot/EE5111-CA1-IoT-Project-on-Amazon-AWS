from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter
import datetime, time, json, boto3

# # ==== Step 1. Setup shadow ====
# folderPath = "./certificates/q5_solar_radiation/"

# # A random programmatic shadow client ID.
# SHADOW_CLIENT = "myShadowClient"

# # The unique hostname that &IoT; generated for 
# # this device.
# HOST_NAME = "a32qbh1jp4p8i5-ats.iot.ap-southeast-1.amazonaws.com"

# # The relative path to the correct root CA file for &IoT;, 
# # which you have already saved onto this device.
# ROOT_CA = folderPath + "AmazonRootCA1.pem"

# # The relative path to your private key file that 
# # &IoT; generated for this device, which you 
# # have already saved onto this device.
# PRIVATE_KEY = folderPath + "e52c555b58-private.pem.key"

# # The relative path to your certificate file that 
# # &IoT; generated for this device, which you 
# # have already saved onto this device.
# CERT_FILE = folderPath + "e52c555b58-certificate.pem.crt.txt"

# # A programmatic shadow handler name prefix.
# SHADOW_HANDLER = "EE5111_A0039875M_SolarRadiation"

# # Automatically called whenever the shadow is updated.
# def myShadowUpdateCallback(payload, responseStatus, token):
#     print()
#     # print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
#     # '/shadow/update/#')
#     # print("payload = " + payload)
#     # print("responseStatus = " + responseStatus)
#     # print("token = " + token)

# # Create, configure, and connect a shadow client.
# myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
# myShadowClient.configureEndpoint(HOST_NAME, 8883)
# myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
#   CERT_FILE)
# myShadowClient.configureConnectDisconnectTimeout(10)
# myShadowClient.configureMQTTOperationTimeout(5)
# myShadowClient.connect()

# # Create a programmatic representation of the shadow.
# myDeviceShadow = myShadowClient.createShadowHandlerWithName(
#   SHADOW_HANDLER, True)

# # ==== Step 2. Upload data ====
# class NpEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.integer):
#             return int(obj)
#         elif isinstance(obj, np.floating):
#             return float(obj)
#         elif isinstance(obj, np.ndarray):
#             return obj.tolist()
#         else:
#             return super(NpEncoder, self).default(obj)

# Process raw data
fileCount = 5
data = pd.read_csv("./input/solar1.csv")
for i in range(2, fileCount+1):
    curData = pd.read_csv("./input/solar"+str(i)+".csv")
    data = data.append(curData)
data.rename(columns={
    'Timestamp':'time',
    'outdoor-module-performance-test ISET Sensor 1 - Irradiation [W/qm]':'sensor1',
    'outdoor-module-performance-test ISET Sensor 2 - Irradiation [W/qm]':'sensor2',
    'outdoor-module-performance-test ISET Sensor 3 - Irradiation [W/qm]':'sensor3'
    }, inplace=True)
data = data.loc[:,['time','sensor1', 'sensor2', 'sensor3']]

# # Upload data
# for i in range(0, len(data)):
#     curTime = str(datetime.datetime.utcnow())
#     curData = data.iloc[i]
#     curData = curData.append(pd.Series([curTime], index=['timestamp']))      # add current time
#     curData = curData.to_dict()
#     curData = {"state": {"reported": curData}}
#     curJson = json.dumps(curData, cls=NpEncoder)
#     myDeviceShadow.shadowUpdate(curJson, myShadowUpdateCallback, 5)
#     print(i+1, "uploaded")
#     time.sleep(0.05)

# # === Step 3. Download data ===
# dynamodb = boto3.resource("dynamodb",
#     region_name='ap-southeast-1',
#     aws_access_key_id='AKIAICTJL7QVLLWZ5MIQ',
#     aws_secret_access_key='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'
# )
# table = dynamodb.Table('EE5111_A0039875M_SolarRadiation')
# response = table.scan()
# data = response['Items']
# while 'LastEvaluatedKey' in response:
#     response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
#     data.extend(response['Items'])
# data = pd.DataFrame(data)

# ==== Step 4. Process data ====
binCount = 24
minIrradiance, maxIrradiance = 10, 1200

# Filter out blank time
data = data[data['sensor1'].notnull() | data['sensor2'].notnull() | data['sensor3'].notnull()]
data['irradiance'] = data[['sensor1','sensor2','sensor2']].mean(axis=1)

# Select data within time range and irradiance range
data['hour'] = list(map(lambda x: int(x.split(' ')[-1].split(':')[0]), data['time']))
data = data[((data['hour']>=7) & (data['hour']<=19) & \
    (data['irradiance']>=minIrradiance) & (data['irradiance']<=maxIrradiance))]

# Calculate time distribution and energy distribution
dataEnergy = data.groupby(pd.cut(data["irradiance"], \
    np.arange(0,maxIrradiance+1,maxIrradiance/binCount))).sum()
dataEnergy.index.set_names('range', inplace=True)
dataEnergy = dataEnergy.reset_index()

# === Step 5. Plot Graph ===
plt.figure()
plt.hist(data.loc[:,'irradiance'].astype(float), bins=binCount, rwidth=0.9, \
    weights=np.ones(len(data))/len(data))
plt.gca().yaxis.set_major_formatter(PercentFormatter(1,decimals=0))
plt.xlabel('Irrandiance [W/m2]')
plt.ylabel('Time Percentage')
plt.title('Singapore Solar Radiation Time Distribution')

plt.figure()
energySum = data.loc[:,'irradiance'].astype(float).sum()
plt.hist(data.loc[:,'irradiance'].astype(float), bins=binCount, rwidth=0.9, \
    weights=data.loc[:,'irradiance'].astype(float)/energySum)
plt.gca().yaxis.set_major_formatter(PercentFormatter(1,decimals=0))
plt.xlabel('Irrandiance [W/m2]')
plt.ylabel('Energy Percentage')
plt.title('Singapore Solar Radiation Power Distribution')
plt.show()