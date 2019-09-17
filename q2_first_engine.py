from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import numpy as np
import pandas as pd
import datetime, time, json

# ==== Step 1. Setup shadow ====
folderPath = "./certificates/q2_first_engine/"

# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a32qbh1jp4p8i5-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = folderPath + "AmazonRootCA1.pem"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = folderPath + "18bfed9318-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = folderPath + "18bfed9318-certificate.pem.crt.txt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "EE5111_A0039875M_JetEngine1"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
    print()
    print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
    '/shadow/update/#')
    print("payload = " + payload)
    print("responseStatus = " + responseStatus)
    print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
  CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
  SHADOW_HANDLER, True)

# ==== Step 2. Readin and process data ====
names = ['id','cycle','os1','os2','os3'] + ['sensor'+str(i) for i in range(1,24)]
data = pd.read_csv("./input/train_FD001.txt", delimiter=' ', names=names, usecols=names[:-2])
data['id'] = list(map(lambda s: 'FD001_'+str(s), data['id']))               # modifiy id
data['maric Number'] = 'A0039875M'                                          # add matric number

# ==== Step 3. Upload data ====
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
         
for i in range(0, len(data)):
    curTime = str(datetime.datetime.utcnow())
    curData = data.iloc[i]
    curData = curData.append(pd.Series([curTime], index=['timestamp']))      # add current time
    curData = curData.to_dict()
    curData = {"state": {"reported": curData}}
    curJson = json.dumps(curData, cls=NpEncoder)
    myDeviceShadow.shadowUpdate(curJson, myShadowUpdateCallback, 5)
    # print(i+1, "uploaded")
    time.sleep(0.3)


