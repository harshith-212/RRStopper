import requests
import json
from base64 import b64encode
import configparser


def get_ambari_credentials():
    """Get the Ambari credentials from the configuration file."""
    config = configparser.ConfigParser()
    config.read("ambari.cfg")

    hostname = config["ambari"]["hostname"]
    port = config["ambari"]["port"]
    username = config["ambari"]["username"]
    password = config["ambari"]["password"]
    cluster_name = config["ambari"]["cluster_name"]
    httpss = config["ambari"]["httpss"]
    return hostname, port, username, password, cluster_name, httpss


def basic_auth(username, password):
    token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
    return f'Basic {token}'


hostname, port, username, password, cluster_name, httpss = get_ambari_credentials()

url = httpss + "://" + hostname + ":" + port + "/api/v1/clusters/" + cluster_name + "/request_schedules"


headers = {
    "Authorization": basic_auth(username, password),
    "X-Requested-By": "ambari",
}


response = requests.get(url, headers=headers)
print("response:", response)

if response.status_code == 200:

    json_data = response.json()
    list = json_data["items"]
    url_det = url + "/" +str(list[-1]['RequestSchedule']['id'])
    response = requests.get(url_det, headers=headers)
    json_data = response.json()
    temp = json_data["RequestSchedule"]["batch"]["batch_requests"][0]['request_body']
    data = json.loads(temp)
    print(":::::::::::::::::::::  STOPPING THE BELOW SERVICE ROLLING RESTART AND SUBSQUENT BATCHES :::::::::::::::::::: \n", data["Requests/resource_filters"][0]["component_name"])
    resp = requests.delete(url_det, headers=headers)
    if(resp.status_code == 200):
        print("::::::::::::::::::::: STOPPED SUCCESSFULLY ::::::::::::::::::::")

else:

    print(response.content)
