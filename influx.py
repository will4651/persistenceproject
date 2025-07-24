from influxdb import InfluxDBClient
import random

client = InfluxDBClient(host='localhost', port=8086)

# client.create_database('py')
# results = client.get_list_database()
# print(results)

client.switch_database('pro335')
r = random.random() * 100

json_body = [
    {
        "measurement": "test",
        "tags": {
            "user": "test"
        },
        "fields": {
            "duration": int(r)
        }
    }
]

client.write_points(json_body)
print(r)