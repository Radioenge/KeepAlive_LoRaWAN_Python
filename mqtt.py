import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
#import time
import json
import base64
from datetime import datetime
import sqlite3

DEVICE_NAME = 'DEV3634-8517'

TTN_BROKER = 'eu.thethings.network'
TTN_USERNAME = 'hwid3011'
#Access Keys da Aplicação  :fica no final da página da aplicação.
TTN_PASSWORD = 'ttn-account-v2.xMwGc-trocar pela susa chave -8pS4Q16g4Ml09b8XnswMEM'
TTN_TOPIC = '+/devices/+/up'
#.format(DEVICE_NAME)

LOCAL_BROKER = '127.0.0.1'
LOCAL_TOPIC = 'home/ttn/payload'


def on_connect(client, userdata, flags, rc):
    """Subscribe to topic after connection to broker is made."""
    print("Connected with result code", str(rc))
    client.subscribe(TTN_TOPIC)
    print("\n Teste Conexao\n")

def on_message(client, userdata, msg):
    """Relay message to a different broker."""
    try:
        pay = json.loads(msg.payload)
        port = pay["port"]
        print(">>>>>------------------------------------------")
        print(pay["dev_id"])
        if(port == 215):
                print("keep alive: ")
                dado = (pay["payload_raw"])
                #print(pay["payload_raw"])
                dado = base64.b64decode(dado).hex()
                x = str(dado).find("ff")
                bat = str(dado)[(x-4):(x-2)]
                i= ((1.8/255)*int(bat,16 ))+1.8
                print("valor bateria = ",round(i,2))
#               ------------------------------------
                ad0_msb = str(dado)[(x+6):(x+8)]
                ad0_lsb = str(dado)[(x+8):(x+10)]
                j=(3.3/4092)*((int(ad0_msb,16)*256)+(int(ad0_lsb,16)))
                print('   ->AD0: ', round(j,2))
                ad1_msb = str(dado)[(x+10):(x+12)]
                ad1_lsb = str(dado)[(x+12):(x+14)]
                k=(3.3/4092)*((int(ad1_msb,16)*256)+(int(ad1_lsb,16)))
                print('   ->AD1', round(k,2))
                entradas = str(dado)[(x+4):(x+6)]
                #print("Entradas: ", entradas)
                entradas_bin = "{0:08b}".format(int(entradas, 16)) 
                print("Entradas: ", entradas_bin)
#               count = 0
#                while (entradas_bin >0):
#                       if( (entradas_bin << count) & 0x01):
#                               print('Entrada ', count, 'alta')
#                       else:
#                               print('Entrada ', count, 'baixa')
#                       count = count+1
                # datetime object containing current date and time
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                print("data: ", dt_string)
                sql_bd = 0 #mudar para 1 após criar o banco de dados sqlite
				# CREATE TABLE lorakeepalive(data text, deveui text, devid, bateria float, ad0 float, ad1 float, port  int);
                if(sql_bd):
                    try:
                            print('date and time =' , dt_string)
                            con_sql=sqlite3.connect('c:\sqlite\lorakeepalive.db')
                            cursor = con_sql.cursor()
                            dev_eui = pay["hardware_serial"]
                            dev_id = pay["dev_id"]
                            cursor.execute("""INSERT INTO lorakeepalive(data, deveui, devid, bateria, ad0, ad1, port) VALUES(?, ?, ?, ?,?, ?, ?)""",(dt_string, dev_eui, dev_id, round(i,2), round(j,2), round(k,2), int(entradas,) ))
                            con_sql.commit()
                            con_sql.close()
                    except Exception as e:
                            print('Exception', e)
        else:
                print("   -> port: ", pay["port"])
                print("Payload: ", base64.b64decode(pay["payload_raw"]))
        print("<<<<<------------------------------------------")  
    except:
        print("\nErro ao tratar o pacote!\n")
    #mensagem = msg
    publish.single(
                LOCAL_TOPIC, payload=msg.payload, qos=0, retain=False,
                hostname=LOCAL_BROKER, port=1883, client_id='ttn-local',
                keepalive=60, will=None, auth=None, tls=None, protocol=mqtt.MQTTv311)
    #print(mensagem)
#    print(ms.payload)    

client = mqtt.Client()
client.username_pw_set(TTN_USERNAME, password=TTN_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(TTN_BROKER, 1883, 60)

client.loop_forever()
#Fontes consultadas:
# https://pypi.org/project/paho-mqtt/
# https://8gwifi.org/base64Hex.jsp
# https://www.dotnetperls.com/substring-python
# https://stackoverflow.com/questions/209513/convert-hex-string-to-int-in-python
# https://stackoverflow.com/questions/455612/limiting-floats-to-two-decimal-points
# https://www.programiz.com/python-programming/datetime/current-datetime
# https://medium.com/@mattvianna/sqlite-principais-comandos-dicas-como-instalar-parte-1-bb94e0134935
# https://stackoverflow.com/questions/4360593/python-sqlite-insert-data-from-variables-into-table
# https://www.sqlite.org/2020/sqlite-tools-win32-x86-3320300.zip
# Instalar o SQLITE e criar a tabela:
# CREATE TABLE lorakeepalive(data text, deveui text, devid, bateria float, ad0 float, ad1 float, port  int);