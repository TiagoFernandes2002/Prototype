import can
import json
import paho.mqtt.client as mqtt

client = mqtt.Client(client_id="SensorClient")


def connect_mqtt():
    """
    Conecta ao broker MQTT especificado.
    """
    global client

    broker_address = "192.168.28.96"
    port = 1884

    def on_log(client, userdata, level, buf):
        print(f"Log MQTT: {buf}")

    # Usar o callback da versão 2
    client.on_log = on_log

    try:
        # Conectar ao broker
        client.connect(broker_address, port, keepalive=60)
        print(f"Conectado ao broker MQTT {broker_address}:{port}")
    except Exception as e:
        print(f"Erro ao conectar ao broker MQTT: {e}")
        raise



# Função para converter JSON em CAN
def json_to_can(json_data):
    """
    Converte JSON em uma mensagem CAN adaptada ao algoritmo.
    """
    arbitration_ids = {
        "BlindSpotDetection": 0x100,
        "PedestrianDetection": 0x101,
        "FrontalCollision": 0x102,
        "RearCollision": 0x103
    }

    algorithm_id = json_data['AlgorithmID']
    arbitration_id = arbitration_ids.get(algorithm_id, 0x1FF)

    # Valores comuns
    status = 1 if json_data['Status'] else 0
    distance = int(json_data['Data']['DistanceToVehicle'] * 100)  # Distância em cm

    # Mensagem CAN específica por algoritmo
    if algorithm_id == "BlindSpotDetection":
        side = 1 if json_data['Data']['Side'].lower() == 'direita' else 0
        data_bytes = [
            status,                     # Byte 0: Status
            distance & 0xFF,            # Byte 1: LSB
            (distance >> 8) & 0xFF,     # Byte 2: MSB
            side                        # Byte 3: Lado
        ]
    else:
        # Para outros algoritmos, lado não é relevante
        data_bytes = [
            status,                 # Byte 0: Status
            distance & 0xFF,        # Byte 1: LSB
            (distance >> 8) & 0xFF  # Byte 2: MSB
        ]

    # Completar com zeros até 8 bytes
    data_bytes += [0] * (8 - len(data_bytes))

    return can.Message(
        arbitration_id=arbitration_id,
        data=data_bytes,
        is_extended_id=False
    )


def send_can_message(json_input):
    connect_mqtt()
    client.loop_start()  # Inicia o loop de evento
    json_data = json.loads(json_input)
    can_message = json_to_can(json_data)

    mqtt_topic = "can/messages"
    mqtt_payload = {
        "AlgorithmID": json_data["AlgorithmID"],
        "CAN_Message": {
            "arbitration_id": can_message.arbitration_id,
            "data": list(can_message.data)
        }
    }

    # Publica a mensagem
    result = client.publish(mqtt_topic, json.dumps(mqtt_payload))
    result.wait_for_publish()  # Aguarda a publicação ser confirmada
    print(f"Mensagem publicada no tópico MQTT {mqtt_topic}: {mqtt_payload}")

    # Aguarda um curto intervalo para garantir o envio
    import time
    time.sleep(1)

    client.loop_stop()
    client.disconnect()
    print("Desconectado do broker MQTT")


if __name__ == "__main__":
    example_json = '''
    {
      "AlgorithmID": "BlindSpotDetection",
      "Timestamp": "2024-04-27T12:35:56.789Z",
      "Priority": "Medium",
      "Status": true,
      "MessageID": "bs-12345",
      "Data": {
        "Side": "Direita",
        "DistanceToVehicle": 1.5
      }
    }
    '''

    example_json2 = '''
    {
      "AlgorithmID": "RearCollision",
      "Timestamp": "2024-04-27T12:35:56.789Z",
      "Priority": "Medium",
      "Status": true,
      "MessageID": "rc-54321",
      "Data": {
        "DistanceToVehicle": 2.0
      }
    }
    '''

    send_can_message(example_json)
    send_can_message(example_json2)

