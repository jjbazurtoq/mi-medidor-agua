import os
import json
import time
import ssl
import requests
import paho.mqtt.client as mqtt

# Configuración (tu empleado lee esto de secretos)
SERVIDOR_MQTT = os.getenv('MQTT_SERVIDOR')
USUARIO_MQTT = os.getenv('MQTT_USUARIO')
CONTRASEÑA_MQTT = os.getenv('MQTT_CONTRASEÑA')
URL_GOOGLE = os.getenv('GOOGLE_URL')

# Lista para guardar los datos que lleguen
datos_recibidos = []

def cuando_se_conecte(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Conectado al buzón MQTT")
        client.subscribe("medidor-agua/datos")  # Escuchar este "canal"
    else:
        print(f"❌ No se pudo conectar: {rc}")

def cuando_llegue_dato(client, userdata, msg):
    try:
        # Convertir el mensaje a datos entendibles
        mensaje = msg.payload.decode('utf-8')
        datos = json.loads(mensaje)
        
        print(f"📩 Llegó dato: {datos}")
        datos_recibidos.append(datos)
        
    except Exception as e:
        print(f"❌ Error leyendo dato: {e}")

def enviar_a_google_sheets(datos):
    try:
        # Enviar como lo hacías antes
        url_completa = f"{URL_GOOGLE}?action=send_data&total_m3={datos['total_m3']}&timestamp={datos['timestamp']}&device_id={datos['device_id']}&status=active"
        
        respuesta = requests.get(url_completa, timeout=30)
        
        if respuesta.status_code == 200:
            print(f"✅ Enviado a Google: {datos['device_id']}")
            return True
        else:
            print(f"❌ Error enviando: {respuesta.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("🚀 Mi empleado empezó a trabajar")
    
    # Conectar al buzón MQTT
    cliente = mqtt.Client()
    cliente.username_pw_set(USUARIO_MQTT, CONTRASEÑA_MQTT)
    
    # Configurar seguridad
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    cliente.tls_set_context(context)
    
    cliente.on_connect = cuando_se_conecte
    cliente.on_message = cuando_llegue_dato
    
    try:
        # Conectarse al servidor
        cliente.connect(SERVIDOR_MQTT, 8883, 60)
        cliente.loop_start()
        
        # Escuchar por 60 segundos
        print("👂 Escuchando datos por 60 segundos...")
        time.sleep(60)
        
        cliente.loop_stop()
        cliente.disconnect()
        
        # Enviar todos los datos recibidos
        print(f"📤 Enviando {len(datos_recibidos)} datos a Google")
        
        for dato in datos_recibidos:
            enviar_a_google_sheets(dato)
            time.sleep(1)  # Pausa 1 segundo entre envíos
        
        print("✅ Trabajo terminado")
        
    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    main()
