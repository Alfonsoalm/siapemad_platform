from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time
import os
import paramiko

# --- Configuración EC2 ---
EC2_HOST = "3.88.169.182"
EC2_USER = "ec2-user"
PATH_TO_PEM_KEY = "C:/Users/CTM40/Desktop/Alfonso Almenara/__Proyectos__/SIAPEMAD/claves-siapemad.pem"
EC2_DEST_DIR = "/home/ec2-user/siapemad_platform/"
LOCAL_EXCEL_FILENAME = "historial_fibaro.xlsx"

# --- Configurar Chrome en modo depuración ---
print("🔧 Iniciando conexión con Chrome en modo depuración (local)...")
chrome_options = Options()
chrome_options.debugger_address = "localhost:9222"
driver = webdriver.Chrome(options=chrome_options)

try:
    print("✅ Conectado a Chrome local. Buscando la pestaña con 'app/history'...")
    found_tab = False
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        print(f"🔍 Verificando pestaña: {driver.current_url}")
        if "app/history" in driver.current_url:
            print("✅ Pestaña correcta encontrada.")
            found_tab = True
            break

    if not found_tab:
        print("❌ No se encontró ninguna pestaña con 'app/history'.")
        driver.quit()
        exit()

    print("⏳ Esperando a que se cargue la página...")
    time.sleep(5)

    print("🔃 Iniciando lectura con scroll dinámico...")

    data = []
    last_row_count = 0
    max_attempts = 10

    for attempt in range(max_attempts):
        print(f"🔁 Iteración de scroll #{attempt + 1}")
        rows = driver.find_elements(By.CLASS_NAME, "event-container")
        print(f"📊 Filas encontradas: {len(rows)} (anteriormente: {last_row_count})")

        if len(rows) == last_row_count:
            print("✅ No se encontraron nuevas filas. Scroll finalizado.")
            break

        last_row_count = len(rows)

        for idx, row in enumerate(rows[len(data):]):
            try:
                estado = row.find_element(By.CLASS_NAME, "event-state").text
                id_ = row.find_element(By.CLASS_NAME, "event-id").text
                nombre = row.find_element(By.CLASS_NAME, "event-name").text
                seccion = row.find_element(By.CLASS_NAME, "event-section-room").text
                tiempo = row.find_element(By.CLASS_NAME, "event-time").text

                registro = {
                    "marquee-text": estado,
                    "event-id": id_,
                    "marquee-text 2": nombre,
                    "marquee-text 3": seccion,
                    "event-time": tiempo
                }
                print(f"📄 Fila {len(data) + 1}: {registro}")
                data.append(registro)
            except Exception as e:
                print(f"⚠️ Error leyendo fila {len(data) + 1}: {e}")


        time.sleep(5)

    if attempt == max_attempts - 1:
        print("⛔ Límite de intentos alcanzado. Finalizando scroll por seguridad.")

    if not data:
        print("⚠️ No se extrajeron datos de la tabla.")
    else:
        df = pd.DataFrame(data)
        df.to_excel(LOCAL_EXCEL_FILENAME, index=False)
        print(f"✅ Excel generado localmente: {LOCAL_EXCEL_FILENAME}")

        print(f"📦 Iniciando transferencia de '{LOCAL_EXCEL_FILENAME}' a EC2 ({EC2_HOST})...")
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=EC2_HOST, username=EC2_USER, key_filename=PATH_TO_PEM_KEY)

            sftp_client = ssh_client.open_sftp()
            try:
                sftp_client.stat(EC2_DEST_DIR)
                print(f"✅ Directorio de destino '{EC2_DEST_DIR}' encontrado en EC2.")
            except FileNotFoundError:
                print(f"🔧 Directorio de destino '{EC2_DEST_DIR}' no encontrado. Creándolo...")
                ssh_client.exec_command(f"mkdir -p {EC2_DEST_DIR}")
                time.sleep(1)
                print("✅ Directorio creado.")

            remote_filepath = os.path.join(EC2_DEST_DIR, LOCAL_EXCEL_FILENAME).replace("\\", "/")
            sftp_client.put(LOCAL_EXCEL_FILENAME, remote_filepath)
            print(f"🎉 ¡Éxito! Archivo '{LOCAL_EXCEL_FILENAME}' enviado a {EC2_HOST}:{remote_filepath}")

        except FileNotFoundError:
            print(f"❌ ERROR: Clave SSH '{PATH_TO_PEM_KEY}' no encontrada.")
        except paramiko.AuthenticationException:
            print("❌ ERROR: Fallo de autenticación. Verifica clave .pem y usuario.")
        except paramiko.SSHException as ssh_exc:
            print(f"❌ ERROR SSH: {ssh_exc}")
        except Exception as e:
            print(f"❌ ERROR inesperado: {e}")
        finally:
            if 'sftp_client' in locals():
                sftp_client.close()
            if 'ssh_client' in locals():
                ssh_client.close()
            print("📦 Conexión SSH cerrada.")

finally:
    print("🚪 Cerrando conexión con el driver (el navegador sigue abierto)...")
    if 'driver' in locals():
        driver.quit()
