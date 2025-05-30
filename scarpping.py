from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time

print("🔧 Iniciando conexión con Chrome en modo depuración...")

# Configurar opciones para conectarse al Chrome existente
chrome_options = Options()
chrome_options.debugger_address = "localhost:9222"  # Puerto expuesto

# Crear driver conectado al navegador abierto
driver = webdriver.Chrome(options=chrome_options)

try:
    print("✅ Conectado a Chrome. Buscando la pestaña con 'app/history'...")

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

    print("🔍 Buscando filas de la tabla...")
    rows = driver.find_elements(By.CLASS_NAME, "event-container")
    print(f"✅ {len(rows)} filas encontradas.")

    data = []

    for idx, row in enumerate(rows):
        try:
            estado = row.find_element(By.CLASS_NAME, "event-state").text
            id_ = row.find_element(By.CLASS_NAME, "event-id").text
            nombre = row.find_element(By.CLASS_NAME, "event-name").text
            seccion = row.find_element(By.CLASS_NAME, "event-section-room").text
            tipo_fuente = row.find_element(By.CLASS_NAME, "event-source-type").text
            tiempo = row.find_element(By.CLASS_NAME, "event-time").text

            registro = {
                "Estado": estado,
                "ID": id_,
                "Nombre": nombre,
                "Sección/Habitación": seccion,
                "Tipo fuente": tipo_fuente,
                "Hora": tiempo
            }
            print(f"📄 Fila {idx + 1}: {registro}")
            data.append(registro)
        except Exception as e:
            print(f"⚠️ Error leyendo fila {idx + 1}: {e}")

    if not data:
        print("⚠️ No se extrajeron datos de la tabla.")

    # Exportar a Excel
    df = pd.DataFrame(data)
    df.to_excel("historial_fibaro.xlsx", index=False)
    print("✅ Excel generado: historial_fibaro.xlsx")

finally:
    print("🚪 Cerrando conexión con el driver (el navegador sigue abierto)...")
    driver.quit()
