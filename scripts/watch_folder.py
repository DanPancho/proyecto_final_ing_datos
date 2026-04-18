import os
import time
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WATCH_DIR = Path("data/processed")
TARGET_FILE = "ecommerce_data_process.csv"

SPARK_COMMAND = [
    "docker", "exec", "spark-master",
    "/opt/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077",
    "--jars", "/opt/data/jars/postgresql.jar",
    "/opt/data/spark_batch.py"
]

class FileArrivalHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.name != TARGET_FILE:
            return

        print(f"Archivo detectado: {file_path}")

        # pequeña espera para evitar leer archivo a medio copiar
        time.sleep(3)

        try:
            print("Ejecutando Spark...")
            result = subprocess.run(
                SPARK_COMMAND,
                check=True,
                text=True,
                capture_output=True
            )
            print("Spark ejecutado correctamente.")
            print(result.stdout)

            os.remove(file_path)
            print(f"Archivo eliminado: {file_path}")
        except subprocess.CalledProcessError as e:
            print("Error ejecutando Spark:")
            print(e.stdout)
            print(e.stderr)

def main():
    WATCH_DIR.mkdir(parents=True, exist_ok=True)

    event_handler = FileArrivalHandler()
    
    observer = Observer()
    observer.schedule(event_handler, str(WATCH_DIR), recursive=False)
    observer.start()

    print(f"Monitoreando carpeta: {WATCH_DIR.resolve()}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

if __name__ == "__main__":
    main()