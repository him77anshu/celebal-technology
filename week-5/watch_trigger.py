from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import subprocess

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".trigger"):
            print("Trigger file detected! Running export...")
            subprocess.run(["python", "export_script.py"])

watch_dir = "./trigger"
os.makedirs(watch_dir, exist_ok=True)
observer = Observer()
observer.schedule(Handler(), path=watch_dir, recursive=False)
observer.start()

print("Watching for .trigger files...")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
