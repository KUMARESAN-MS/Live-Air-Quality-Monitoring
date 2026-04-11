import os
import urllib.request
from pathlib import Path

def setup_windows():
    print("--- Spark Windows Setup Helper ---")
    
    # 1. Define paths
    base_dir = Path(__file__).resolve().parent.parent
    hadoop_dir = base_dir / "hadoop"
    bin_dir = hadoop_dir / "bin"
    winutils_path = bin_dir / "winutils.exe"
    
    # 2. Create directories
    print(f"Creating directory: {bin_dir}")
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. Download binaries (Hadoop 3.3.5 version)
    files = ["winutils.exe", "hadoop.dll", "hdfs.dll"]
    base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/"
    
    for filename in files:
        target_path = bin_dir / filename
        if not target_path.exists():
            url = base_url + filename
            print(f"Downloading {filename} from {url}...")
            try:
                urllib.request.urlretrieve(url, target_path)
                print(f"{filename} download successful!")
            except Exception as e:
                print(f"{filename} download failed: {e}")
                print(f"Please download it manually from {url} and save it to: hadoop/bin/{filename}")
        else:
            print(f"{filename} already exists.")
        
    print("\nSUCCESS!")
    print(f"Local HADOOP_HOME set to: {hadoop_dir}")
    print("You can now run the Spark processor.")

if __name__ == "__main__":
    setup_windows()
