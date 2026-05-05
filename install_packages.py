import subprocess
import sys

def main():
    requirements_file = "requirements.txt"
    
    try:
        with open(requirements_file, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: {requirements_file} not found.")
        return

    packages = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#"):
            packages.append(line)

    print(f"Found {len(packages)} packages to install.")
    print("=" * 50)

    success_count = 0
    failed_packages = []

    for idx, pkg in enumerate(packages, 1):
        print(f"[{idx}/{len(packages)}] Installing {pkg} ...", end=" ", flush=True)
        
        # Run pip install with timeout and max retries to fail faster if blocked
        cmd = [
            sys.executable, "-m", "pip", "install", pkg, 
            "--retries", "1", 
            "--timeout", "10"
        ]
        
        try:
            result = subprocess.run(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True,
                timeout=30 # Total timeout for the subprocess
            )
            
            if result.returncode == 0:
                print("[SUCCESS]")
                success_count += 1
            else:
                print("[FAILED]")
                print(f"   Reason: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'Unknown Error'}")
                failed_packages.append(pkg)
                
        except subprocess.TimeoutExpired:
            print("TIMEOUT (Network Blocked?)")
            failed_packages.append(pkg)

    print("=" * 50)
    print(f"Installation Complete. {success_count}/{len(packages)} succeeded.")
    
    if failed_packages:
        print("\nFailed Packages:")
        for pkg in failed_packages:
            print(f" - {pkg}")

if __name__ == "__main__":
    main()
