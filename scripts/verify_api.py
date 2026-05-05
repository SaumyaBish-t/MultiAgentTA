"""
Test API endpoints and print results
"""
import httpx
import json

def test_api():
    base_url = "http://localhost:8001"
    endpoints = ["/status", "/health/detailed", "/regime"]
    
    for ep in endpoints:
        print(f"\nTesting {ep}...")
        try:
            resp = httpx.get(f"{base_url}{ep}", timeout=10)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                print(json.dumps(resp.json(), indent=2))
            else:
                print(resp.text)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
