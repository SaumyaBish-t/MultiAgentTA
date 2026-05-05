import redis
import json
from config.settings import settings

def check_redis():
    r = redis.from_url(settings.redis_url, decode_responses=True)
    state = r.get('portfolio:current:state')
    if state:
        data = json.loads(state)
        print(f"Portfolio value: ${float(data.get('total_value', 0)):,.0f}")
        print(f"   Positions: {len(data.get('positions', []))}")
        print(f"   Cash: ${float(data.get('cash', 0)):,.0f}")
    else:
        print("No portfolio state in Redis")

    quality = r.get('execution:quality:score:latest')
    print(f"Execution quality score: {quality}")

if __name__ == "__main__":
    check_redis()
