import redis
from common.config import settings


def get_redis() -> redis.Redis:
    return redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
