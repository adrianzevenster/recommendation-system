
docker exec -it recommendation-system-redis-1 redis-cli DEL watched:u001
docker exec -it recommendation-system-redis-1 redis-cli DEL recent:u001
docker exec -it recommendation-system-redis-1 redis-cli DEL genre_affinity:u001
docker exec -it recommendation-system-redis-1 redis-cli DEL popular:ZA
docker exec -it recommendation-system-redis-1 redis-cli DEL popular:UK
docker exec -it recommendation-system-redis-1 redis-cli DEL popular:US