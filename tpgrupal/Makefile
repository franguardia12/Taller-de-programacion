prune: prune

prune:
	docker-compose down --remove-orphans
	docker system prune -f
	docker image prune -a -f