import redis

ResponseError = redis.ResponseError

class StrictRedis(redis.StrictRedis):
    
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        return self.connection_pool.request(self, *args, **options)
    
    def pipeline(self, transaction=True, shard_hint=None):
        return StrictPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)