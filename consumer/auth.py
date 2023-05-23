import jwt

from consumer.util import Clock


class JWTGenerator:

    def __init__(self, la_key: str, la_key_id: str, clock: Clock):
        self.la_key = la_key
        self.la_key_id = la_key_id
        self.clock = clock or Clock()

    def generate(self, account_id: str) -> str:
        iat = self.clock.get_time()
        token = jwt.encode({
            'iss': 'kafka-consumer.la',
            'aud': account_id + '.kafka.la',
            'exp': iat + 60,
            'iat': iat
        }, self.la_key, algorithm="RS256", headers={'kid': self.la_key_id})
        return token
