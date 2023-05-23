from unittest import TestCase

from consumer.auth import JWTGenerator
from consumer.util import Clock


class ClockMock(Clock):

    def __init__(self, time: int):
        self.time = time

    def get_time(self) -> int:
        return self.time


class JWTGeneratorTest(TestCase):

    def test_generate(self):
        with open("./resources/test.key", "r") as file:
            key = file.read()
        expected = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImxhX2tleV9pZCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJrYWZrYS1jb25zdW1lci5sYSIsImF1ZCI6ImFjY291bnRfaWQua2Fma2EubGEiLCJleHAiOjEwNjAsImlhdCI6MTAwMH0.LBRaUVdcFjFcBAArlqdQHjfJu_RrLj3fqXMwyncLuQHxDjiBAB-0LHd_d4I0zHEJUKmpo1jZf4JmZ0r5ENTPnJ8d_w2a-s-wjSUgkmjj-hioun1cH8ZQzFQbd3aaekSGGlKNoPPlqzTAqOb4AtUujm0RwfjiWfckujE3SGrMdNtQo2K9fJvCYoSZRawGQw3GRmRbI03TN_NEB73LM5QHVRhRY5yKSUE_mS1KL0-ekS3PbxYQucqrtWrprpe6qgvprc6CxxvHI2mWsafFAdS_nMbGXX_q5cPG0HvRc9RC0DjkHfzSBdOLzUcEYTLl-msk_N1NQ4XlYCN4edUERFynce2nGZgK_sJXuLkC6zYi8BqaNuFk_t-q1u3YmtSgpNxmtlzsC3wzZN1tQ7NTF4wC8xfitTIFgX4HtoPX2dAkkiwc7mcLFCgEVi60Fq8GM4xGem6Y7yiFopwAdjM5--FNC_SMtd4TtNoauMpGnO3rxUQSQ05knLrvejmf5hC_SyOAum3C2krZBmiU5KXEHQIqvJ3-P7ni4u6nHdBDdlRFZsZwAnGGxKnw8AWk8jAHZm_ChA7TMf6vJWdG3IPi5RLopeCtA-5HRuhguQGBa7jh2SpuWuTNO4mj15BRx_Tw581Vgqk0lKH9gwi9PhOhaBlFULdnB6NZ2yUCOdoznP7QRMU"
        generator = JWTGenerator(key, "la_key_id", ClockMock(1000))
        real = generator.generate("account_id")

        self.assertEqual(expected, real)
