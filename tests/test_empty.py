import os

print(f"RABBITMQ_HOST: {os.environ.get('RABBITMQ_HOST')}")


def test_demo():
    assert 1 == 1
