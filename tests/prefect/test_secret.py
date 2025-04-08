from prefect import flow
from prefect.blocks.system import Secret


@flow
def test_secret():
    try:
        secret = Secret.load("fpl-minio-secret-key").get()
        print("Secret loaded successfully!")
        print(f"First 3 characters: {secret[:3]}...")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_secret()
