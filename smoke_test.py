from hackathon import create_app
from hackathon.extensions import db


app = create_app()


if __name__ == "__main__":
    with app.app_context():
        db.create_all()

    client = app.test_client()
    response = client.get("/")

    print(f"status_code={response.status_code}")
    print(f"response_length={len(response.data)}")

