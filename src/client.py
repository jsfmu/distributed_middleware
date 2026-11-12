import argparse
import json

import requests


def main():
    parser = argparse.ArgumentParser(
        description="AI Middleware Client"
    )

    parser.add_argument(
        "--middleware-host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--middleware-port",
        type=int,
        default=8000
    )

    parser.add_argument(
        "--input",
        required=True
    )

    args = parser.parse_args()

    url = (
        f"http://{args.middleware_host}:"
        f"{args.middleware_port}/infer"
    )

    try:
        response = requests.post(
            url,
            json={
                "input": args.input
            },
            timeout=10
        )

        print(
            json.dumps(
                response.json(),
                indent=2
            )
        )

    except requests.RequestException as exc:
        print(f"Request failed: {exc}")


if __name__ == "__main__":
    main()