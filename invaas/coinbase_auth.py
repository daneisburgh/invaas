import http.client
import hmac
import hashlib
import json
import os
import time

from urllib.parse import urlencode
from typing import Union, Dict


class CoinbaseAuth:
    """
    Singleton class for Coinbase authentication.
    """

    def __init__(self, api_key, api_secret):
        """
        Initialize the CBAuth instance with API credentials.
        """
        if api_key and api_secret:
            self.api_key = api_key
            self.api_secret = api_secret
        else:
            raise Exception("Coinbase API keys not found")

    def __call__(
        self,
        method: str,
        path: str,
        body: Union[Dict, str] = "",
        params: Dict[str, str] = None,
    ) -> Dict:
        """
        Prepare and send an authenticated request to the Coinbase API.

        :param method: HTTP method (e.g., 'GET', 'POST')
        :param path: API endpoint path
        :param body: Request payload
        :param params: URL parameters
        :return: Response from the Coinbase API as a dictionary
        """
        path = self.add_query_params(path, params)
        body_encoded = self.prepare_body(body)
        headers = self.create_headers(method, path, body)
        return self.send_request(method, path, body_encoded, headers)

    def add_query_params(self, path, params):
        if params:
            query_params = urlencode(params)
            path = f"{path}?{query_params}"
        return path

    def prepare_body(self, body):
        return json.dumps(body).encode("utf-8") if body else b""

    def create_headers(self, method, path, body):
        timestamp = str(int(time.time()))
        message = timestamp + method.upper() + path.split("?")[0] + (json.dumps(body) if body else "")
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        return {
            "Content-Type": "application/json",
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": signature,
            "CB-ACCESS-TIMESTAMP": timestamp,
        }

    def send_request(self, method, path, body_encoded, headers):
        conn = http.client.HTTPSConnection("api.coinbase.com")
        conn.request(method, path, body_encoded, headers)
        res = conn.getresponse()
        data = res.read()
        response_data = json.loads(data.decode("utf-8"))

        if res.status != 200:
            raise Exception(response_data)

        return response_data
