"""
Maat Management API Operator for Apache Airflow.

This operator provides integration with the Maat Management API,
supporting operations for Resource and Service Inventory Management.
"""

from typing import Any, Dict, Optional
import json
import time
import requests

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class MaatAPIOperator(BaseOperator):
    """
    Custom operator to interact with Maat Management API.

    This operator supports all CRUD operations for:
    - Service Inventory Management
    - Resource Inventory Management

    :param endpoint: API endpoint path (e.g., '/serviceInventoryManagement/v4.0.0/service')
    :param method: HTTP method (GET, POST, PATCH, DELETE)
    :param base_url: Base URL of the Maat API (default: http://192.168.64.7:8080)
    :param data: Request body data (for POST/PATCH requests)
    :param query_params: Query parameters (for GET requests)
    :param headers: Additional HTTP headers
    :param verify_ssl: Whether to verify SSL certificates (default: False)
    :param response_check: Optional callable to validate response
    :param do_xcom_push: Whether to push the response to XCom (default: True)
    """

    template_fields = ('endpoint', 'method', 'data', 'query_params')
    template_fields_renderers = {'data': 'json', 'query_params': 'json'}
    ui_color = '#4CAF50'
    ui_fgcolor = '#FFFFFF'

    def __init__(
        self,
        *,
        endpoint: str,
        method: str = 'GET',
        base_url: str = 'http://192.168.64.7:8080',
        data: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        verify_ssl: bool = False,
        response_check: Optional[callable] = None,
        do_xcom_push: bool = True,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method.upper() if not method.startswith('{{') else method
        self.base_url = base_url.rstrip('/')
        self.data = data
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.verify_ssl = verify_ssl
        self.response_check = response_check
        self.do_xcom_push = do_xcom_push

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the HTTP request to Maat API.

        :param context: Airflow task context
        :return: Response data (JSON if applicable, otherwise text)
        """
        # Validate method after templating
        method = self.method.upper()
        valid_methods = ['GET', 'POST', 'PATCH', 'DELETE']
        if method not in valid_methods:
            raise AirflowException(
                f"Invalid HTTP method: {method}. Must be one of {valid_methods}"
            )

        url = f"{self.base_url}{self.endpoint}"

        # Set default headers
        request_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        request_headers.update(self.headers)

        # Log the request
        self.log.info(f"Making {method} request to: {url}")
        self.log.info(f"Headers: {request_headers}")
        if self.query_params:
            self.log.info(f"Query params: {self.query_params}")
        if self.data:
            self.log.info(f"Request body: {json.dumps(self.data, indent=2)}")

        # Retry logic for connection errors and timeouts
        max_retries = 3
        retry_delay_seconds = 60

        for attempt in range(1, max_retries + 1):
            try:
                # Make the HTTP request
                response = requests.request(
                    method=method,
                    url=url,
                    json=self.data if self.data else None,
                    params=self.query_params,
                    headers=request_headers,
                    verify=self.verify_ssl,
                    timeout=60
                )

                # Log response
                self.log.info(f"Response status code: {response.status_code}")
                self.log.info(f"Response headers: {dict(response.headers)}")

                # Push status code to XCom for downstream tasks
                context['ti'].xcom_push(key='http_status_code', value=response.status_code)

                # Check for 5xx server errors only
                if response.status_code >= 500:
                    self.log.error(f"Server Error (5xx): {response.status_code}")
                    self.log.error(f"Response body: {response.text}")
                    raise AirflowException(
                        f"HTTP {response.status_code} error from Maat API: {response.text}"
                    )

                # Log 4xx client errors but don't raise exception
                if 400 <= response.status_code < 500:
                    self.log.warning(f"Client Error (4xx): {response.status_code}")
                    self.log.warning(f"Response body: {response.text}")

                # Parse response
                response_data = None
                if response.text:
                    try:
                        response_data = response.json()
                        self.log.info(f"Response JSON: {json.dumps(response_data, indent=2)}")
                    except json.JSONDecodeError:
                        response_data = response.text
                        self.log.info(f"Response text: {response_data}")
                else:
                    self.log.info("Empty response body")
                    response_data = {"status": "success", "status_code": response.status_code}

                # Always include status code in response data
                if isinstance(response_data, dict):
                    response_data['http_status_code'] = response.status_code
                else:
                    # If response is text, wrap it in a dict with status code
                    response_data = {
                        'response': response_data,
                        'http_status_code': response.status_code
                    }

                # Custom response validation
                if self.response_check and not self.response_check(response_data):
                    raise AirflowException("Response check failed")

                return response_data

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                error_type = "Connection error" if isinstance(e, requests.exceptions.ConnectionError) else "Request timeout"
                self.log.error(f"{error_type} on attempt {attempt}/{max_retries}: {e}")

                if attempt < max_retries:
                    self.log.warning(f"Retrying in {retry_delay_seconds} seconds...")
                    time.sleep(retry_delay_seconds)
                else:
                    self.log.error(f"Maat API is unavailable after {max_retries} attempts")
                    raise AirflowException("Maat is unavailable")

            except requests.exceptions.RequestException as e:
                self.log.error(f"Request error: {e}")
                raise AirflowException(f"Request to Maat API failed: {e}")


class MaatServiceOperator(MaatAPIOperator):
    """
    Specialized operator for Service Inventory Management operations.

    :param service_id: Service ID (required for retrieve, update, delete operations)
    :param operation: Operation type ('list', 'create', 'retrieve', 'update', 'delete')
    :param service_data: Service data (for create/update operations)
    :param query_params: Query parameters (for list/retrieve operations)
    """

    def __init__(
        self,
        *,
        operation: str,
        service_id: Optional[str] = None,
        service_data: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        # Determine endpoint and method based on operation
        operation = operation.lower()

        if operation == 'list':
            endpoint = '/serviceInventoryManagement/v4.0.0/service'
            method = 'GET'
            data = None
            qp = query_params or {}
        elif operation == 'create':
            endpoint = '/serviceInventoryManagement/v4.0.0/service'
            method = 'POST'
            data = service_data
            qp = {}
        elif operation == 'retrieve':
            if not service_id:
                raise AirflowException("service_id is required for retrieve operation")
            endpoint = f'/serviceInventoryManagement/v4.0.0/service/{service_id}'
            method = 'GET'
            data = None
            qp = query_params or {}
        elif operation == 'update':
            if not service_id:
                raise AirflowException("service_id is required for update operation")
            endpoint = f'/serviceInventoryManagement/v4.0.0/service/{service_id}'
            method = 'PATCH'
            data = service_data
            qp = {}
        elif operation == 'delete':
            if not service_id:
                raise AirflowException("service_id is required for delete operation")
            endpoint = f'/serviceInventoryManagement/v4.0.0/service/{service_id}'
            method = 'DELETE'
            data = None
            qp = {}
        else:
            raise AirflowException(
                f"Invalid operation: {operation}. Must be one of: list, create, retrieve, update, delete"
            )

        super().__init__(
            endpoint=endpoint,
            method=method,
            data=data,
            query_params=qp,
            **kwargs
        )


class MaatResourceOperator(MaatAPIOperator):
    """
    Specialized operator for Resource Inventory Management operations.

    :param resource_id: Resource ID (required for retrieve, update, delete operations)
    :param operation: Operation type ('list', 'create', 'retrieve', 'update', 'delete')
    :param resource_data: Resource data (for create/update operations)
    :param query_params: Query parameters (for list/retrieve operations)
    """

    def __init__(
        self,
        *,
        operation: str,
        resource_id: Optional[str] = None,
        resource_data: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        # Determine endpoint and method based on operation
        operation = operation.lower()

        if operation == 'list':
            endpoint = '/resourceInventoryManagement/v4.0.0/resource'
            method = 'GET'
            data = None
            qp = query_params or {}
        elif operation == 'create':
            endpoint = '/resourceInventoryManagement/v4.0.0/resource'
            method = 'POST'
            data = resource_data
            qp = {}
        elif operation == 'retrieve':
            if not resource_id:
                raise AirflowException("resource_id is required for retrieve operation")
            endpoint = f'/resourceInventoryManagement/v4.0.0/resource/{resource_id}'
            method = 'GET'
            data = None
            qp = query_params or {}
        elif operation == 'update':
            if not resource_id:
                raise AirflowException("resource_id is required for update operation")
            endpoint = f'/resourceInventoryManagement/v4.0.0/resource/{resource_id}'
            method = 'PATCH'
            data = resource_data
            qp = {}
        elif operation == 'delete':
            if not resource_id:
                raise AirflowException("resource_id is required for delete operation")
            endpoint = f'/resourceInventoryManagement/v4.0.0/resource/{resource_id}'
            method = 'DELETE'
            data = None
            qp = {}
        else:
            raise AirflowException(
                f"Invalid operation: {operation}. Must be one of: list, create, retrieve, update, delete"
            )

        super().__init__(
            endpoint=endpoint,
            method=method,
            data=data,
            query_params=qp,
            **kwargs
        )

from operators.maat_api_operator import MaatAPIOperator

__all__ = ['MaatAPIOperator']