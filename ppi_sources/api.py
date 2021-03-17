from aiohttp import ClientTimeout, ClientResponseError
from contextlib import asynccontextmanager
from io import StringIO
from msgpack import unpackb
import pandas as pd
from yarl import URL


class SourceAPIClientResponseError(ClientResponseError):
    def __init__(self, request_info, history, json_body, *, status=None, message="", headers=None):
        super().__init__(request_info, history, status=status, message=message, headers=headers)
        self._json_body = json_body

    @property
    def json_body(self):
        return self._json_body


class SourceAPIClient(object):
    NO_TIMEOUT = ClientTimeout()
    DEFAULT_TIMEOUT = ClientTimeout()

    def __init__(self, http_client, host, source_name):
        self.client = http_client
        self.host = URL(host)
        self.source_name = source_name

    def complete_url(self, path):
        return self.host.with_path(path)


    async def _raise_for_status_with_desc(self, resp):
        # bug in aiohttp < 3.8: resp.ok calls raise_for_status which releases the response

        # copied parts from 3.8 and inlined resp.ok from 3.8
        if resp.status >= 400:
            json_body = None

            try:
                json_body = await resp.json()
            except e:
                print('SourceAPIClient._raise_for_status_with_desc: error getting response body:', e)

            # copied from aiohttp/client_reqrep.py @3.8
            assert resp.reason is not None
            resp.release()

            raise SourceAPIClientResponseError(
                resp.request_info,
                resp.history,
                json_body,
                status=resp.status,
                message=resp.reason,
                headers=resp.headers)


    @asynccontextmanager
    async def request(self, method, path, **kwargs):
        url = self.complete_url(path)

        raise_for_status = kwargs.pop('raise_for_status', False)

        try:
            async with self.client.request(method, url, **kwargs) as resp:
                if callable(raise_for_status):
                    raise_for_status(resp)
                elif raise_for_status:
                    await self._raise_for_status_with_desc(resp)

                yield resp

        except SourceAPIClientResponseError as exc:
            print('Source API client HTTP error:', exc.status, exc.message, 'body:', exc.json_body)
            raise

        except ClientResponseError as exc:
            print('HTTP error:', exc.status, exc.message)
            raise


    async def request_msgpack(self, method, path, **kwargs):
        headers = {'Accept': 'application/x-msgpack'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return unpackb(await resp.read(), raw=False)


    async def request_tsv(self, method, path, **kwargs):
        headers = {'Accept': 'text/tab-separated-values'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return pd.read_csv(StringIO(await resp.text()), sep='\t', header=0, index_col=False)


    async def request_dataframe(self, method, path, **kwargs):
        return pd.DataFrame(await self.request_msgpack(method, path, **kwargs))


    async def request_json(self, method, path, **kwargs):
        headers = {'Accept': 'application/json'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return await resp.json()


    async def get_name(self, desc):
        headers = {'Accept': 'text/plain'}

        path = URL('/') / self.source_name / 'name'

        async with self.request(client, 'GET', path, json=desc) as resp:
            return await resp.text()
