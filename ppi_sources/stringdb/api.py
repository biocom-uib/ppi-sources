from aiohttp import ClientSession, TCPConnector
import numpy as np
import pandas as pd
import sys

from ppi_sources.api import SourceAPIClient
from ppi_sources.source import Source
from ppi_sources.stringdb.types import StringDBNetwork, StringDBBitscoreMatrix



class StringDBAPISource(Source):
    def __init__(self, http_client, host):
        super().__init__()

        self.client = SourceAPIClient(http_client, host, 'stringdb')


    async def get_network(self, net_desc):
        name = f"stringdb_{net_desc['species_id']}"
        #name = await self.client.get_name(net_desc)

        ext_ids = await self.client.request_dataframe('POST', '/db/stringdb/items/proteins/select', json={
            'columns': ['protein_id', 'protein_external_id'],
            'filter': {
                'species_id': [net_desc['species_id']]
            }
        })
        ext_ids = ext_ids.rename(columns={
            'protein_id': 'string_id',
            'protein_external_id': 'external_id'
        })

        edges_df = await self.client.request_dataframe('POST', '/db/stringdb/network/edges/select', json=net_desc)

        return StringDBNetwork(name,
                [net_desc['species_id']],
                ext_ids.set_index('string_id').external_id,
                edges_df)


    async def build_custom_network(self, net_desc):
        edges_array = np.array(net_desc['edges'], dtype=str)

        vert_ids = np.unique(edges_array.flatten()).tolist()

        # assume that the user intended to use protein_id if given numeric IDs,
        # protein_external_id otherwise
        provided_stringdb_col = 'protein_external_id'

        if all(vert_id.isnumeric() for vert_id in vert_ids):
            provided_stringdb_col = 'protein_id'
            vert_ids = [int(vert_id) for vert_id in vert_ids]

        # request a mapping between protein_id and protein_external_id, also get related species_id's
        vert_data = await self.client.request_dataframe('POST', '/db/stringdb/items/proteins/select', json={
            'columns': ['protein_id', 'species_id', 'protein_external_id'],
            'filter': {
                provided_stringdb_col: vert_ids
            }
        })

        stringdb_col_mapping = {
            'protein_id': 'string_id',
            'protein_external_id': 'external_id'
        }
        vert_data = vert_data.rename(columns=stringdb_col_mapping)

        species_ids = vert_data.species_id.unique().tolist()

        # built the edge data frame
        edges_df = pd.DataFrame(edges_array, columns=['node_id_a', 'node_id_b'])

        # ensure that all edges are known & mapped correctly to string_id's
        old_nrows = len(edges_df)

        if provided_stringdb_col == 'protein_id':
            known_ids = set(map(str, vert_data.string_id))

            edges_df = edges_df.loc[
                    edges_df.node_id_a.isin(known_ids)
                    & edges_df.node_id_b.isin(known_ids)]
        else:
            ext_id_to_string_id = vert_data.set_index('external_id').string_id

            edges_df = edges_df \
                .apply(lambda col: col.map(ext_id_to_string_id)) \
                .dropna()

        if old_nrows != len(edges_df):
            print(f'string.api.build_custom_network: dropped {old_nrows - len(edges_df)} unknown edges out of {old_nrows}',
                file=sys.stderr)

        name = "stringdb_custom_" + '_'.join(map(str, sorted(species_ids)))
        #name = await self.client.get_name(net_desc)

        return StringDBNetwork(name,
                species_ids,
                vert_data.set_index('string_id').external_id,
                edges_df)


    async def get_bitscore_matrix(self, net1, net2):
        df = await self.client.request_dataframe('POST', '/db/stringdb/bitscore/select', json={
            'net1_species_ids': list(net1.species_ids),
            'net1_protein_ids': list(net1.string_ids),

            'net2_species_ids': list(net2.species_ids),
            'net2_protein_ids': list(net2.string_ids)
        })
        df = df.rename(columns={
            'protein_id_a': 'protein_id_a',
            'protein_id_b': 'protein_id_b',
            'bitscore': 'bitscore'
        })

        return StringDBBitscoreMatrix(df, net1, net2, by='string_id')


    async def get_ontology_mapping(self, networks):
        species_ids = {species_id for network in networks for species_id in network.species_ids}

        return await self.client.request_msgpack('POST', '/db/stringdb/go/annotations/select', json={
            'species_ids': list(species_ids)
        })


try:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def stringdb_api_source(host=None, *, timeout=SourceAPIClient.DEFAULT_TIMEOUT, http_client=None):
        if http_client is None:
            async with ClientSession(timeout=timeout) as session:
                yield StringDBAPISource(session, host=host)
        else:
            yield StringDBAPISource(session, host=host)

except ImportError:
    pass
