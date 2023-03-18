# client_auth.py
"""Handle Keycloak OAUTH authentication tasks."""

from argparse import ArgumentParser, Namespace
import logging

from rest_tools.client import ClientCredentialsAuth, RestClient, SavedDeviceGrantAuth
from wipac_dev_tools import from_environment

from indexer import defaults
from indexer.config import OAuthConfiguration, RestConfiguration

LOG = logging.getLogger(__name__)


def add_auth_to_argparse(parser: ArgumentParser) -> None:
    """Add auth args to argparse."""
    config = from_environment({
        'FILE_CATALOG_REST_URL': defaults.FILE_CATALOG_REST_URL,
        'ICEPROD_REST_URL': defaults.ICEPROD_REST_URL,
        'OAUTH_CLIENT_ID': defaults.OAUTH_CLIENT_ID,
        'OAUTH_CLIENT_SECRET': '',
        'OAUTH_URL': defaults.OAUTH_URL,
        'REST_RETRIES': str(defaults.REST_RETRIES),
        'REST_TIMEOUT': str(defaults.REST_TIMEOUT),
    })

    rest_description = '''
        Use these options to configure access to the REST API.
        Can also be specified via env variables: FILE_CATALOG_REST_URL, ICEPROD_REST_URL,
        REST_TIMEOUT, and REST_RETRIES.
    '''
    rest_group = parser.add_argument_group('REST API', rest_description)
    rest_group.add_argument('--file-catalog-rest-url',
                            default=config['FILE_CATALOG_REST_URL'],
                            help='URL for File Catalog REST API')
    rest_group.add_argument('--iceprod-rest-url',
                            default=config['ICEPROD_REST_URL'],
                            help='URL for IceProd REST API')
    rest_group.add_argument('--rest-timeout',
                            default=int(config['REST_TIMEOUT']),
                            type=int,
                            help=f'request timeout (default: {defaults.REST_TIMEOUT}s)')
    rest_group.add_argument('--rest-retries',
                            default=int(config['REST_RETRIES']),
                            type=int,
                            help=f'number of retries to attempt (default: {defaults.REST_RETRIES})')

    oauth_description = '''
        Use either user credentials or client credentials to authenticate.
        Can also be specified via env variables: OAUTH_URL, OAUTH_CLIENT_ID,
        and OAUTH_CLIENT_SECRET.
    '''
    oauth_group = parser.add_argument_group('OAuth', oauth_description)
    oauth_group.add_argument('--oauth-url',
                             default=config['OAUTH_URL'],
                             help='The OAuth server URL for OpenID discovery')
    oauth_group.add_argument('--oauth-client-id',
                             default=config['OAUTH_CLIENT_ID'],
                             help='The OAuth client id')
    oauth_group.add_argument('--oauth-client-secret',
                             default=config['OAUTH_CLIENT_SECRET'],
                             help='The OAuth client secret, to enable client credential mode')


def create_oauth_config(args: Namespace) -> OAuthConfiguration:
    """Create an OAuthConfiguration object from parsed command-line arguments."""
    if not args.oauth_client_secret:
        if args.oauth_client_id == 'file-catalog-indexer':
            args.oauth_client_id = 'file-catalog-indexer-public'
    return {
        "oauth_url": args.oauth_url,
        "oauth_client_id": args.oauth_client_id,
        "oauth_client_secret": args.oauth_client_secret,
    }


def create_rest_config(args: Namespace) -> RestConfiguration:
    """Create a RestConfiguration object from parsed command-line arguments."""
    return {
        "file_catalog_rest_url": args.file_catalog_rest_url,
        "iceprod_rest_url": args.iceprod_rest_url,
        "rest_timeout": args.rest_timeout,
        "rest_retries": args.rest_retries,
    }


def create_file_catalog_rest_client(oauth_config: OAuthConfiguration,
                                    rest_config: RestConfiguration) -> RestClient:
    """Create a RestClient from argparse args."""
    if oauth_config["oauth_client_secret"]:
        LOG.debug('Using client credentials to authenticate with the File Catalog')
        return ClientCredentialsAuth(
            address=rest_config["file_catalog_rest_url"],
            token_url=oauth_config["oauth_url"],
            client_id=oauth_config["oauth_client_id"],
            client_secret=oauth_config["oauth_client_secret"],
            timeout=rest_config["rest_timeout"],
            retries=rest_config["rest_retries"],
        )
    else:
        LOG.debug('Using user credentials to authenticate with the File Catalog')
        return SavedDeviceGrantAuth(
            address=rest_config["file_catalog_rest_url"],
            filename='.file-catalog-indexer-auth',
            token_url=oauth_config["oauth_url"],
            client_id=oauth_config["oauth_client_id"],
            timeout=rest_config["rest_timeout"],
            retries=rest_config["rest_retries"],
        )


def create_iceprod_rest_client(oauth_config: OAuthConfiguration,
                               rest_config: RestConfiguration) -> RestClient:
    """Create a RestClient from argparse args."""
    if oauth_config["oauth_client_secret"]:
        LOG.debug('Using client credentials to authenticate with IceProd')
        return ClientCredentialsAuth(
            address=rest_config["iceprod_rest_url"],
            token_url=oauth_config["oauth_url"],
            client_id=oauth_config["oauth_client_id"],
            client_secret=oauth_config["oauth_client_secret"],
            timeout=rest_config["rest_timeout"],
            retries=rest_config["rest_retries"],
        )
    else:
        LOG.debug('Using user credentials to authenticate with IceProd')
        return SavedDeviceGrantAuth(
            address=rest_config["iceprod_rest_url"],
            filename='.file-catalog-indexer-auth',
            token_url=oauth_config["oauth_url"],
            client_id=oauth_config["oauth_client_id"],
            timeout=rest_config["rest_timeout"],
            retries=rest_config["rest_retries"],
        )
