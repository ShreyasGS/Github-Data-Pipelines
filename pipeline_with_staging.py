import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# Define source which groups all github resources
@dlt.source
def github_source(access_token=dlt.secrets.value): #API key stored securely using dlt
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator()
    )

    # Define a resource which fetches repos data
    @dlt.resource(name="repos")
    def repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield page

    # Define a resource which fetches repos data
    @dlt.resource(name="contributors")
    def contributors():
        for page in client.paginate("repos/dlt-hub/dlt/contributors"):
            yield page

    return [repos, contributors]

# Pipeline with S3 staging for Snowflake
pipeline = dlt.pipeline(
    pipeline_name="github_s3_pipeline",
    destination="snowflake",
    staging="filesystem",  # Enable S3 staging
    dataset_name="github_ext_stg_test",
    progress="log"
)

# Run the pipeline
load_info = pipeline.run(github_source)
print(load_info)
