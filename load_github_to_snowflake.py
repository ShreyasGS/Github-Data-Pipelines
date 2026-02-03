import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.source
def github_source(access_token=dlt.secrets.value):
    """Fetch GitHub data from dlt-hub organization"""
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(token=access_token),
        paginator=HeaderLinkPaginator()
    )
    
    @dlt.resource(name="repos", write_disposition="merge", primary_key="id")
    def repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield page
    
    @dlt.resource(name="contributors", write_disposition="append")
    def contributors():
        for page in client.paginate("repos/dlt-hub/dlt/contributors"):
            yield page
    
    return [repos, contributors]

def load_github():
    """Run the pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="github_spcs_pipeline",
        destination="snowflake",
        staging="filesystem",  # S3 staging
        dataset_name="github_spcs",
        progress="log"
    )
    
    load_info = pipeline.run(github_source())
    print(load_info)
    print(f"✅ Pipeline completed successfully!")

if __name__ == "__main__":
    load_github()