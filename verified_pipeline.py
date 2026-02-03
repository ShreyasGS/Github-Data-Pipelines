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
            paginator=HeaderLinkPaginator(),
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28"
            }
    )

    # Define a resource which fetches repos data
    @dlt.resource(name="repos", write_disposition="replace")
    def repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield page

    # Define a resource which fetches issues data (incremental by updated timestamp)
    @dlt.resource(name="issues", write_disposition="append")
    def issues(
        updated_at=dlt.sources.incremental(
            "updated_at",
            initial_value="2026-01-01T00:00:00Z",
        )
    ):
        for page in client.paginate(
            "repos/dlt-hub/dlt/issues",
            params={
                "state": "open",  # Only get open issues
                "sort": "updated",
                "direction": "desc",
                "since": updated_at.start_value,  # For incremental loading
            },
        ):
            yield page

    return [repos, issues]


if __name__ == "__main__":
    #Define pipeline
    pipeline = dlt.pipeline(
            pipeline_name="github_inc_test_pipeline",
            destination="snowflake",
            dataset_name="github_inc_test", #Dataset name in Snowflake
            progress="log"
        )

    load_info = pipeline.run(github_source())
    print(load_info)