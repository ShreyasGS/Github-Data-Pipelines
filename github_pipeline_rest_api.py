import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": dlt.secrets["sources.access_token"],
        },
        "paginator": "header_link"
    },
    "resources": [
        {
            "name": "repos_test",
            "endpoint": {
                "path": "orgs/dlt-hub/repos"
            },
        },
        {
          "name": "contributors_test",
          "endpoint": {
            "path": "repos/dlt-hub/dlt/contributors",
          },
        },
    ],
}

github_source = rest_api_source(config)

pipeline = dlt.pipeline(
        pipeline_name="github_test",
        destination="snowflake",
        dataset_name="github_snw_test",
        progress="log"  
    )

load_info = pipeline.run(github_source)
print(load_info)